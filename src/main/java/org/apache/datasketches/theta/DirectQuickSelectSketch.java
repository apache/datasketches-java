/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.theta;

import static org.apache.datasketches.Util.LONG_MAX_VALUE_AS_DOUBLE;
import static org.apache.datasketches.Util.MIN_LG_ARR_LONGS;
import static org.apache.datasketches.Util.computeSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.P_FLOAT;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.getMemBytes;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.insertFlags;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.apache.datasketches.theta.PreambleUtil.insertP;
import static org.apache.datasketches.theta.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertUnionThetaLong;
import static org.apache.datasketches.theta.Rebuilder.actLgResizeFactor;
import static org.apache.datasketches.theta.Rebuilder.moveAndResize;
import static org.apache.datasketches.theta.Rebuilder.quickSelectAndRebuild;
import static org.apache.datasketches.theta.Rebuilder.resize;
import static org.apache.datasketches.theta.UpdateReturnState.InsertedCountIncremented;
import static org.apache.datasketches.theta.UpdateReturnState.InsertedCountIncrementedRebuilt;
import static org.apache.datasketches.theta.UpdateReturnState.InsertedCountIncrementedResized;
import static org.apache.datasketches.theta.UpdateReturnState.RejectedDuplicate;
import static org.apache.datasketches.theta.UpdateReturnState.RejectedOverTheta;

import org.apache.datasketches.Family;
import org.apache.datasketches.HashOperations;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The default Theta Sketch using the QuickSelect algorithm.
 * This subclass implements methods, which affect the state (update, rebuild, reset)
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketch extends DirectQuickSelectSketchR {
  MemoryRequestServer memReqSvr_ = null; //never serialized

  private DirectQuickSelectSketch(
      final long seed,
      final WritableMemory wmem) {
    super(seed, wmem);
  }

  /**
   * Construct a new sketch instance and initialize the given Memory as its backing store.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param p
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf Currently internally fixed at 2. Unless dstMem is not configured with a valid
   * MemoryRequest, in which case the rf is effectively 1, which is no resizing at all and the
   * dstMem must be large enough for a full sketch.
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param memReqSvr the given MemoryRequestServer
   * @param dstMem the given Memory object destination. It cannot be null.
   * It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function.
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   */
  DirectQuickSelectSketch(
      final int lgNomLongs,
      final long seed,
      final float p,
      final ResizeFactor rf,
      final MemoryRequestServer memReqSvr,
      final WritableMemory dstMem,
      final boolean unionGadget) {
    super(seed, dstMem);

    //Choose family, preambleLongs
    final Family family;
    final int preambleLongs;
    if (unionGadget) {
      preambleLongs = Family.UNION.getMinPreLongs();
      family = Family.UNION;
    }
    else {
      preambleLongs = Family.QUICKSELECT.getMinPreLongs();
      family = Family.QUICKSELECT;
    }

    //Choose RF, minReqBytes, lgArrLongs.
    final int lgRF = rf.lg();
    final int lgArrLongs = (lgRF == 0) ? lgNomLongs + 1 : MIN_LG_ARR_LONGS;
    final int minReqBytes = getMemBytes(lgArrLongs, preambleLongs);

    //Make sure Memory is large enough
    final long curMemCapBytes = dstMem.getCapacity();
    if (curMemCapBytes < minReqBytes) {
      throw new SketchesArgumentException(
        "Memory capacity is too small: " + curMemCapBytes + " < " + minReqBytes);
    }

    //@formatter:off
    //Build preamble
    insertPreLongs(dstMem, preambleLongs);                 //byte 0
    insertLgResizeFactor(dstMem, lgRF);                    //byte 0
    insertSerVer(dstMem, SER_VER);                         //byte 1
    insertFamilyID(dstMem, family.getID());                //byte 2
    insertLgNomLongs(dstMem, lgNomLongs);                  //byte 3
    insertLgArrLongs(dstMem, lgArrLongs);                  //byte 4
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true : 00100 = 4
    insertFlags(dstMem, EMPTY_FLAG_MASK);                  //byte 5
    insertSeedHash(dstMem, computeSeedHash(seed));    //bytes 6,7
    insertCurCount(dstMem, 0);                             //bytes 8-11
    insertP(dstMem, p);                                    //bytes 12-15
    final long thetaLong = (long)(p * LONG_MAX_VALUE_AS_DOUBLE);
    insertThetaLong(dstMem, thetaLong);                    //bytes 16-23
    if (unionGadget) {
      insertUnionThetaLong(dstMem, thetaLong);
    }
    //@formatter:on

    //clear hash table area
    dstMem.clear(preambleLongs << 3, 8 << lgArrLongs);

    hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    memReqSvr_ = memReqSvr;
  }

  /**
   * Wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch writableWrap(final WritableMemory srcMem, final long seed) {
    final int preambleLongs = extractPreLongs(srcMem);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(srcMem);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(srcMem);                   //byte 4

    UpdateSketch.checkUnionQuickSelectFamily(srcMem, preambleLongs, lgNomLongs);
    checkMemIntegrity(srcMem, seed, preambleLongs, lgNomLongs, lgArrLongs);

    if (isResizeFactorIncorrect(srcMem, lgNomLongs, lgArrLongs)) {
      //If incorrect it sets it to X2 which always works.
      insertLgResizeFactor(srcMem, ResizeFactor.X2.lg());
    }

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(seed, srcMem);
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  /**
   * Fast-wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.  This does NO validity checking of the given Memory.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch fastWritableWrap(final WritableMemory srcMem, final long seed) {
    final int lgNomLongs = extractLgNomLongs(srcMem);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(srcMem);                   //byte 4

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(seed, srcMem);
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  //Sketch

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    final int lgNomLongs = getLgNomLongs();
    final int preambleLongs = wmem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    if (getRetainedEntries(true) > (1 << lgNomLongs)) {
      quickSelectAndRebuild(wmem_, preambleLongs, lgNomLongs);
    }
    return this;
  }

  @Override
  public void reset() {
    //clear hash table
    //hash table size and hashTableThreshold stays the same
    //lgArrLongs stays the same
    //thetaLongs resets to p
    final int arrLongs = 1 << getLgArrLongs();
    final int preambleLongs = wmem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int preBytes = preambleLongs << 3;
    wmem_.clear(preBytes, arrLongs * 8L); //clear data array
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    wmem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    wmem_.putInt(RETAINED_ENTRIES_INT, 0);
    final float p = wmem_.getFloat(P_FLOAT);
    final long thetaLong = (long) (p * LONG_MAX_VALUE_AS_DOUBLE);
    wmem_.putLong(THETA_LONG, thetaLong);
  }

  //restricted methods

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    HashOperations.checkHashCorruption(hash);

    wmem_.putByte(FLAGS_BYTE, (byte) (wmem_.getByte(FLAGS_BYTE) & ~EMPTY_FLAG_MASK));
    final long thetaLong = getThetaLong();
    final int lgNomLongs = getLgNomLongs();
    //The over-theta test
    if (HashOperations.continueCondition(thetaLong, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta or zero.
    }

    final int lgArrLongs = getLgArrLongs();
    final int preambleLongs = wmem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;

    //The duplicate test
    final int index =
        HashOperations.hashSearchOrInsertMemory(wmem_, lgArrLongs, hash, preambleLongs << 3);
    if (index >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, increment curCount
    final int curCount = getRetainedEntries(true) + 1;
    wmem_.putInt(RETAINED_ENTRIES_INT, curCount); //update curCount

    if (isOutOfSpace(curCount)) { //we need to do something, we are out of space

      if (lgArrLongs > lgNomLongs) { //at full size, rebuild
        //Assumes no dirty values, changes thetaLong, curCount_
        assert (lgArrLongs == (lgNomLongs + 1))
            : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs;
        //rebuild, refresh curCount based on # values in the hashtable.
        quickSelectAndRebuild(wmem_, preambleLongs, lgNomLongs);
        return InsertedCountIncrementedRebuilt;
      } //end of rebuild, exit

      else { //Not at full size, resize. Should not get here if lgRF = 0 and memCap is too small.
        final int lgRF = getLgRF();
        final int actLgRF = actLgResizeFactor(wmem_.getCapacity(), lgArrLongs, preambleLongs, lgRF);
        int tgtLgArrLongs = Math.min(lgArrLongs + actLgRF, lgNomLongs + 1);

        if (actLgRF > 0) { //Expand in current Memory
          //lgArrLongs will change; thetaLong, curCount will not
          resize(wmem_, preambleLongs, lgArrLongs, tgtLgArrLongs);
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs, tgtLgArrLongs);
          return InsertedCountIncrementedResized;
        } //end of Expand in current memory, exit.

        else {
          //Request more memory, then resize. lgArrLongs will change; thetaLong, curCount will not
          final int preBytes = preambleLongs << 3;
          tgtLgArrLongs = Math.min(lgArrLongs + lgRF, lgNomLongs + 1);
          final int tgtArrBytes = 8 << tgtLgArrLongs;
          final int reqBytes = tgtArrBytes + preBytes;

          memReqSvr_ = (memReqSvr_ == null) ? wmem_.getMemoryRequestServer() : memReqSvr_;

          final WritableMemory newDstMem = memReqSvr_.request(reqBytes);

          moveAndResize(wmem_, preambleLongs, lgArrLongs, newDstMem, tgtLgArrLongs, thetaLong);

          memReqSvr_.requestClose(wmem_, newDstMem);

          wmem_ = newDstMem;
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs, tgtLgArrLongs);
          return InsertedCountIncrementedResized;
        } //end of Request more memory to resize
      } //end of resize
    } //end of isOutOfSpace
    return InsertedCountIncremented;
  }

}
