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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
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
import static org.apache.datasketches.theta.PreambleUtil.getSegBytes;
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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.HashOperations;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * The default Theta Sketch using the QuickSelect algorithm.
 * This subclass implements methods, which affect the state (update, rebuild, reset)
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketch extends DirectQuickSelectSketchR {

  private DirectQuickSelectSketch(
      final long seed,
      final MemorySegment wseg) {
    super(seed, wseg);
  }

  /**
   * Construct a new sketch instance and initialize the given MemorySegment as its backing store.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param p
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf Resize Factor
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstSeg the given MemorySegment object destination. It cannot be null.
   * It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function.
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   */
  DirectQuickSelectSketch(
      final int lgNomLongs,
      final long seed,
      final float p,
      final ResizeFactor rf,
      final MemorySegment dstSeg,
      final boolean unionGadget) {
    this(
        checkSegSize(lgNomLongs, rf, dstSeg, unionGadget),
        //SpotBugs CT_CONSTRUCTOR_THROW is false positive.
        //this construction scheme is compliant with SEI CERT Oracle Coding Standard for Java / OBJ11-J
        lgNomLongs,
        seed,
        p,
        rf,
        dstSeg,
        unionGadget);
  }

  private DirectQuickSelectSketch(
      final boolean secure, //required part of Finalizer Attack prevention
      final int lgNomLongs,
      final long seed,
      final float p,
      final ResizeFactor rf,
      final MemorySegment dstSeg,
      final boolean unionGadget) {
    super(seed, dstSeg);
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
    final int lgArrLongs = (lgRF == 0) ? lgNomLongs + 1 : ThetaUtil.MIN_LG_ARR_LONGS;

    //@formatter:off
    //Build preamble
    insertPreLongs(dstSeg, preambleLongs);                 //byte 0
    insertLgResizeFactor(dstSeg, lgRF);                    //byte 0
    insertSerVer(dstSeg, SER_VER);                         //byte 1
    insertFamilyID(dstSeg, family.getID());                //byte 2
    insertLgNomLongs(dstSeg, lgNomLongs);                  //byte 3
    insertLgArrLongs(dstSeg, lgArrLongs);                  //byte 4
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true : 00100 = 4
    insertFlags(dstSeg, EMPTY_FLAG_MASK);                  //byte 5
    insertSeedHash(dstSeg, Util.computeSeedHash(seed));    //bytes 6,7
    insertCurCount(dstSeg, 0);                             //bytes 8-11
    insertP(dstSeg, p);                                    //bytes 12-15
    final long thetaLong = (long)(p * LONG_MAX_VALUE_AS_DOUBLE);
    insertThetaLong(dstSeg, thetaLong);                    //bytes 16-23
    if (unionGadget) {
      insertUnionThetaLong(dstSeg, thetaLong);
    }
    //@formatter:on

    //clear hash table area
    dstSeg.asSlice(preambleLongs << 3, Long.BYTES << lgArrLongs).fill((byte)0);

    hashTableThreshold_ = getOffHeapHashTableThreshold(lgNomLongs, lgArrLongs);
  }

  private static final boolean checkSegSize(
      final int lgNomLongs, final ResizeFactor rf, final MemorySegment dstSeg, final boolean unionGadget) {
    final int preambleLongs = (unionGadget) ? Family.UNION.getMinPreLongs() : Family.QUICKSELECT.getMinPreLongs();
    final int lgRF = rf.lg();
    final int lgArrLongs = (lgRF == 0) ? lgNomLongs + 1 : ThetaUtil.MIN_LG_ARR_LONGS;
    final int minReqBytes = getSegBytes(lgArrLongs, preambleLongs);
    final long curSegCapBytes = dstSeg.byteSize();
    if (curSegCapBytes < minReqBytes) {
      throw new SketchesArgumentException(
        "MemorySegment capacity is too small: " + curSegCapBytes + " < " + minReqBytes);
    }
    return true;
  }

  /**
   * Wrap a sketch around the given source MemorySegment containing sketch data that originated from
   * this sketch.
   * @param srcSeg The given MemorySegment object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch writableWrap(final MemorySegment srcSeg, final long seed) {
    final int preambleLongs = extractPreLongs(srcSeg);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(srcSeg);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(srcSeg);                   //byte 4

    UpdateSketch.checkUnionQuickSelectFamily(srcSeg, preambleLongs, lgNomLongs);
    checkSegIntegrity(srcSeg, seed, preambleLongs, lgNomLongs, lgArrLongs);

    if (isResizeFactorIncorrect(srcSeg, lgNomLongs, lgArrLongs)) {
      //If incorrect it sets it to X2 which always works.
      insertLgResizeFactor(srcSeg, ResizeFactor.X2.lg());
    }

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(seed, srcSeg);
    dqss.hashTableThreshold_ = getOffHeapHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  /**
   * Fast-wrap a sketch around the given source MemorySegment containing sketch data that originated from
   * this sketch.  This does NO validity checking of the given MemorySegment.
   * @param srcSeg The given MemorySegment must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch fastWritableWrap(final MemorySegment srcSeg, final long seed) {
    final int lgNomLongs = extractLgNomLongs(srcSeg);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(srcSeg);                   //byte 4

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(seed, srcSeg);
    dqss.hashTableThreshold_ = getOffHeapHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  //Sketch

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    final int lgNomLongs = getLgNomLongs();
    final int preambleLongs = wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F;
    if (getRetainedEntries(true) > (1 << lgNomLongs)) {
      quickSelectAndRebuild(wseg_, preambleLongs, lgNomLongs);
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
    final int preambleLongs = wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F;
    final int preBytes = preambleLongs << 3;
    wseg_.asSlice(preBytes, arrLongs * 8L).fill((byte)0);
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    wseg_.set(JAVA_BYTE, FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    wseg_.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, 0);
    final float p = wseg_.get(JAVA_FLOAT_UNALIGNED, P_FLOAT);
    final long thetaLong = (long) (p * LONG_MAX_VALUE_AS_DOUBLE);
    wseg_.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong);
  }

  //restricted methods

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    HashOperations.checkHashCorruption(hash);

    wseg_.set(JAVA_BYTE, FLAGS_BYTE, (byte) (wseg_.get(JAVA_BYTE, FLAGS_BYTE) & ~EMPTY_FLAG_MASK));
    final long thetaLong = getThetaLong();
    final int lgNomLongs = getLgNomLongs();
    //The over-theta test
    if (HashOperations.continueCondition(thetaLong, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta or zero.
    }

    final int lgArrLongs = getLgArrLongs();
    final int preambleLongs = wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F;

    //The duplicate test
    final int index =
        HashOperations.hashSearchOrInsertMemorySegment(wseg_, lgArrLongs, hash, preambleLongs << 3);
    if (index >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, increment curCount
    final int curCount = getRetainedEntries(true) + 1;
    wseg_.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, curCount); //update curCount

    if (isOutOfSpace(curCount)) { //we need to do something, we are out of space

      if (lgArrLongs > lgNomLongs) { //at full size, rebuild
        //Assumes no dirty values, changes thetaLong, curCount_
        assert (lgArrLongs == (lgNomLongs + 1)) : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs;
        //rebuild, refresh curCount based on # values in the hashtable.
        quickSelectAndRebuild(wseg_, preambleLongs, lgNomLongs);
        return InsertedCountIncrementedRebuilt;
      } //end of rebuild, exit

      else { //Not at full size, resize. Should not get here if lgRF = 0 and segCap is too small.
        final int lgRF = getLgRF();
        final int actLgRF = actLgResizeFactor(wseg_.byteSize(), lgArrLongs, preambleLongs, lgRF);
        int tgtLgArrLongs = Math.min(lgArrLongs + actLgRF, lgNomLongs + 1);

        if (actLgRF > 0) { //Expand in current MemorySegment
          //lgArrLongs will change; thetaLong, curCount will not
          resize(wseg_, preambleLongs, lgArrLongs, tgtLgArrLongs);
          hashTableThreshold_ = getOffHeapHashTableThreshold(lgNomLongs, tgtLgArrLongs);
          return InsertedCountIncrementedResized;
        } //end of Expand in current MemorySegment, exit.

        else {
          //Request more space, then resize. lgArrLongs will change; thetaLong, curCount will not
          final int preBytes = preambleLongs << 3;
          tgtLgArrLongs = Math.min(lgArrLongs + lgRF, lgNomLongs + 1);
          final int tgtArrBytes = 8 << tgtLgArrLongs;
          final int reqBytes = tgtArrBytes + preBytes;
          final MemorySegment newDstSeg = MemorySegment.ofArray(new byte[reqBytes]);

          moveAndResize(wseg_, preambleLongs, lgArrLongs, newDstSeg, tgtLgArrLongs, thetaLong);
          wseg_ = newDstSeg;

          hashTableThreshold_ = getOffHeapHashTableThreshold(lgNomLongs, tgtLgArrLongs);
          return InsertedCountIncrementedResized;
        } //end of Request more space to resize
      } //end of resize
    } //end of isOutOfSpace
    return InsertedCountIncremented;
  }

}
