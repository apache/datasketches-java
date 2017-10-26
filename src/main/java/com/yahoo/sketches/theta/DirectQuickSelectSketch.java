/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.getMemBytes;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertThetaLong;
import static com.yahoo.sketches.theta.Rebuilder.actLgResizeFactor;
import static com.yahoo.sketches.theta.Rebuilder.moveAndResize;
import static com.yahoo.sketches.theta.Rebuilder.quickSelectAndRebuild;
import static com.yahoo.sketches.theta.Rebuilder.resize;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;

import com.yahoo.memory.MemoryRequestServer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

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
final class DirectQuickSelectSketch extends DirectQuickSelectSketchR {

  private DirectQuickSelectSketch(final int lgNomLongs, final long seed, final int preambleLongs,
          final WritableMemory wmem) {
    super(Math.max(lgNomLongs, MIN_LG_NOM_LONGS), seed, preambleLongs, wmem);
  }

  /**
   * Get a new sketch instance and initialize the given Memory as its backing store.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param p
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf Currently internally fixed at 2. Unless dstMem is not configured with a valid
   * MemoryRequest, in which case the rf is effectively 1, which is no resizing at all and the
   * dstMem must be large enough for a full sketch.
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstMem the given Memory object destination. It cannot be null.
   * It will be cleared prior to use.
   * @param unionGadget true if this sketch is implementing the Union gadget function.
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   * @return instance of this sketch
   */
  static DirectQuickSelectSketch initNewDirectInstance(final int lgNomLongs, final long seed,
      final float p, final ResizeFactor rf, final WritableMemory dstMem, final boolean unionGadget) {

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
    final Object memObj = dstMem.getArray(); //may be null
    final long memAdd = dstMem.getCumulativeOffset(0L);

    insertPreLongs(memObj, memAdd, preambleLongs);                 //byte 0
    insertLgResizeFactor(memObj, memAdd, lgRF);                    //byte 0
    insertSerVer(memObj, memAdd, SER_VER);                         //byte 1
    insertFamilyID(memObj, memAdd, family.getID());                //byte 2
    insertLgNomLongs(memObj, memAdd, lgNomLongs);                  //byte 3
    insertLgArrLongs(memObj, memAdd, lgArrLongs);                  //byte 4
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true : 00100 = 4
    insertFlags(memObj, memAdd, EMPTY_FLAG_MASK);                  //byte 5
    insertSeedHash(memObj, memAdd, Util.computeSeedHash(seed));    //bytes 6,7
    insertCurCount(memObj, memAdd, 0);                             //bytes 8-11
    insertP(memObj, memAdd, p);                                    //bytes 12-15
    final long thetaLong = (long)(p * MAX_THETA_LONG_AS_DOUBLE);
    insertThetaLong(memObj, memAdd, thetaLong);                    //bytes 16-23
    //@formatter:on

    //clear hash table area
    dstMem.clear(preambleLongs << 3, 8 << lgArrLongs);

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(lgNomLongs, seed, preambleLongs, dstMem);

    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
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
    final Object memObj = srcMem.getArray(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    final int preambleLongs = extractPreLongs(memObj, memAdd);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(memObj, memAdd);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(memObj, memAdd);                   //byte 4

    UpdateSketch.checkUnionQuickSelectFamily(memObj, memAdd, preambleLongs, lgNomLongs);
    checkMemIntegrity(srcMem, memObj, memAdd, seed, preambleLongs, lgNomLongs, lgArrLongs);

    final int lgRF = extractLgResizeFactor(memObj, memAdd);               //byte 0
    final ResizeFactor myRF = ResizeFactor.getRF(lgRF);
    if ((myRF == ResizeFactor.X1)
            && (lgArrLongs != Util.startingSubMultiple(lgNomLongs + 1, myRF, MIN_LG_ARR_LONGS))) {
      insertLgResizeFactor(memObj, memAdd, ResizeFactor.X2.lg());
    }

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(lgNomLongs, seed, preambleLongs, srcMem);
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
    final Object memObj = srcMem.getArray();
    final long memAdd = srcMem.getCumulativeOffset(0L);

    final int preambleLongs = extractPreLongs(memObj, memAdd);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(memObj, memAdd);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(memObj, memAdd);                   //byte 4

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(lgNomLongs, seed, preambleLongs, srcMem);
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  //Sketch


  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    if (getRetainedEntries(true) > (1 << getLgNomLongs())) {
      quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_);
    }
    return this;
  }

  @Override
  public final void reset() {
    //clear hash table
    //hash table size and hashTableThreshold stays the same
    //lgArrLongs stays the same
    //thetaLongs resets to p
    final int arrLongs = 1 << getLgArrLongs();
    final int preBytes = preambleLongs_ << 3;
    mem_.clear(preBytes, arrLongs * 8); //clear data array
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    mem_.putInt(RETAINED_ENTRIES_INT, 0);
    final float p = mem_.getFloat(P_FLOAT);
    final long thetaLong = (long) (p * MAX_THETA_LONG_AS_DOUBLE);
    mem_.putLong(THETA_LONG, thetaLong);
  }

  //restricted methods

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    HashOperations.checkHashCorruption(hash);

    mem_.putByte(FLAGS_BYTE, (byte) (mem_.getByte(FLAGS_BYTE) & ~EMPTY_FLAG_MASK));
    final long thetaLong = getThetaLong();

    //The over-theta test
    if (HashOperations.continueCondition(thetaLong, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }

    final int lgArrLongs = getLgArrLongs();

    //The duplicate test
    final int index =
        HashOperations.fastHashSearchOrInsert(mem_, lgArrLongs, hash, preambleLongs_ << 3);
    if (index >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, increment curCount
    final int curCount = getRetainedEntries() + 1;
    mem_.putInt(RETAINED_ENTRIES_INT, curCount); //update curCount

    if (curCount > hashTableThreshold_) { //we need to do something, we are out of space

      if (lgArrLongs > lgNomLongs_) { //at full size, rebuild
        //Assumes no dirty values, changes thetaLong, curCount_
        assert (lgArrLongs == (lgNomLongs_ + 1))
            : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs_;
        //rebuild, refresh curCount based on # values in the hashtable.
        quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_);
      } //end of rebuild, exit

      else { //Not at full size, resize. Should not get here if lgRF = 0 and memCap is too small.
        final int lgRF = getLgRF();
        final int actLgRF = actLgResizeFactor(mem_.getCapacity(), lgArrLongs, preambleLongs_, lgRF);
        int tgtLgArrLongs = Math.min(lgArrLongs + actLgRF, lgNomLongs_ + 1);

        if (actLgRF > 0) { //Expand in current Memory
          //lgArrLongs will change; thetaLong, curCount will not
          resize(mem_, preambleLongs_, lgArrLongs, tgtLgArrLongs);
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, tgtLgArrLongs);
        } //end of Expand in current memory, exit.

        else {
          //Request more memory, then resize. lgArrLongs will change; thetaLong, curCount will not
          final int preBytes = preambleLongs_ << 3;
          tgtLgArrLongs = Math.min(lgArrLongs + lgRF, lgNomLongs_ + 1);
          final int tgtArrBytes = 8 << tgtLgArrLongs;
          final int reqBytes = tgtArrBytes + preBytes;

          final MemoryRequestServer memoryRequestServer = mem_.getMemoryRequestServer();
          final WritableMemory newDstMem = memoryRequestServer.request(reqBytes);

          moveAndResize(mem_, preambleLongs_, lgArrLongs, newDstMem, tgtLgArrLongs, thetaLong);

          mem_ = newDstMem;
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, tgtLgArrLongs);
        } //end of Request more memory to resize
      } //end of resize
    }
    return InsertedCountIncremented;
  }

}
