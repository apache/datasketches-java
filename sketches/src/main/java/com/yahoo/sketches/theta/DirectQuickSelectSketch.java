/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_RESIZE_FACTOR_BIT;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.extractP;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;
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

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryUtil;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * The default Theta Sketch using the QuickSelect algorithm.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DirectQuickSelectSketch extends DirectUpdateSketch {
  private static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space

  //These values may be accessed on every update, thus are also kept on-heap for speed.
  private final int preambleLongs_;
  private int hashTableThreshold_; //only on heap, never serialized.
  private Memory mem_;

  private DirectQuickSelectSketch(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf, final int preambleLongs) {
    super(lgNomLongs,
        seed,
        p,
        rf
    );
    preambleLongs_ = preambleLongs;
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
      final float p, final ResizeFactor rf, final Memory dstMem, final boolean unionGadget) {

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
    final Object memObj = dstMem.array(); //may be null
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
        new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, preambleLongs);

    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    dqss.mem_ = dstMem;
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
  static DirectQuickSelectSketch wrapInstance(final Memory srcMem, final long seed) {
    final int preambleLongs;
    final ResizeFactor myRF;
    final int serVer;
    final int familyID;
    final int lgNomLongs;
    final int lgArrLongs;
    final int flags;
    final short seedHash;
    final float p;
    final long thetaLong;
    if (srcMem.isReadOnly() && !srcMem.isDirect()) {
      preambleLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
      myRF = ResizeFactor.getRF((srcMem.getByte(PREAMBLE_LONGS_BYTE) >> LG_RESIZE_FACTOR_BIT) & 0X3);
      serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
      familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
      lgNomLongs = srcMem.getByte(LG_NOM_LONGS_BYTE) & 0XFF;
      lgArrLongs = srcMem.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
      flags = srcMem.getByte(FLAGS_BYTE) & 0XFF;
      seedHash = srcMem.getShort(SEED_HASH_SHORT);
      p = srcMem.getFloat(P_FLOAT);
      thetaLong = srcMem.getLong(THETA_LONG);
    } else {
      final Object memObj = srcMem.array(); //may be null
      final long memAdd = srcMem.getCumulativeOffset(0L);

      preambleLongs = extractPreLongs(memObj, memAdd);                  //byte 0
      myRF = ResizeFactor.getRF(extractLgResizeFactor(memObj, memAdd)); //byte 0
      serVer = extractSerVer(memObj, memAdd);                           //byte 1
      familyID = extractFamilyID(memObj, memAdd);                       //byte 2
      lgNomLongs = extractLgNomLongs(memObj, memAdd);                   //byte 3
      lgArrLongs = extractLgArrLongs(memObj, memAdd);                   //byte 4
      flags = extractFlags(memObj, memAdd);                             //byte 5
      seedHash = (short)extractSeedHash(memObj, memAdd);                //byte 6,7
      p = extractP(memObj, memAdd);                                     //bytes 12-15
      thetaLong = extractThetaLong(memObj, memAdd);                     //bytes 16-23
    }

    if (serVer != SER_VER) {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Serialization Version: " + serVer);
    }

    final Family family = Family.idToFamily(familyID);
    if (family.equals(Family.UNION)) {
      if (preambleLongs != Family.UNION.getMinPreLongs()) {
        throw new SketchesArgumentException(
            "Possible corruption: Invalid PreambleLongs value for UNION: " + preambleLongs);
      }
    }
    else if (family.equals(Family.QUICKSELECT)) {
      if (preambleLongs != Family.QUICKSELECT.getMinPreLongs()) {
        throw new SketchesArgumentException(
            "Possible corruption: Invalid PreambleLongs value for QUICKSELECT: " + preambleLongs);
      }
    } else {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }

    if (lgNomLongs < MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException(
          "Possible corruption: Current Memory lgNomLongs < min required size: "
              + lgNomLongs + " < " + MIN_LG_NOM_LONGS);
    }

    final int flagsMask =
        ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
    if ((flags & flagsMask) > 0) {
      throw new SketchesArgumentException(
        "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
    }

    Util.checkSeedHashes(seedHash, Util.computeSeedHash(seed));

    final long curCapBytes = srcMem.getCapacity();
    final int minReqBytes = getMemBytes(lgArrLongs, preambleLongs);
    if (curCapBytes < minReqBytes) {
      throw new SketchesArgumentException(
          "Possible corruption: Current Memory size < min required size: "
              + curCapBytes + " < " + minReqBytes);
    }

    final double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
    if ((lgArrLongs <= lgNomLongs) && (theta < p) ) {
      throw new SketchesArgumentException(
        "Possible corruption: Theta cannot be < p and lgArrLongs <= lgNomLongs. "
            + lgArrLongs + " <= " + lgNomLongs + ", Theta: " + theta + ", p: " + p);
    }

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(lgNomLongs, seed, p, myRF, preambleLongs);
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    dqss.mem_ = srcMem;
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
  static DirectQuickSelectSketch fastWrap(final Memory srcMem, final long seed) {
    final int preambleLongs;
    final ResizeFactor myRF;
    final int lgNomLongs;
    final int lgArrLongs;
    final float p;

    if (srcMem.isReadOnly() && !srcMem.isDirect()) { //Read-Only Heap
      preambleLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
      myRF = ResizeFactor.getRF((srcMem.getByte(PREAMBLE_LONGS_BYTE) >> LG_RESIZE_FACTOR_BIT) & 0X3);
      lgNomLongs = srcMem.getByte(LG_NOM_LONGS_BYTE) & 0XFF;
      lgArrLongs = srcMem.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
      p = srcMem.getFloat(P_FLOAT);

    } else {
      final Object memObj = srcMem.array(); //may be null
      final long memAdd = srcMem.getCumulativeOffset(0L);
      preambleLongs = extractPreLongs(memObj, memAdd);                  //byte 0
      myRF = ResizeFactor.getRF(extractLgResizeFactor(memObj, memAdd)); //byte 0
      lgNomLongs = extractLgNomLongs(memObj, memAdd);                   //byte 3
      lgArrLongs = extractLgArrLongs(memObj, memAdd);                   //byte 4
      p = extractP(memObj, memAdd);                                     //bytes 12-15
    }

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(lgNomLongs, seed, p, myRF, preambleLongs);
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    dqss.mem_ = srcMem;
    return dqss;
  }

  //Sketch

  @Override
  public int getRetainedEntries(final boolean valid) {
    return mem_.getInt(RETAINED_ENTRIES_INT);
  }

  @Override
  public boolean isEmpty() {
    return (mem_.getByte(FLAGS_BYTE) & EMPTY_FLAG_MASK) > 0;
  }

  @Override
  public byte[] toByteArray() { //MY_FAMILY is stored in mem_
    final byte lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE);
    final int lengthBytes = (preambleLongs_ + (1 << lgArrLongs)) << 3;
    final byte[] byteArray = new byte[lengthBytes];
    final Memory mem = new NativeMemory(byteArray);
    mem_.copy(0, mem, 0, lengthBytes);
    return byteArray;
  }

  @Override
  public Family getFamily() {
    final int familyID = mem_.getByte(FAMILY_BYTE) & 0XFF;
    return Family.idToFamily(familyID);
  }

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
  int getPreambleLongs() {
    return preambleLongs_;
  }

  @Override
  long[] getCache() {
    final long lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
    final long[] cacheArr = new long[1 << lgArrLongs];
    final Memory mem = new NativeMemory(cacheArr);
    mem_.copy(preambleLongs_ << 3, mem, 0, 8 << lgArrLongs);
    return cacheArr;
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  @Override
  long getThetaLong() {
    return mem_.getLong(THETA_LONG);
  }

  @Override
  boolean isDirty() {
    return false; //Always false for QuickSelectSketch
  }

  @Override
  int getLgArrLongs() {
    return mem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
  }

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
    final int index;
    if (mem_.isReadOnly() && !mem_.isDirect()) {
      index = HashOperations.hashSearchOrInsert(mem_, lgArrLongs, hash, preambleLongs_ << 3);
    } else {
      index = HashOperations.fastHashSearchOrInsert(
          mem_.array(), mem_.getCumulativeOffset(0L), lgArrLongs, hash, preambleLongs_ << 3);
    }
    if (index >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, increment curCount
    final int curCount = getRetainedEntries() + 1;
    mem_.putInt(RETAINED_ENTRIES_INT, curCount); //update curCount

    if (curCount > hashTableThreshold_) { //we need to do something, we are out of space

      if (lgArrLongs > lgNomLongs_) { //at full size, rebuild
        //Assumes no dirty values, changes thetaLong, curCount_
        assert (lgArrLongs == lgNomLongs_ + 1)
            : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs_;
        //rebuild, refresh curCount based on # values in the hashtable.
        quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_);
      } //end of rebuild, exit

      else { //Not at full size, resize. Should not get here if lgRF = 0 and memCap is too small.
        final int lgRF = getLgResizeFactor();
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

          final Memory newDstMem = MemoryUtil.memoryRequestHandler(mem_, reqBytes, false);

          moveAndResize(mem_, preambleLongs_, lgArrLongs, newDstMem, tgtLgArrLongs, thetaLong);
          mem_.getMemoryRequest().free(mem_, newDstMem); //normal free mechanism via MemoryRequest

          mem_ = newDstMem;
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, tgtLgArrLongs);
        } //end of Request more memory to resize
      } //end of resize
    }
    return InsertedCountIncremented;
  }

  //special set methods

  /**
   * Returns the cardinality limit given the current size of the hash table array.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  private static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    //FindBugs may complain if DQS_RESIZE_THRESHOLD == REBUILD_THRESHOLD, but this allows us
    // to tune these constants for different sketches.
    final double fraction = (lgArrLongs <= lgNomLongs) ? DQS_RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }

}
