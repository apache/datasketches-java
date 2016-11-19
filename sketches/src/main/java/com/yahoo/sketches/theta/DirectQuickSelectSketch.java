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
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractP;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.Rebuilder.actLgResizeFactor;
import static com.yahoo.sketches.theta.Rebuilder.moveAndResize;
import static com.yahoo.sketches.theta.Rebuilder.quickSelectAndRebuild;
import static com.yahoo.sketches.theta.Rebuilder.resize;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRequest;
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

  //These values may be accessed on every update, thus are kept on-heap for speed.
  private final int preambleLongs_;
  private int lgArrLongs_;
  private int hashTableThreshold_; //only on heap, never serialized.
  private int curCount_;           //use setCurCount()
  private long thetaLong_;         //use setThetaLong()
  private boolean empty_;

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
   * Get a new sketch instance using the given Memory as its backing store.
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
  static DirectQuickSelectSketch getInstance(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf, final Memory dstMem, final boolean unionGadget) {

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
    final int minReqBytes = PreambleUtil.getMemBytes(lgArrLongs, preambleLongs);

    //Make sure Memory is large enough
    final long curMemCapBytes = dstMem.getCapacity();
    if (curMemCapBytes < minReqBytes) {
      throw new SketchesArgumentException(
        "Memory capacity is too small: " + curMemCapBytes + " < " + minReqBytes);
    }
    final int curCount = 0;
    //@formatter:off
    //Build preamble
    long pre0, pre1;
    final long thetaLong;
    pre0 = insertPreLongs(preambleLongs, 0L);                   //byte 0
    pre0 = insertResizeFactor(lgRF, pre0);                      //byte 0
    pre0 = insertSerVer(SER_VER, pre0);                         //byte 1
    pre0 = insertFamilyID(family.getID(), pre0);                //byte 2
    pre0 = insertLgNomLongs(lgNomLongs, pre0);                  //byte 3
    pre0 = insertLgArrLongs(lgArrLongs, pre0);                  //byte 4
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true : 00100 = 4
    pre0 = insertFlags(EMPTY_FLAG_MASK, pre0);                  //byte 5
    pre0 = insertSeedHash(Util.computeSeedHash(seed), pre0);    //bytes 6,7
    pre1 = curCount;                                            //bytes 8-11
    pre1 = insertP(p, pre1);                                    //bytes 12-15
    thetaLong = (long)(p * MAX_THETA_LONG_AS_DOUBLE);           //bytes 16-23
    //@formatter:on
    //Insert preamble into Memory, only responsible for first 3 longs
    final long[] preArr = {pre0, pre1, thetaLong};
    dstMem.putLongArray(0, preArr, 0, 3);

    //clear hash table area
    dstMem.clear(preambleLongs << 3, 8 << lgArrLongs);

    final DirectQuickSelectSketch dqss =
        new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, preambleLongs);
    dqss.lgArrLongs_ = lgArrLongs;
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    dqss.curCount_ = curCount;
    dqss.thetaLong_ = thetaLong;
    dqss.empty_ = true;
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
  static DirectQuickSelectSketch getInstance(final Memory srcMem, final long seed) {
    final long[] preArr = new long[3];
    srcMem.getLongArray(0, preArr, 0, 3); //extract the preamble
    final long long0 = preArr[0];
    final int preambleLongs = extractPreLongs(long0);                           //byte 0
    final ResizeFactor myRF = ResizeFactor.getRF(extractResizeFactor(long0));   //byte 0
    final int serVer = extractSerVer(long0);                                    //byte 1
    final int familyID = extractFamilyID(long0);                                //byte 2
    final int lgNomLongs = extractLgNomLongs(long0);                            //byte 3
    final int lgArrLongs = extractLgArrLongs(long0);                            //byte 4
    final int flags = extractFlags(long0);                                      //byte 5
    final short seedHash = (short)extractSeedHash(long0);                       //byte 6,7
    final long long1 = preArr[1];
    final int curCount = extractCurCount(long1);                                //bytes 8-11
    final float p = extractP(long1);                                            //bytes 12-15
    final long thetaLong = preArr[2];                                           //bytes 16-23

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
    final int minReqBytes = PreambleUtil.getMemBytes(lgArrLongs, preambleLongs);
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
    dqss.lgArrLongs_ = lgArrLongs;
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    dqss.curCount_ = curCount;
    dqss.thetaLong_ = thetaLong;
    dqss.empty_ = (flags & EMPTY_FLAG_MASK) > 0;
    dqss.mem_ = srcMem;
    return dqss;
  }

  //Sketch

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  @Override
  public byte[] toByteArray() { //MY_FAMILY is stored in mem_
    final int lengthBytes = (preambleLongs_ + (1 << lgArrLongs_)) << 3;
    final byte[] byteArray = new byte[lengthBytes];
    final Memory mem = new NativeMemory(byteArray);
    NativeMemory.copy(mem_, 0, mem, 0, lengthBytes);
    return byteArray;
  }

  @Override
  public Family getFamily() {
    final int familyID = mem_.getByte(FAMILY_BYTE);
    return Family.idToFamily(familyID);
  }



  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    if (getRetainedEntries(true) > (1 << getLgNomLongs())) {
      quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_, lgArrLongs_, curCount_);
      //Reset local variables
      curCount_ = mem_.getInt(RETAINED_ENTRIES_INT);
      thetaLong_ = mem_.getLong(THETA_LONG);
    }
    return this;
  }

  @Override
  public final void reset() {
    //clear hash table
    //hash table size and hashTableThreshold stays the same
    //lgArrLongs stays the same
    final int arrLongs = 1 << getLgArrLongs();
    final int preBytes = preambleLongs_ << 3;
    mem_.clear(preBytes, arrLongs * 8); //clear data array
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);       //byte 5
    empty_ = true;
    curCount_ = setCurCount(mem_, 0);
    final float p = mem_.getFloat(P_FLOAT);
    thetaLong_ = setThetaLong(mem_, (long)(p * MAX_THETA_LONG_AS_DOUBLE));
  }

  //restricted methods

  @Override
  int getPreambleLongs() {
    return preambleLongs_;
  }

  @Override
  long[] getCache() {
    final long[] cacheArr = new long[1 << lgArrLongs_];
    final Memory mem = new NativeMemory(cacheArr);
    NativeMemory.copy(mem_, preambleLongs_ << 3, mem, 0, 8 << lgArrLongs_);
    return cacheArr;
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

  @Override
  boolean isDirty() {
    return false; //Always false for QuickSelectSketch
  }

  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    HashOperations.checkHashCorruption(hash);

    if (empty_) {
      mem_.clearBits(FLAGS_BYTE, (byte)EMPTY_FLAG_MASK);
      empty_ = false;
    }

    //The over-theta test
    if (HashOperations.continueCondition(thetaLong_, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }

    //The duplicate test
    if (HashOperations.hashSearchOrInsert(mem_, lgArrLongs_, hash, preambleLongs_ << 3) >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, increment curCount
    mem_.putInt(RETAINED_ENTRIES_INT, ++curCount_); //update curCount

    if (curCount_ > hashTableThreshold_) { //we need to do something, we are out of space

      if (lgArrLongs_ > lgNomLongs_) { //at full size, rebuild
        //Assumes no dirty values, changes thetaLong_, curCount_
        assert
          (lgArrLongs_ == lgNomLongs_ + 1) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
        quickSelectAndRebuild(mem_, preambleLongs_, lgNomLongs_, lgArrLongs_, curCount_);  //rebuild
        curCount_ = mem_.getInt(RETAINED_ENTRIES_INT);
        thetaLong_ = mem_.getLong(THETA_LONG);
      } //end of rebuild

      else { //Not at full size, resize. Should not get here if lgRF = 0 and memCap is too small.
        final int lgRF = getLgResizeFactor();
        final int actLgRF = actLgResizeFactor(mem_.getCapacity(), lgArrLongs_, preambleLongs_, lgRF);
        int tgtLgArrLongs = Math.min(lgArrLongs_ + actLgRF, lgNomLongs_ + 1);
        if (actLgRF > 0) { //Expand in current Memory
          resize(mem_, preambleLongs_, lgArrLongs_, tgtLgArrLongs);
          //update locals
          lgArrLongs_ = mem_.getByte(LG_ARR_LONGS_BYTE);
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
        } //end of Expand in current memory

        else { //Request more memory, then resize
          final int preBytes = preambleLongs_ << 3;
          tgtLgArrLongs = Math.min(lgArrLongs_ + lgRF, lgNomLongs_ + 1);
          final int tgtArrBytes = 8 << tgtLgArrLongs;
          final int reqBytes = tgtArrBytes + preBytes;
          //if (tgtArrBytes < 2*(curMemCap));

          final MemoryRequest memReq = mem_.getMemoryRequest();
          final Memory dstMem = memReq.request(reqBytes);
          if (dstMem == null) { //returned a null
            throw new SketchesArgumentException("MemoryRequest callback cannot be null.");
          }
          final long newCap = dstMem.getCapacity();
          if (newCap < reqBytes) {
            memReq.free(dstMem);
            throw new SketchesArgumentException("Requested memory not granted: " + newCap + " < "
                + reqBytes);
          }
          moveAndResize(mem_, preambleLongs_, lgArrLongs_, dstMem, tgtLgArrLongs, thetaLong_);

          memReq.free(mem_, dstMem); //normal free mechanism via MemoryRequest
          mem_ = dstMem;
          lgArrLongs_ = mem_.getByte(LG_ARR_LONGS_BYTE);
          hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
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

  private static final long setThetaLong(final Memory mem, final long newThetaLong) {
    mem.putLong(THETA_LONG, newThetaLong);
    return newThetaLong;
  }

  private static final int setCurCount(final Memory mem, final int newCurCount) {
    mem.putInt(RETAINED_ENTRIES_INT, newCurCount);
    return newCurCount;
  }

}
