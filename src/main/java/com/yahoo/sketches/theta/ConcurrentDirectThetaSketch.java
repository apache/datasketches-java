/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_RESIZE_FACTOR_BIT;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
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
import static com.yahoo.sketches.theta.Rebuilder.quickSelectAndRebuild;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * @author eshcar
 * @author Lee Rhodes
 */
final class ConcurrentDirectThetaSketch extends UpdateSketch {
  static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  static ExecutorService propagationExecutorService;

  final long seed_;        //provided, kept only on heap, never serialized.
  int hashTableThreshold_; //computed, kept only on heap, never serialized.
  WritableMemory mem_;

  private volatile long volatileThetaLong_;
  private volatile double estimation_;
  // A flag to coordinate between several propagation threads
  private AtomicBoolean propagationInProgress_;

  private ConcurrentDirectThetaSketch(
      final long seed,
      final int hashTableThreshold,
      final WritableMemory dstMem,
      final int poolThreads) {
    if (propagationExecutorService == null) {
      propagationExecutorService = Executors.newWorkStealingPool(poolThreads);
    }
    seed_ = seed;
    hashTableThreshold_ = hashTableThreshold;
    volatileThetaLong_ = Long.MAX_VALUE;
    mem_ = dstMem;
    estimation_ = 0;
    propagationInProgress_ = new AtomicBoolean(false);
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
  static ConcurrentDirectThetaSketch initNewDirectInstance(
      final int lgNomLongs,
      final long seed,
      final WritableMemory dstMem,
      final int poolThreads) {

    final Family family = Family.QUICKSELECT;
    final int preambleLongs = Family.QUICKSELECT.getMinPreLongs();

    //Choose RF, minReqBytes, lgArrLongs.
    final int lgRF = 0;
    final int lgArrLongs = Math.max(lgNomLongs + 1, MIN_LG_ARR_LONGS);
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
    insertSeedHash(dstMem, Util.computeSeedHash(seed));    //bytes 6,7
    insertCurCount(dstMem, 0);                             //bytes 8-11
    insertP(dstMem, 1.0F);                                 //bytes 12-15
    final long thetaLong = Long.MAX_VALUE;
    insertThetaLong(dstMem, thetaLong);                    //bytes 16-23
    //@formatter:on

    //clear hash table area
    dstMem.clear(preambleLongs << 3, 8 << lgArrLongs);
    final int hashTableThreshold = setHashTableThreshold(lgNomLongs, lgArrLongs);
    final ConcurrentDirectThetaSketch cds =
        new ConcurrentDirectThetaSketch(seed, hashTableThreshold, dstMem, poolThreads);
    return cds;
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) {
    if (!compact) {
      final byte lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE);
      final int preambleLongs = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
      final int lengthBytes = (preambleLongs + (1 << lgArrLongs)) << 3;
      return lengthBytes;
    }
    final int preLongs = getCurrentPreambleLongs(true);
    final int curCount = getRetainedEntries(true);

    return (preLongs + curCount) << 3;
  }

  @Override
  public Family getFamily() {
    final int familyID = mem_.getByte(FAMILY_BYTE) & 0XFF;
    return Family.idToFamily(familyID);
  }

  @Override
  public ResizeFactor getResizeFactor() {
    return ResizeFactor.getRF(getLgRF());
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //always valid
    return mem_.getInt(RETAINED_ENTRIES_INT);
  }

  @Override
  public boolean hasMemory() {
    return true;
  }

  @Override
  public boolean isDirect() {
    return mem_.isDirect();
  }

  @Override
  public boolean isEmpty() {
    return (mem_.getByte(FLAGS_BYTE) & EMPTY_FLAG_MASK) > 0;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return mem_.isSameResource(that);
  }

  @Override
  public byte[] toByteArray() { //MY_FAMILY is stored in mem_ TODO DO WE NEED THIS?
    final byte lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE);
    final int preambleLongs = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int lengthBytes = (preambleLongs + (1 << lgArrLongs)) << 3;
    final byte[] byteArray = new byte[lengthBytes];
    final WritableMemory mem = WritableMemory.wrap(byteArray);
    mem_.copyTo(0, mem, 0, lengthBytes);
    return byteArray;
  }

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    final int lgNomLongs = getLgNomLongs();
    final int preambleLongs = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    if (getRetainedEntries(true) > (1 << lgNomLongs)) {
      quickSelectAndRebuild(mem_, preambleLongs, lgNomLongs);
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
    final int preambleLongs = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int preBytes = preambleLongs << 3;
    mem_.clear(preBytes, arrLongs * 8); //clear data array
    //flags: bigEndian = readOnly = compact = ordered = false; empty = true.
    mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    mem_.putInt(RETAINED_ENTRIES_INT, 0);
    final float p = mem_.getFloat(P_FLOAT);
    final long thetaLong = (long) (p * MAX_THETA_LONG_AS_DOUBLE);
    mem_.putLong(THETA_LONG, thetaLong);
  }

  @Override
  public int getLgNomLongs() {
    return PreambleUtil.extractLgNomLongs(mem_);
  }

  //restricted methods

  @Override
  long[] getCache() {
    final long lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
    final int preambleLongs = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final long[] cacheArr = new long[1 << lgArrLongs];
    final WritableMemory mem = WritableMemory.wrap(cacheArr);
    mem_.copyTo(preambleLongs << 3, mem, 0, 8 << lgArrLongs);
    return cacheArr;
  }

  @Override
  int getCurrentPreambleLongs(final boolean compact) {
    if (!compact) { return PreambleUtil.extractPreLongs(mem_); }
    return computeCompactPreLongs(getThetaLong(), isEmpty(), getRetainedEntries(true));
  }

  @Override
  WritableMemory getMemory() {
    return mem_;
  }

  @Override
  float getP() {
    return mem_.getFloat(P_FLOAT);
  }

  @Override
  long getSeed() {
    return seed_;
  }

  @Override
  short getSeedHash() {
    return (short) PreambleUtil.extractSeedHash(mem_);
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
  boolean isOutOfSpace(final int numEntries) {
    return numEntries > hashTableThreshold_;
  }

  @Override
  int getLgArrLongs() {
    return mem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
  }

  int getLgRF() {
    return (mem_.getByte(PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT) & 0X3;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    HashOperations.checkHashCorruption(hash);

    mem_.putByte(FLAGS_BYTE, (byte) (mem_.getByte(FLAGS_BYTE) & ~EMPTY_FLAG_MASK));
    final long thetaLong = getThetaLong();
    final int lgNomLongs = getLgNomLongs();
    //The over-theta test
    if (HashOperations.continueCondition(thetaLong, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }

    final int lgArrLongs = getLgArrLongs();
    final int preambleLongs = mem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;

    //The duplicate test
    final int index =
        HashOperations.fastHashSearchOrInsert(mem_, lgArrLongs, hash, preambleLongs << 3);
    if (index >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, increment curCount
    final int curCount = getRetainedEntries() + 1;
    mem_.putInt(RETAINED_ENTRIES_INT, curCount); //update curCount

    if (isOutOfSpace(curCount)) { //we need to do something, we are out of space
      //Must be at full size, rebuild
      //Assumes no dirty values, changes thetaLong, curCount_
      assert (lgArrLongs == (lgNomLongs + 1))
            : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs;
      //rebuild, refresh curCount based on # values in the hashtable.
      quickSelectAndRebuild(mem_, preambleLongs, lgNomLongs);
    }
    return InsertedCountIncremented;
  }

  /**
   * Returns the cardinality limit given the current size of the hash table array.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    //FindBugs may complain if DQS_RESIZE_THRESHOLD == REBUILD_THRESHOLD, but this allows us
    // to tune these constants for different sketches.
    final double fraction = (lgArrLongs <= lgNomLongs) ? DQS_RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }

  void setThetaLong(final long thetaLong) {
    mem_.putLong(THETA_LONG, thetaLong);
  }

  //Concurrent methods

  /**
   * Propogate the ConcurrentHeapThetaBuffer into this sketch
   * @param bufferIn the given ConcurrentHeapThetaBuffer
   * @param compactSketch an optional, ordered compact sketch with the data
   */
  public void propagate(final ConcurrentHeapThetaBuffer bufferIn,
      final HeapCompactOrderedSketch compactSketch) {
    final BackgroundThetaPropagation job =
        new BackgroundThetaPropagation(bufferIn, compactSketch, this);
    propagationExecutorService.execute(job);
  }

  public double getEstimationSnapshot() {
    return estimation_;
  }

  public long getVolatileTheta() {
    return volatileThetaLong_;
  }

  AtomicBoolean getPropogationInProgress() {
    return propagationInProgress_;
  }

  private static class BackgroundThetaPropagation implements Runnable {
    private ConcurrentHeapThetaBuffer bufferIn;
    private HeapCompactOrderedSketch compactSketch;
    private ConcurrentDirectThetaSketch shared;

    public BackgroundThetaPropagation(
        final ConcurrentHeapThetaBuffer bufferIn,
        final HeapCompactOrderedSketch compactSketch,
        final ConcurrentDirectThetaSketch shared) {
      this.bufferIn = bufferIn;
      this.compactSketch = compactSketch;
      this.shared = shared;
    }

    @Override
    public void run() {
      assert shared.getVolatileTheta() <= bufferIn.getThetaLong();

      while (!shared.propagationInProgress_.compareAndSet(false,true)) {
        //busy wait until we can propagate
      }
      //At this point we are sure only a single thread is propagating data to the shared sketch

      // propagate values from input sketch one by one
      if (compactSketch != null) { //Use early stop
        final long[] cacheIn = compactSketch.getCache();
        for (int i = 0; i < cacheIn.length; i++) {
          final long hashIn = cacheIn[i];
          if (hashIn >= shared.getVolatileTheta()) {
            break; //early stop
          }
          shared.hashUpdate(hashIn); // backdoor update, hash function is bypassed
        }
      } else {
        final long[] cacheIn = bufferIn.getCache();
        for (int i = 0; i < cacheIn.length; i++) {
          final long hashIn = cacheIn[i];
          shared.hashUpdate(hashIn); // backdoor update, hash function is bypassed
        }
      }

      //update volatile theta, uniques estimate and propagation flag
      final long sharedThetaLong = shared.getThetaLong();
      shared.volatileThetaLong_ = sharedThetaLong;
      shared.estimation_ = shared.getEstimate();
      bufferIn.reset();
      bufferIn.setThetaLong(sharedThetaLong);
      //propagation completed, not in-progress, reset shared flag
      shared.propagationInProgress_.set(false);
    }
  }

}
