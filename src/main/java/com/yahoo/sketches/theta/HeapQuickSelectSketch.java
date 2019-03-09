/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.Util.MIN_LG_ARR_LONGS;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.Util.RESIZE_THRESHOLD;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.extractP;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;
import static java.lang.Math.max;
import static java.lang.Math.min;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapQuickSelectSketch extends HeapUpdateSketch {
  private final Family MY_FAMILY;

  private final int preambleLongs_;
  private int lgArrLongs_;
  private int hashTableThreshold_;  //never serialized
  int curCount_;
  long thetaLong_;
  boolean empty_;

  private long[] cache_;

  private HeapQuickSelectSketch(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf, final int preambleLongs, final Family family) {
    super(lgNomLongs, seed, p, rf);
    preambleLongs_ = preambleLongs;
    MY_FAMILY = family;
  }

  /**
   * Construct a new sketch instance on the java heap.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param unionGadget true if this sketch is implementing the Union gadget function.
   * Otherwise, it is behaving as a normal QuickSelectSketch.
   */
  HeapQuickSelectSketch(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf, final boolean unionGadget) {
    super(lgNomLongs, seed, p, rf);

    //Choose family, preambleLongs
    if (unionGadget) {
      preambleLongs_ = Family.UNION.getMinPreLongs();
      MY_FAMILY = Family.UNION;
    }
    else {
      preambleLongs_ = Family.QUICKSELECT.getMinPreLongs();
      MY_FAMILY = Family.QUICKSELECT;
    }

    lgArrLongs_ = Util.startingSubMultiple(lgNomLongs + 1, rf, MIN_LG_ARR_LONGS);
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs_);
    curCount_ = 0;
    thetaLong_ = (long)(p * MAX_THETA_LONG_AS_DOUBLE);
    empty_ = true; //other flags: bigEndian = readOnly = compact = ordered = false;
    cache_ = new long[1 << lgArrLongs_];
  }

  /**
   * Heapify a sketch from a Memory UpdateSketch or Union object
   * containing sketch data.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return instance of this sketch
   */
  static HeapQuickSelectSketch heapifyInstance(final Memory srcMem, final long seed) {
    final int preambleLongs = extractPreLongs(srcMem);            //byte 0
    final int lgNomLongs = extractLgNomLongs(srcMem);             //byte 3
    final int lgArrLongs = extractLgArrLongs(srcMem);             //byte 4

    checkUnionQuickSelectFamily(srcMem, preambleLongs, lgNomLongs);
    checkMemIntegrity(srcMem, seed, preambleLongs, lgNomLongs, lgArrLongs);

    final float p = extractP(srcMem);                             //bytes 12-15
    final int lgRF = extractLgResizeFactor(srcMem);               //byte 0
    ResizeFactor myRF = ResizeFactor.getRF(lgRF);
    final int familyID = extractFamilyID(srcMem);
    final Family family = Family.idToFamily(familyID);

    if ((myRF == ResizeFactor.X1)
            && (lgArrLongs != Util.startingSubMultiple(lgNomLongs + 1, myRF, MIN_LG_ARR_LONGS))) {
      myRF = ResizeFactor.X2;
    }

    final HeapQuickSelectSketch hqss = new HeapQuickSelectSketch(lgNomLongs, seed, p, myRF,
        preambleLongs, family);
    hqss.lgArrLongs_ = lgArrLongs;
    hqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    hqss.curCount_ = extractCurCount(srcMem);
    hqss.thetaLong_ = extractThetaLong(srcMem);
    hqss.empty_ = PreambleUtil.isEmpty(srcMem);
    hqss.cache_ = new long[1 << lgArrLongs];
    srcMem.getLongArray(preambleLongs << 3, hqss.cache_, 0, 1 << lgArrLongs); //read in as hash table
    return hqss;
  }

  //Sketch

  @Override
  public Family getFamily() {
    return MY_FAMILY;
  }

  @Override
  public HashIterator iterator() {
    return new HeapHashIterator(cache_, 1 << lgArrLongs_, thetaLong_);
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public long getThetaLong() {
    return thetaLong_;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  @Override
  public byte[] toByteArray() {
    return toByteArray(preambleLongs_, (byte) MY_FAMILY.getID());
  }

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    if (getRetainedEntries(true) > (1 << getLgNomLongs())) {
      quickSelectAndRebuild();
    }
    return this;
  }

  @Override
  public void reset() {
    final ResizeFactor rf = getResizeFactor();
    final int lgArrLongsSM = Util.startingSubMultiple(lgNomLongs_ + 1, rf, MIN_LG_ARR_LONGS);
    if (lgArrLongsSM == lgArrLongs_) {
      final int arrLongs = cache_.length;
      assert (1 << lgArrLongs_) == arrLongs;
      java.util.Arrays.fill(cache_,  0L);
    }
    else {
      cache_ = new long[1 << lgArrLongsSM];
      lgArrLongs_ = lgArrLongsSM;
    }
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    empty_ = true;
    curCount_ = 0;
    thetaLong_ =  (long)(getP() * MAX_THETA_LONG_AS_DOUBLE);
  }

  //restricted methods

  @Override
  long[] getCache() {
    return cache_;
  }

  @Override
  int getCurrentPreambleLongs(final boolean compact) {
    if (!compact) { return preambleLongs_; }
    return computeCompactPreLongs(thetaLong_, empty_, curCount_);
  }

  //only used by ConcurrentHeapThetaBuffer & Test
  int getHashTableThreshold() {
    return hashTableThreshold_;
  }

  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }

  @Override
  WritableMemory getMemory() {
    return null;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    HashOperations.checkHashCorruption(hash);
    empty_ = false;

    //The over-theta test
    if (HashOperations.continueCondition(thetaLong_, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }

    //The duplicate test
    if (HashOperations.hashSearchOrInsert(cache_, lgArrLongs_, hash) >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, must increment curCount
    curCount_++;

    if (isOutOfSpace(curCount_)) { //we need to do something, we are out of space
      //must rebuild or resize
      if (lgArrLongs_ <= lgNomLongs_) { //resize
        resizeCache();
      }
      else { //Already at tgt size, must rebuild
        assert (lgArrLongs_ == (lgNomLongs_ + 1)) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
        quickSelectAndRebuild(); //Changes thetaLong_, curCount_, reassigns cache
      }
    }
    return InsertedCountIncremented;
  }

  @Override
  boolean isDirty() {
    return false;
  }

  @Override
  boolean isOutOfSpace(final int numEntries) {
    return numEntries > hashTableThreshold_;
  }

  //Must resize. Changes lgArrLongs_ and cache_. theta and count don't change.
  // Used by hashUpdate()
  private final void resizeCache() {
    final ResizeFactor rf = getResizeFactor();
    final int lgTgtLongs = lgNomLongs_ + 1;
    final int lgDeltaLongs = lgTgtLongs - lgArrLongs_;
    final int lgResizeFactor = max(min(rf.lg(), lgDeltaLongs), 1); //rf_.lg() could be 0
    lgArrLongs_ += lgResizeFactor; // new tgt size

    final long[] tgtArr = new long[1 << lgArrLongs_];
    final int newCount = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);

    assert newCount == curCount_;  //Assumes no dirty values.
    curCount_ = newCount;

    cache_ = tgtArr;
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
  }

  //array stays the same size. Changes theta and thus count
  private final void quickSelectAndRebuild() {
    final int arrLongs = 1 << lgArrLongs_;

    final int pivot = (1 << lgNomLongs_) + 1; // pivot for QS

    thetaLong_ = selectExcludingZeros(cache_, curCount_, pivot); //messes up the cache_

    // now we rebuild to clean up dirty data, update count, reconfigure as a hash table
    final long[] tgtArr = new long[arrLongs];
    curCount_ = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
    cache_ = tgtArr;
    //hashTableThreshold stays the same
  }

  /**
   * Returns the cardinality limit given the current size of the hash table array.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    final double fraction = (lgArrLongs <= lgNomLongs) ? RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }

}
