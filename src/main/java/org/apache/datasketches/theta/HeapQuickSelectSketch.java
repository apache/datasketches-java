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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.QuickSelect.selectExcludingZeros;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractLgResizeFactor;
import static org.apache.datasketches.theta.PreambleUtil.extractP;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.UpdateReturnState.InsertedCountIncremented;
import static org.apache.datasketches.theta.UpdateReturnState.InsertedCountIncrementedRebuilt;
import static org.apache.datasketches.theta.UpdateReturnState.InsertedCountIncrementedResized;
import static org.apache.datasketches.theta.UpdateReturnState.RejectedDuplicate;
import static org.apache.datasketches.theta.UpdateReturnState.RejectedOverTheta;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.thetacommon.HashOperations;
import org.apache.datasketches.thetacommon.ThetaUtil;

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

    lgArrLongs_ = ThetaUtil.startingSubMultiple(lgNomLongs + 1, rf.lg(), ThetaUtil.MIN_LG_ARR_LONGS);
    hashTableThreshold_ = getHashTableThreshold(lgNomLongs, lgArrLongs_);
    curCount_ = 0;
    thetaLong_ = (long)(p * LONG_MAX_VALUE_AS_DOUBLE);
    empty_ = true; //other flags: bigEndian = readOnly = compact = ordered = false;
    cache_ = new long[1 << lgArrLongs_];
  }

  /**
   * Heapify a sketch from a MemorySegment UpdateSketch or Union object
   * containing sketch data.
   * @param srcSeg The source MemorySegment object.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return instance of this sketch
   */
  static HeapQuickSelectSketch heapifyInstance(final MemorySegment srcSeg, final long seed) {
    final int preambleLongs = extractPreLongs(srcSeg);            //byte 0
    final int lgNomLongs = extractLgNomLongs(srcSeg);             //byte 3
    final int lgArrLongs = extractLgArrLongs(srcSeg);             //byte 4

    checkUnionQuickSelectFamily(srcSeg, preambleLongs, lgNomLongs);
    checkSegIntegrity(srcSeg, seed, preambleLongs, lgNomLongs, lgArrLongs);

    final float p = extractP(srcSeg);                             //bytes 12-15
    final int seglgRF = extractLgResizeFactor(srcSeg);            //byte 0
    ResizeFactor segRF = ResizeFactor.getRF(seglgRF);
    final int familyID = extractFamilyID(srcSeg);
    final Family family = Family.idToFamily(familyID);

    if (isResizeFactorIncorrect(srcSeg, lgNomLongs, lgArrLongs)) {
      segRF = ResizeFactor.X2; //X2 always works.
    }

    final HeapQuickSelectSketch hqss = new HeapQuickSelectSketch(lgNomLongs, seed, p, segRF,
        preambleLongs, family);
    hqss.lgArrLongs_ = lgArrLongs;
    hqss.hashTableThreshold_ = getHashTableThreshold(lgNomLongs, lgArrLongs);
    hqss.curCount_ = extractCurCount(srcSeg);
    hqss.thetaLong_ = extractThetaLong(srcSeg);
    hqss.empty_ = PreambleUtil.isEmptyFlag(srcSeg);
    hqss.cache_ = new long[1 << lgArrLongs];
    MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, preambleLongs << 3, hqss.cache_, 0, 1 << lgArrLongs); //read in as hash table
    return hqss;
  }

  //Sketch

  @Override
  public double getEstimate() {
    return Sketch.estimate(thetaLong_, curCount_);
  }

  @Override
  public Family getFamily() {
    return MY_FAMILY;
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public long getThetaLong() {
    return empty_ ? Long.MAX_VALUE : thetaLong_;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  @Override
  public HashIterator iterator() {
    return new HeapHashIterator(cache_, thetaLong_);
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
    final int lgArrLongsSM = ThetaUtil.startingSubMultiple(lgNomLongs_ + 1, rf.lg(), ThetaUtil.MIN_LG_ARR_LONGS);
    if (lgArrLongsSM == lgArrLongs_) {
      final int arrLongs = cache_.length;
      assert (1 << lgArrLongs_) == arrLongs;
      java.util.Arrays.fill(cache_,  0L);
    }
    else {
      cache_ = new long[1 << lgArrLongsSM];
      lgArrLongs_ = lgArrLongsSM;
    }
    hashTableThreshold_ = getHashTableThreshold(lgNomLongs_, lgArrLongs_);
    empty_ = true;
    curCount_ = 0;
    thetaLong_ =  (long)(getP() * LONG_MAX_VALUE_AS_DOUBLE);
  }

  //restricted methods

  @Override
  long[] getCache() {
    return cache_;
  }

  @Override
  int getCompactPreambleLongs() {
    return CompactOperations.computeCompactPreLongs(empty_, curCount_, thetaLong_);
  }

  @Override
  int getCurrentPreambleLongs() {
    return preambleLongs_;
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
        return InsertedCountIncrementedResized;
      }
      //Already at tgt size, must rebuild
      assert (lgArrLongs_ == (lgNomLongs_ + 1)) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
      quickSelectAndRebuild(); //Changes thetaLong_, curCount_, reassigns cache
      return InsertedCountIncrementedRebuilt;
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

  //Must resize. Changes lgArrLongs_, cache_, hashTableThreshold;
  // theta and count don't change.
  // Used by hashUpdate()
  private final void resizeCache() {
    final ResizeFactor rf = getResizeFactor();
    final int lgMaxArrLongs = lgNomLongs_ + 1;
    final int lgDeltaLongs = lgMaxArrLongs - lgArrLongs_;
    final int lgResizeFactor = max(min(rf.lg(), lgDeltaLongs), 1); //rf_.lg() could be 0
    lgArrLongs_ += lgResizeFactor; // new arr size

    final long[] tgtArr = new long[1 << lgArrLongs_];
    final int newCount = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);

    assert newCount == curCount_;  //Assumes no dirty values.
    curCount_ = newCount;

    cache_ = tgtArr;
    hashTableThreshold_ = getHashTableThreshold(lgNomLongs_, lgArrLongs_);
  }

  //array stays the same size. Changes theta and thus count
  private final void quickSelectAndRebuild() {
    final int arrLongs = 1 << lgArrLongs_; // generally 2 * k,

    final int pivot = (1 << lgNomLongs_) + 1; // pivot for QS = k + 1

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
  private static final int getHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    final double fraction = (lgArrLongs <= lgNomLongs) ? ThetaUtil.RESIZE_THRESHOLD : ThetaUtil.REBUILD_THRESHOLD;
    return (int) (fraction * (1 << lgArrLongs));
  }

}
