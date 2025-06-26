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

package org.apache.datasketches.theta2;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.sqrt;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
import static org.apache.datasketches.common.Util.checkBounds;
import static org.apache.datasketches.theta2.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta2.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgResizeFactor;
import static org.apache.datasketches.theta2.PreambleUtil.extractP;
import static org.apache.datasketches.theta2.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta2.UpdateReturnState.InsertedCountIncremented;
import static org.apache.datasketches.theta2.UpdateReturnState.InsertedCountNotIncremented;
import static org.apache.datasketches.theta2.UpdateReturnState.RejectedDuplicate;
import static org.apache.datasketches.theta2.UpdateReturnState.RejectedOverTheta;
import static org.apache.datasketches.thetacommon2.HashOperations.STRIDE_MASK;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon2.HashOperations;
import org.apache.datasketches.thetacommon2.ThetaUtil;

/**
 * This sketch uses the
 * <a href="{@docRoot}/resources/dictionary.html#thetaSketch">Theta Sketch Framework</a>
 * and the
 * <a href="{@docRoot}/resources/dictionary.html#alphaTCF">Alpha TCF</a> algorithm
 * with a single cache.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class HeapAlphaSketch extends HeapUpdateSketch {
  private static final int ALPHA_MIN_LG_NOM_LONGS = 9; //The smallest Log2 k allowed => 512.
  private final double alpha_;  // computed from lgNomLongs
  private final long split1_;   // computed from alpha and p

  private int lgArrLongs_;
  private int hashTableThreshold_;  //never serialized
  private int curCount_ = 0;
  private long thetaLong_;
  private boolean empty_ = true;

  private long[] cache_;
  private boolean dirty_ = false;

  private HeapAlphaSketch(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf, final double alpha, final long split1) {
    super(lgNomLongs, seed, p, rf);
    alpha_ = alpha;
    split1_ = split1;
  }

  /**
   * Get a new sketch instance on the java heap.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return instance of this sketch
   */
  static HeapAlphaSketch newHeapInstance(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf) {

    if (lgNomLongs < ALPHA_MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException(
        "This sketch requires a minimum nominal entries of " + (1 << ALPHA_MIN_LG_NOM_LONGS));
    }

    final double nomLongs = (1L << lgNomLongs);
    final double alpha = nomLongs / (nomLongs + 1.0);
    final long split1 = (long) (((p * (alpha + 1.0)) / 2.0) * LONG_MAX_VALUE_AS_DOUBLE);

    final HeapAlphaSketch has = new HeapAlphaSketch(lgNomLongs, seed, p, rf, alpha, split1);

    final int lgArrLongs = ThetaUtil.startingSubMultiple(lgNomLongs + 1, rf.lg(), ThetaUtil.MIN_LG_ARR_LONGS);
    has.lgArrLongs_ = lgArrLongs;
    has.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    has.curCount_ = 0;
    has.thetaLong_ = (long)(p * LONG_MAX_VALUE_AS_DOUBLE);
    has.empty_ = true; //other flags: bigEndian = readOnly = compact = ordered = false;
    has.cache_ = new long[1 << lgArrLongs];
    return has;
  }

  /**
   * Heapify a sketch from a MemorySegment object containing sketch data.
   * @param srcSeg The source MemorySegment object.
   * It must have a size of at least 24 bytes.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return instance of this sketch
   */
  static HeapAlphaSketch heapifyInstance(final MemorySegment srcSeg, final long expectedSeed) {
    Objects.requireNonNull(srcSeg, "Source MemorySegment must not be null");
    checkBounds(0, 24, srcSeg.byteSize());
    final int preambleLongs = extractPreLongs(srcSeg);            //byte 0
    final int lgNomLongs = extractLgNomLongs(srcSeg);             //byte 3
    final int lgArrLongs = extractLgArrLongs(srcSeg);             //byte 4

    checkAlphaFamily(srcSeg, preambleLongs, lgNomLongs);
    checkSegIntegrity(srcSeg, expectedSeed, preambleLongs, lgNomLongs, lgArrLongs);

    final float p = extractP(srcSeg);                             //bytes 12-15
    final int seglgRF = extractLgResizeFactor(srcSeg);            //byte 0
    ResizeFactor segRF = ResizeFactor.getRF(seglgRF);

    final double nomLongs = (1L << lgNomLongs);
    final double alpha = nomLongs / (nomLongs + 1.0);
    final long split1 = (long) (((p * (alpha + 1.0)) / 2.0) * LONG_MAX_VALUE_AS_DOUBLE);

    if (isResizeFactorIncorrect(srcSeg, lgNomLongs, lgArrLongs)) {
      segRF = ResizeFactor.X2; //X2 always works.
    }

    final HeapAlphaSketch has = new HeapAlphaSketch(lgNomLongs, expectedSeed, p, segRF, alpha, split1);
    has.lgArrLongs_ = lgArrLongs;
    has.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    has.curCount_ = extractCurCount(srcSeg);
    has.thetaLong_ = extractThetaLong(srcSeg);
    has.empty_ = PreambleUtil.isEmptyFlag(srcSeg);
    has.cache_ = new long[1 << lgArrLongs];
    MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, preambleLongs << 3, has.cache_, 0, 1 << lgArrLongs); //read in as hash table
    return has;
  }

  //Sketch

  @Override
  public Family getFamily() {
    return Family.ALPHA;
  }

  @Override
  public HashIterator iterator() {
    return new HeapHashIterator(cache_, thetaLong_);
  }

  @Override
  public double getEstimate() {
    return (thetaLong_ > split1_)
        ? Sketch.estimate(thetaLong_, curCount_)
        : (1 << lgNomLongs_) * (LONG_MAX_VALUE_AS_DOUBLE / thetaLong_);
  }

  @Override
  public double getLowerBound(final int numStdDev) {
    if ((numStdDev < 1) || (numStdDev > 3)) {
      throw new SketchesArgumentException("numStdDev can only be the values 1, 2 or 3.");
    }
    double lb;
    if (isEstimationMode()) {
      final int validCount = getRetainedEntries(true);
      if (validCount > 0) {
        final double est = getEstimate();
        final double var = getVariance(1 << lgNomLongs_, getP(), alpha_, getTheta(), validCount);
        lb = est - (numStdDev * sqrt(var));
        lb = max(lb, 0.0);
      }
      else {
        lb = 0.0;
      }
    }
    else {
      lb = curCount_;
    }
    return lb;
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    if (curCount_ > 0) {
      if (valid && isDirty()) {
        final int curCount = HashOperations.countPart(getCache(), getLgArrLongs(), getThetaLong());
        return curCount;
      }
    }
    return curCount_;
  }

  @Override
  public long getThetaLong() {
    return thetaLong_;
  }

  @Override
  public double getUpperBound(final int numStdDev) {
    if ((numStdDev < 1) || (numStdDev > 3)) {
      throw new SketchesArgumentException("numStdDev can only be the values 1, 2 or 3.");
    }
    if (isEstimationMode()) {
      final double var =
          getVariance(1 << lgNomLongs_, getP(), alpha_, getTheta(), getRetainedEntries(true));
      return getEstimate() + (numStdDev * sqrt(var));
    }
    return curCount_;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  /*
   * Alpha Sketch Preamble Layout ( same as Theta UpdateSketch )
   * <pre>
   * Long || Start Byte Adr:
   * Adr:
   *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |        0           |
   *  0   ||    Seed Hash    | Flags  |  LgArr | LgNom  | FamID  | SerVer | lgRF | PreLongs=3  |
   *
   *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
   *  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
   *
   *      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
   *  2   ||---------------------------------Theta---------------------------------------------|
   * </pre>
   */

  @Override
  public byte[] toByteArray() {
    return toByteArray(Family.ALPHA.getMinPreLongs(), (byte) Family.ALPHA.getID());
  }

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    if (isDirty()) {
      rebuildDirty();
    }
    return this;
  }

  @Override
  public final void reset() {
    final int lgArrLongs =
        ThetaUtil.startingSubMultiple(lgNomLongs_ + 1, getResizeFactor().lg(), ThetaUtil.MIN_LG_ARR_LONGS);
    if (lgArrLongs == lgArrLongs_) {
      final int arrLongs = cache_.length;
      assert (1 << lgArrLongs_) == arrLongs;
      java.util.Arrays.fill(cache_, 0L);
    }
    else {
      cache_ = new long[1 << lgArrLongs];
      lgArrLongs_ = lgArrLongs;
    }
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    empty_ = true;
    curCount_ = 0;
    thetaLong_ =  (long)(getP() * LONG_MAX_VALUE_AS_DOUBLE);
    dirty_ = false;
  }

  //restricted methods

  @Override
  int getCompactPreambleLongs() {
    return CompactOperations.computeCompactPreLongs(empty_, curCount_, thetaLong_);
  }

  @Override
  int getCurrentPreambleLongs() {
    return Family.ALPHA.getMinPreLongs();
  }

  @Override
  long[] getCache() {
    return cache_;
  }

  @Override
  boolean isDirty() {
    return dirty_;
  }

  @Override
  boolean isOutOfSpace(final int numEntries) {
    return numEntries > hashTableThreshold_;
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

    //The duplicate/inserted tests
    if (dirty_) { //may have dirty values, must be at tgt size
      return enhancedHashInsert(cache_, hash);
    }

    //NOT dirty, the other duplicate or inserted test
    if (HashOperations.hashSearchOrInsert(cache_, lgArrLongs_, hash) >= 0) {
      return UpdateReturnState.RejectedDuplicate;
    }
    //insertion occurred, must increment
    curCount_++;
    final int r = (thetaLong_ > split1_) ? 0 : 1; //are we in sketch mode? (i.e., seen k+1 inserts?)
    if (r == 0) { //not yet sketch mode (has not seen k+1 inserts), but could be sampling
      if (curCount_ > (1 << lgNomLongs_)) { // > k
        //Reached the k+1 insert. Must be at tgt size or larger.
        //Transition to Sketch Mode. Happens only once.
        //Decrement theta, make dirty, don't bother check size, already not-empty.
        thetaLong_ = (long) (thetaLong_ * alpha_);
        dirty_ = true; //now may have dirty values
      }
      else {
        //inserts (not entries!) <= k. It may not be at tgt size.
        //Check size, don't decrement theta. cnt already ++, empty_ already false;
        if (isOutOfSpace(curCount_)) {
          resizeClean(); //not dirty, not at tgt size.
        }
      }
    }
    else { //r > 0: sketch mode and not dirty (e.g., after a rebuild).
      //dec theta, make dirty, cnt already ++, must be at tgt size or larger. check for rebuild
      assert (lgArrLongs_ > lgNomLongs_) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
      thetaLong_ = (long) (thetaLong_ * alpha_); //decrement theta
      dirty_ = true; //now may have dirty values
      if (isOutOfSpace(curCount_)) {
        rebuildDirty(); // at tgt size and maybe dirty
      }
    }
    return UpdateReturnState.InsertedCountIncremented;
  }

  /**
   * Enhanced Knuth-style Open Addressing, Double Hash insert.
   * The insertion process will overwrite an already existing, dirty (over-theta) value if one is
   * found in the search.
   * If an empty cell is found first, it will be inserted normally.
   *
   * @param hashTable the hash table to insert into
   * @param hash must not be 0. If not a duplicate, it will be inserted into the hash array
   * @return <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  final UpdateReturnState enhancedHashInsert(final long[] hashTable, final long hash) {
    final int arrayMask = (1 << lgArrLongs_) - 1; // arrayLongs -1
    // make odd and independent of curProbe:
    final int stride = (2 * (int) ((hash >>> lgArrLongs_) & STRIDE_MASK)) + 1;
    int curProbe = (int) (hash & arrayMask);
    long curTableHash = hashTable[curProbe];
    final int loopIndex = curProbe;

    // This is the enhanced part
    // Search for duplicate or zero, or opportunity to replace garbage.
    while ((curTableHash != hash) && (curTableHash != 0)) {
      // curHash is not a duplicate and not zero

      if (curTableHash >= thetaLong_) { // curTableHash is garbage, do enhanced insert
        final int rememberPos = curProbe; // remember its position.
        // Now we must make sure there are no duplicates in this search path,
        //   so we keep searching
        curProbe = (curProbe + stride) & arrayMask; // move forward
        curTableHash = hashTable[curProbe];
        while ((curTableHash != hash) && (curTableHash != 0)) {
          curProbe = (curProbe + stride) & arrayMask;
          curTableHash = hashTable[curProbe];
        }
        // curTableHash is a duplicate or zero
        if (curTableHash == hash) {
          return RejectedDuplicate; // duplicate, just return
        }
        assert (curTableHash == 0); // must be zero
        // Now that we know there are no duplicates we can
        // go back and insert at first garbage value position
        hashTable[rememberPos] = hash;
        thetaLong_ = (long) (thetaLong_ * alpha_); //decrement theta
        dirty_ = true; //the decremented theta could have produced a new dirty value
        return InsertedCountNotIncremented;
      }

      // curTableHash was not a duplicate, not zero, and NOT garbage,
      // so we keep searching
      assert (curTableHash < thetaLong_);
      curProbe = (curProbe + stride) & arrayMask;
      curTableHash = hashTable[curProbe];

      // ensure no infinite loop
      if (curProbe == loopIndex) {
        throw new SketchesArgumentException("No empty slot in table!");
      }
      // end of Enhanced insert
    } // end while and search

    // curTableHash is a duplicate or zero and NOT garbage
    if (curTableHash == hash) {
      return RejectedDuplicate; // duplicate, just return
    }
    // must be zero, so insert and increment
    assert (curTableHash == 0);
    hashTable[curProbe] = hash;
    thetaLong_ = (long) (thetaLong_ * alpha_); //decrement theta
    dirty_ = true; //the decremented theta could have produced a new dirty value
    if (++curCount_ > hashTableThreshold_) {
      rebuildDirty(); //at tgt size and maybe dirty
    }
    return InsertedCountIncremented;
  }

  //At tgt size or greater
  //Checks for rare lockup condition
  // Used by hashUpdate(), rebuild()
  private final void rebuildDirty() {
    final int curCountBefore = curCount_;
    forceRebuildDirtyCache(); //changes curCount_ only
    if (curCountBefore == curCount_) {
      //clean but unsuccessful at reducing count, must take drastic measures, very rare.
      forceResizeCleanCache(1);
    }
  }

  //curCount > hashTableThreshold
  //Checks for rare lockup condition
  // Used by hashUpdate()
  private final void resizeClean() {
    //must resize, but are we at tgt size?
    final int lgTgtLongs = lgNomLongs_ + 1;
    if (lgTgtLongs > lgArrLongs_) {
      //not yet at tgt size
      final ResizeFactor rf = getResizeFactor();
      final int lgDeltaLongs = lgTgtLongs - lgArrLongs_; //must be > 0
      final int lgResizeFactor = max(min(rf.lg(), lgDeltaLongs), 1); //rf_.lg() could be 0
      forceResizeCleanCache(lgResizeFactor);
    }
    else {
      //at tgt size or larger, no dirty values, must take drastic measures, very rare.
      forceResizeCleanCache(1);
    }
  }

  //Force resize. Changes lgArrLongs_ only. Theta doesn't change, count doesn't change.
  // Used by rebuildDirty(), resizeClean()
  private final void forceResizeCleanCache(final int lgResizeFactor) {
    assert (!dirty_); // Should never be dirty before a resize.
    lgArrLongs_ += lgResizeFactor; // new tgt size
    final long[] tgtArr = new long[1 << lgArrLongs_];
    final int newCount = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
    assert (curCount_ == newCount);
    curCount_ = newCount;
    cache_ = tgtArr;
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
  }

  //Cache stays the same size. Must be dirty. Theta doesn't change, count will change.
  // Used by rebuildDirtyAtTgtSize()
  private final void forceRebuildDirtyCache() {
    final long[] tgtArr = new long[1 << lgArrLongs_];
    curCount_ = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
    cache_ = tgtArr;
    dirty_ = false;
    //hashTableThreshold stays the same
  }

  // @formatter:off
  /**
   * Computes an estimate of the error variance based on Historic Inverse Probability (HIP)
   * estimators.  See Cohen: All-Distances Sketches, Revisited: HIP Estimators for Massive Graph
   * Analysis, Nov 2014.
   * <pre>
   * Table of sketch states and how Upper and Lower Bounds are computed
   *
   * Theta P    Count  Empty  EstMode Est   UB  LB   Comments
   * 1.0   1.0  0      T      F       0     0   0    Empty Sketch-mode only sketch
   * 1.0   1.0  N      F      F       N     N   N    Degenerate Sketch-mode only sketch
   * &lt;1.0  1.0  -      F      T       est   HIP HIP  Normal Sketch-mode only sketch
   *  P    &lt;1.0 0      T      F       0     0   0    Virgin sampling sketch
   *  P    &lt;1.0 N      F      T       est   HIP HIP  Degenerate sampling sketch
   *  &lt;P   &lt;1.0 N      F      T       est   HIP HIP  Sampling sketch also in sketch-mode
   * </pre>
   * @param k alias for nominal entries.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
   * @param alpha the value of alpha for this sketch
   * @param theta <a href="{@docRoot}/resources/dictionary.html#theta">See <i>theta</i></a>.
   * @param count the current valid count.
   * @return the variance.
   */
  // @formatter:on
  private static final double getVariance(final double k, final double p, final double alpha,
      final double theta, final int count) {
    final double kPlus1 = k + 1.0;
    final double y = 1.0 / p;
    final double ySq = y * y;
    final double ySqMinusY = ySq - y;
    final int r = getR(theta, alpha, p);
    final double result;
    if (r == 0) {
      result = count * ySqMinusY;
    }
    else if (r == 1) {
      result = kPlus1 * ySqMinusY; //term1
    }
    else { //r > 1
      final double b = 1.0 / alpha;
      final double bSq = b * b;
      final double x = p / theta;
      final double xSq = x * x;
      final double term1 = kPlus1 * ySqMinusY;
      final double term2 = y / (1.0 - bSq);
      final double term3 = (((y * bSq) - (y * xSq) - b - bSq) + x + (x * b));
      result = term1 + (term2 * term3);
    }
    final double term4 = (1 - theta) / (theta * theta);
    return result + term4;
  }

  /**
   * Computes whether there have been 0, 1, or 2 or more actual insertions into the cache in a
   * numerically safe way.
   * @param theta <a href="{@docRoot}/resources/dictionary.html#theta">See Theta</a>.
   * @param alpha internal computed value alpha.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
   * @return R.
   */
  private static final int getR(final double theta, final double alpha, final double p) {
    final double split1 = (p * (alpha + 1.0)) / 2.0;
    if (theta > split1) { return 0; }
    if (theta > (alpha * split1)) { return 1; }
    return 2;
  }

  /**
   * Returns the cardinality limit given the current size of the hash table array.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  private static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    final double fraction = (lgArrLongs <= lgNomLongs) ? ThetaUtil.RESIZE_THRESHOLD : ThetaUtil.REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }

  static void checkAlphaFamily(final MemorySegment seg, final int preambleLongs, final int lgNomLongs) {
    //Check Family
    final int familyID = extractFamilyID(seg);                       //byte 2
    final Family family = Family.idToFamily(familyID);
    if (family.equals(Family.ALPHA)) {
      if (preambleLongs != Family.ALPHA.getMinPreLongs()) {
        throw new SketchesArgumentException(
            "Possible corruption: Invalid PreambleLongs value for ALPHA: " + preambleLongs);
      }
    }
    else {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }

    //Check lgNomLongs
    if (lgNomLongs < ALPHA_MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException(
        "Possible corruption: This sketch requires a minimum nominal entries of "
            + (1 << ALPHA_MIN_LG_NOM_LONGS));
    }
  }

}
