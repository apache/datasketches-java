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

package org.apache.datasketches.req;

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.FloatsSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesAPI;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;

/**
 * This Relative Error Quantiles Sketch is the Java implementation based on the paper
 * "Relative Error Streaming Quantiles" by Graham Cormode, Zohar Karnin, Edo Liberty,
 * Justin Thaler, Pavel Veselý, and loosely derived from a Python prototype written by Pavel Veselý.
 *
 * <p>Reference: https://arxiv.org/abs/2004.01668</p>
 *
 * <p>This implementation differs from the algorithm described in the paper in the following:</p>
 *
 * <ul>
 * <li>The algorithm requires no upper bound on the stream length.
 * Instead, each relative-compactor counts the number of compaction operations performed
 * so far (via variable state). Initially, the relative-compactor starts with INIT_NUMBER_OF_SECTIONS.
 * Each time the number of compactions (variable state) exceeds 2^{numSections - 1}, we double
 * numSections. Note that after merging the sketch with another one variable state may not correspond
 * to the number of compactions performed at a particular level, however, since the state variable
 * never exceeds the number of compactions, the guarantees of the sketch remain valid.</li>
 *
 * <li>The size of each section (variable k and sectionSize in the code and parameter k in
 * the paper) is initialized with a number set by the user via variable k.
 * When the number of sections doubles, we decrease sectionSize by a factor of sqrt(2).
 * This is applied at each level separately. Thus, when we double the number of sections, the
 * nominal compactor size increases by a factor of approx. sqrt(2) (+/- rounding).</li>
 *
 * <li>The merge operation here does not perform "special compactions", which are used in the paper
 * to allow for a tight mathematical analysis of the sketch.</li>
 * </ul>
 *
 * <p>This implementation provides a number of capabilities not discussed in the paper or provided
 * in the Python prototype.</p>
 *
 * <ul><li>The Python prototype only implemented high accuracy for low ranks. This implementation
 * provides the user with the ability to choose either high rank accuracy or low rank accuracy at
 * the time of sketch construction.</li>
 * <li>The Python prototype only implemented a comparison criterion of "INCLUSIVE". This implementation
 * allows the user to switch back and forth between the "INCLUSIVE" criterion and the "EXCLUSIVE" criterion.</li>
 * <li>This implementation provides extensive debug visibility into the operation of the sketch with
 * two levels of detail output. This is not only useful for debugging, but is a powerful tool to
 * help users understand how the sketch works.</li>
 * </ul>
 *
 * @see QuantilesAPI
 *
 * @author Edo Liberty
 * @author Pavel Vesely
 * @author Lee Rhodes
 */
public final class ReqSketch extends BaseReqSketch {

  static class CompactorReturn {
    int deltaRetItems;
    int deltaNomSize;
  }

  //static finals
  private static final String LS = System.getProperty("line.separator");
  static final byte MIN_K = 4;
  static final byte NOM_CAP_MULT = 2;

  //finals
  private final int k; //default is 12 (1% @ 95% Confidence)
  private final boolean hra; //default is true
  //state variables
  private long totalN = 0;
  private float minItem = Float.NaN;
  private float maxItem = Float.NaN;
  //computed from compactors
  private int retItems = 0; //number of retained items in the sketch
  private int maxNomSize = 0; //sum of nominal capacities of all compactors
  //Objects
  private ReqSketchSortedView reqSV = null;
  private List<ReqCompactor> compactors = new ArrayList<>();
  private ReqDebug reqDebug = null; //user config, default: null, can be set after construction.

  private final CompactorReturn cReturn = new CompactorReturn(); //used in compress()

  private final Random rand;

  /**
   * Construct from elements. After sketch is constructed, retItems and maxNomSize must be computed.
   * Used by ReqSerDe.
   */
  ReqSketch(final int k, final boolean hra, final long totalN, final float minItem,
      final float maxItem, final List<ReqCompactor> compactors) {
    checkK(k);
    this.k = k;
    this.hra = hra;
    this.totalN = totalN;
    this.minItem = minItem;
    this.maxItem = maxItem;
    this.compactors = compactors;
    this.rand = new Random();
  }

  /**
   * Normal Constructor used by ReqSketchBuilder.
   * @param k Controls the size and error of the sketch. It must be even and in the range
   * [4, 1024].
   * The default number 12 roughly corresponds to 1% relative error guarantee at 95% confidence.
   * @param highRankAccuracy if true, the default, the high ranks are prioritized for better
   * accuracy. Otherwise the low ranks are prioritized for better accuracy.
   * @param reqDebug the debug handler. It may be null.
   */
  ReqSketch(final int k, final boolean highRankAccuracy, final ReqDebug reqDebug) {
    checkK(k);
    this.k = k;
    this.hra = highRankAccuracy;
    this.reqDebug = reqDebug;
    this.rand = (reqDebug == null) ? new Random() : new Random(1);
    grow();
  }

  /**
   * Copy Constructor.  Only used in test.
   * @param other the other sketch to be deep copied into this one.
   */
  ReqSketch(final ReqSketch other) {
    this.k = other.k;
    this.hra = other.hra;
    this.totalN = other.totalN;
    this.retItems = other.retItems;
    this.maxNomSize = other.maxNomSize;
    this.minItem = other.minItem;
    this.maxItem = other.maxItem;
    this.reqDebug = other.reqDebug;
    this.reqSV = null;
    this.rand = (reqDebug == null) ? new Random() : new Random(1);

    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.add(new ReqCompactor(other.compactors.get(i)));
    }
  }

  /**
   * Returns a new ReqSketchBuilder
   * @return a new ReqSketchBuilder
   */
  public static final ReqSketchBuilder builder() {
    return new ReqSketchBuilder();
  }

  /**
   * Returns an ReqSketch on the heap from a Memory image of the sketch.
   * @param mem The Memory object holding a valid image of an ReqSketch
   * @return an ReqSketch on the heap from a Memory image of the sketch.
   */
  public static ReqSketch heapify(final Memory mem) {
    return ReqSerDe.heapify(mem);
  }

  @Override
  public int getK() {
    return k;
  }

  /**
   * This checks the given float array to make sure that it contains only finite numbers
   * and is monotonically increasing.
   * @param splits the given array
   */
  static void validateSplits(final float[] splits) {
    final int len = splits.length;
    for (int i = 0; i < len; i++) {
      final float v = splits[i];
      if (!Float.isFinite(v)) {
        throw new SketchesArgumentException("Numbers must be finite");
      }
      if (i < len - 1 && v >= splits[i + 1]) {
        throw new SketchesArgumentException(
          "Numbers must be unique and monotonically increasing");
      }
    }
  }

  @Override
  public double[] getCDF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return reqSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public boolean getHighRankAccuracyMode() {
    return hra;
  }

  @Override
  public float getMaxItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return maxItem;
  }

  @Override
  public float getMinItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return minItem;
  }

  @Override
  public long getN() {
    return totalN;
  }

  @Override
  /**
   * This is an unsupported operation for this sketch
   */
  public double getNormalizedRankError(final boolean pmf) {
    throw new UnsupportedOperationException(UNSUPPORTED_MSG);

  }

  @Override
  public double[] getPMF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return reqSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public float getQuantile(final double normRank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    if (normRank < 0 || normRank > 1.0) {
      throw new SketchesArgumentException(
        "Normalized rank must be in the range [0.0, 1.0]: " + normRank);
    }
    refreshSortedView();
    return reqSV.getQuantile(normRank, searchCrit);
  }

  @Override
  public float[] getQuantiles(final double[] normRanks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    final int len = normRanks.length;
    final float[] qArr = new float[len];
    for (int i = 0; i < len; i++) {
      qArr[i] = reqSV.getQuantile(normRanks[i], searchCrit);
    }
    return qArr;
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.95.
   */
  @Override
  public float getQuantileLowerBound(final double rank) {
    return getQuantile(getRankLowerBound(rank, 2), INCLUSIVE);
  }

  @Override
  public float getQuantileLowerBound(final double rank, final int numStdDev) {
    return getQuantile(getRankLowerBound(rank, numStdDev), INCLUSIVE);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.95.
   */
  @Override
  public float getQuantileUpperBound(final double rank) {
    return getQuantile(getRankUpperBound(rank, 2), INCLUSIVE);
  }

  @Override
  public float getQuantileUpperBound(final double rank, final int numStdDev) {
    return getQuantile(getRankUpperBound(rank, numStdDev), INCLUSIVE);
  }

  @Override
  public double getRank(final float quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return reqSV.getRank(quantile, searchCrit);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.95.
   */
  @Override
  public double getRankLowerBound(final double rank) {
    return getRankLB(k, getNumLevels(), rank, 2, hra, getN());
  }

  @Override
  public double getRankLowerBound(final double rank, final int numStdDev) {
    return getRankLB(k, getNumLevels(), rank, numStdDev, hra, getN());
  }

  @Override
  public double[] getRanks(final float[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    final int numQuantiles = quantiles.length;
    final double[] retArr = new double[numQuantiles];
    for (int i = 0; i < numQuantiles; i++) {
      retArr[i] = reqSV.getRank(quantiles[i], searchCrit); //already normalized
    }
    return retArr;
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.95.
   */
  @Override
  public double getRankUpperBound(final double rank) {
    return getRankUB(k, getNumLevels(), rank, 2, hra, getN());
  }

  @Override
  public double getRankUpperBound(final double rank, final int numStdDev) {
    return getRankUB(k, getNumLevels(), rank, numStdDev, hra, getN());
  }

  @Override
  public int getNumRetained() { return retItems; }

  @Override
  public int getSerializedSizeBytes() {
    final ReqSerDe.SerDeFormat serDeFormat = ReqSerDe.getSerFormat(this);
    return ReqSerDe.getSerBytes(this, serDeFormat);
  }

  @Override
  public FloatsSortedView getSortedView() {
    refreshSortedView();
    return reqSV;
  }

  @Override
  public boolean isEmpty() {
    return totalN == 0;
  }

  @Override
  public boolean isEstimationMode() {
    return getNumLevels() > 1;
  }

  @Override
  public QuantilesFloatsSketchIterator iterator() {
    return new ReqSketchIterator(this);
  }

  @Override
  public ReqSketch merge(final ReqSketch other) {
    if (other == null || other.isEmpty()) { return this; }
    if (other.hra != hra) {
      throw new SketchesArgumentException(
          "Both sketches must have the same HighRankAccuracy setting.");
    }
    totalN += other.totalN;
    //update min, max items, n
    if (Float.isNaN(minItem) || other.minItem < minItem) { minItem = other.minItem; }
    if (Float.isNaN(maxItem) || other.maxItem > maxItem) { maxItem = other.maxItem; }
    //Grow until self has at least as many compactors as other
    while (getNumLevels() < other.getNumLevels()) { grow(); }
    //Merge the items in all height compactors
    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.get(i).merge(other.compactors.get(i));
    }
    maxNomSize = computeMaxNomSize();
    retItems = computeTotalRetainedItems();
    if (retItems >= maxNomSize) {
      compress();
    }
    assert retItems < maxNomSize;
    reqSV = null;
    return this;
  }

  @Override
  public void reset() {
    totalN = 0;
    retItems = 0;
    maxNomSize = 0;
    minItem = Float.NaN;
    maxItem = Float.NaN;
    reqSV = null;
    compactors = new ArrayList<>();
    grow();
  }

  @Override
  public byte[] toByteArray() {
    return ReqSerDe.toByteArray(this);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("**********Relative Error Quantiles Sketch Summary**********").append(LS);
    sb.append("  K               : " + k).append(LS);
    sb.append("  N               : " + totalN).append(LS);
    sb.append("  Retained Items  : " + retItems).append(LS);
    sb.append("  Min Item        : " + minItem).append(LS);
    sb.append("  Max Item        : " + maxItem).append(LS);
    sb.append("  Estimation Mode : " + isEstimationMode()).append(LS);
    sb.append("  High Rank Acc   : " + hra).append(LS);
    sb.append("  Levels          : " + compactors.size()).append(LS);
    sb.append("************************End Summary************************").append(LS);
    return sb.toString();
  }

  @Override
  public void update(final float item) {
    if (Float.isNaN(item)) { return; }
    if (isEmpty()) {
      minItem = item;
      maxItem = item;
    } else {
      if (item < minItem) { minItem = item; }
      if (item > maxItem) { maxItem = item; }
    }
    final FloatBuffer buf = compactors.get(0).getBuffer();
    buf.append(item);
    retItems++;
    totalN++;
    if (retItems >= maxNomSize) {
      buf.sort();
      compress();
    }
    reqSV = null;
  }

  @Override
  public String viewCompactorDetail(final String fmt, final boolean allData) {
    final StringBuilder sb = new StringBuilder();
    sb.append("*********Relative Error Quantiles Compactor Detail*********").append(LS);
    sb.append("Compactor Detail: Ret Items: ").append(getNumRetained())
      .append("  N: ").append(getN());
    sb.append(LS);
    for (int i = 0; i < getNumLevels(); i++) {
      final ReqCompactor c = compactors.get(i);
      sb.append(c.toListPrefix()).append(LS);
      if (allData) { sb.append(c.getBuffer().toHorizList(fmt, 20)).append(LS); }
    }
    sb.append("************************End Detail*************************").append(LS);
    return sb.toString();
  }

  /**
   * Computes a new bound for determining when to compress the sketch.
   */
  int computeMaxNomSize() {
    int cap = 0;
    for (final ReqCompactor c : compactors) { cap += c.getNomCapacity(); }
    return cap;
  }

  /**
   * Computes the retained Items for the sketch.
   */
  int computeTotalRetainedItems() {
    int count = 0;
    for (final ReqCompactor c : compactors) {
      count += c.getBuffer().getCount();
    }
    return count;
  }

  List<ReqCompactor> getCompactors() {
    return compactors;
  }

  int getMaxNomSize() {
    return maxNomSize;
  }

  /**
   * Gets the number of levels of compactors in the sketch.
   * @return the number of levels of compactors in the sketch.
   */
  int getNumLevels() {
    return compactors.size();
  }

  void setMaxNomSize(final int maxNomSize) {
    this.maxNomSize = maxNomSize;
  }

  void setRetainedItems(final int retItems) {
    this.retItems = retItems;
  }

  private static void checkK(final int k) {
    if ((k & 1) > 0 || k < 4 || k > 1024) {
      throw new SketchesArgumentException(
          "<i>K</i> must be even and in the range [4, 1024]: " + k );
    }
  }

  private void compress() {
    if (reqDebug != null) { reqDebug.emitStartCompress(); }
    for (int h = 0; h < compactors.size(); h++) {
      final ReqCompactor c = compactors.get(h);
      final int compRetItems = c.getBuffer().getCount();
      final int compNomCap = c.getNomCapacity();

      if (compRetItems >= compNomCap) {
        if (h + 1 >= getNumLevels()) { //at the top?
          if (reqDebug != null) { reqDebug.emitMustAddCompactor(); }
          grow(); //add a level, increases maxNomSize
        }
        final FloatBuffer promoted = c.compact(cReturn, this.rand);
        compactors.get(h + 1).getBuffer().mergeSortIn(promoted);
        retItems += cReturn.deltaRetItems;
        maxNomSize += cReturn.deltaNomSize;
        //we specifically decided not to do lazy compression.
      }
    }
    reqSV = null;
    if (reqDebug != null) { reqDebug.emitCompressDone(); }
  }

  private void grow() {
    final byte lgWeight = (byte)getNumLevels();
    if (lgWeight == 0 && reqDebug != null) { reqDebug.emitStart(this); }
    compactors.add(new ReqCompactor(lgWeight, hra, k, reqDebug));
    maxNomSize = computeMaxNomSize();
    if (reqDebug != null) { reqDebug.emitNewCompactor(lgWeight); }
  }

  private final void refreshSortedView() {
    reqSV = (reqSV == null) ? new ReqSketchSortedView(this) : reqSV;
  }

}
