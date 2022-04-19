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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;

/**
 * This Relative Error Quantiles Sketch is the Java implementation based on the paper
 * "Relative Error Streaming Quantiles", https://arxiv.org/abs/2004.01668, and loosely derived from
 * a Python prototype written by Pavel Vesely.
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
 * the paper) is initialized with a value set by the user via variable k.
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
 * <ul><li>The Python prototype only implemented high accuracy for low ranks. This implementation
 * provides the user with the ability to choose either high rank accuracy or low rank accuracy at
 * the time of sketch construction.</li>
 * <li>The Python prototype only implemented a comparison criterion of "&le;". This implementation
 * allows the user to switch back and forth between the "&le;" criterion and the "&lt;" criterion.</li>
 * <li>This implementation provides extensive debug visibility into the operation of the sketch with
 * two levels of detail output. This is not only useful for debugging, but is a powerful tool to
 * help users understand how the sketch works.</li>
 * </ul>
 *
 * @author Edo Liberty
 * @author Pavel Vesely
 * @author Lee Rhodes
 */
public class ReqSketch extends BaseReqSketch {
  //static finals
  private static final String LS = System.getProperty("line.separator");
  static final byte INIT_NUMBER_OF_SECTIONS = 3;
  static final byte MIN_K = 4;
  static final byte NOM_CAP_MULT = 2;
  //These two factors are used by upper and lower bounds
  private static final double relRseFactor = Math.sqrt(0.0512 / INIT_NUMBER_OF_SECTIONS);
  private static final double fixRseFactor = .084;
  //finals
  private final int k; //default is 12 (1% @ 95% Conf)
  private final boolean hra; //default is true
  //state variables
  private boolean ltEq = false; //default: LT, can be set after construction
  private long totalN;
  private float minValue = Float.NaN;
  private float maxValue = Float.NaN;
  //computed from compactors
  private int retItems = 0; //number of retained items in the sketch
  private int maxNomSize = 0; //sum of nominal capacities of all compactors
  //Objects
  private ReqAuxiliary aux = null;
  private List<ReqCompactor> compactors = new ArrayList<>();
  private ReqDebug reqDebug = null; //user config, default: null, can be set after construction.
  private final CompactorReturn cReturn = new CompactorReturn(); //used in compress()

  /**
   * Normal Constructor used by ReqSketchBuilder.
   * @param k Controls the size and error of the sketch. It must be even and in the range
   * [4, 1024], inclusive.
   * The default value of 12 roughly corresponds to 1% relative error guarantee at 95% confidence.
   * @param highRankAccuracy if true, the default, the high ranks are prioritized for better
   * accuracy. Otherwise the low ranks are prioritized for better accuracy.
   * @param reqDebug the debug handler. It may be null.
   */
  ReqSketch(final int k, final boolean highRankAccuracy, final ReqDebug reqDebug) {
    checkK(k);
    this.k = k;
    hra = highRankAccuracy;
    retItems = 0;
    maxNomSize = 0;
    totalN = 0;
    this.reqDebug = reqDebug;
    grow();
  }

  /**
   * Copy Constructor.  Only used in test.
   * @param other the other sketch to be deep copied into this one.
   */
  ReqSketch(final ReqSketch other) {
    k = other.k;
    hra = other.hra;

    totalN = other.totalN;
    retItems = other.retItems;
    maxNomSize = other.maxNomSize;
    minValue = other.minValue;
    maxValue = other.maxValue;
    ltEq = other.ltEq;
    reqDebug = other.reqDebug;
    //aux does not need to be copied

    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.add(new ReqCompactor(other.compactors.get(i)));
    }
    aux = null;
  }

  /**
   * Construct from elements. After sketch is constructed, retItems and maxNomSize must be computed.
   * Used by ReqSerDe.
   */
  ReqSketch(final int k, final boolean hra, final long totalN, final float minValue,
      final float maxValue, final List<ReqCompactor> compactors) {
    checkK(k);
    this.k = k;
    this.hra = hra;
    this.totalN = totalN;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.compactors = compactors;
  }

  /**
   * Returns an ReqSketch on the heap from a Memory image of the sketch.
   * @param mem The Memory object holding a valid image of an ReqSketch
   * @return an ReqSketch on the heap from a Memory image of the sketch.
   */
  public static ReqSketch heapify(final Memory mem) {
    return ReqSerDe.heapify(mem);
  }

  /**
   * Returns a new ReqSketchBuilder
   * @return a new ReqSketchBuilder
   */
  public static final ReqSketchBuilder builder() {
    return new ReqSketchBuilder();
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
        final FloatBuffer promoted = c.compact(cReturn);
        compactors.get(h + 1).getBuffer().mergeSortIn(promoted);
        retItems += cReturn.deltaRetItems;
        maxNomSize += cReturn.deltaNomSize;
        //we specifically decided not to do lazy compression.
      }
    }
    aux = null;
    if (reqDebug != null) { reqDebug.emitCompressDone(); }
  }

  ReqAuxiliary getAux() {
    return aux;
  }

  @Override
  public double[] getCDF(final float[] splitPoints) {
    if (isEmpty()) { return null; }
    final int numBkts = splitPoints.length + 1;
    final double[] outArr = new double[numBkts];
    final long[] buckets = getPMForCDF(splitPoints);
    for (int j = 0; j < numBkts; j++) {
      outArr[j] = (double)buckets[j] / getN();
    }
    return outArr;
  }

  List<ReqCompactor> getCompactors() {
    return compactors;
  }

  private long getCount(final float value) {
    if (isEmpty()) { return 0; }
    final int numComp = compactors.size();
    long cumNnr = 0;
    for (int i = 0; i < numComp; i++) { //cycle through compactors
      final ReqCompactor c = compactors.get(i);
      final long wt = 1L << c.getLgWeight();
      final FloatBuffer buf = c.getBuffer();
      cumNnr += buf.getCountWithCriterion(value, ltEq) * wt;
    }
    return cumNnr;
  }

  private long[] getCounts(final float[] values) {
    final int numValues = values.length;
    final int numComp = compactors.size();
    final long[] cumNnrArr = new long[numValues];
    if (isEmpty()) { return cumNnrArr; }
    for (int i = 0; i < numComp; i++) { //cycle through compactors
      final ReqCompactor c = compactors.get(i);
      final long wt = 1L << c.getLgWeight();
      final FloatBuffer buf = c.getBuffer();
      for (int j = 0; j < numValues; j++) {
        cumNnrArr[j] += buf.getCountWithCriterion(values[j], ltEq) * wt;
      }
    }
    return cumNnrArr;
  }

  boolean getLtEq() {
    return ltEq;
  }

  @Override
  public boolean getHighRankAccuracy() {
    return hra;
  }

  int getK() {
    return k;
  }

  int getMaxNomSize() {
    return maxNomSize;
  }

  @Override
  public float getMaxValue() {
    return maxValue;
  }

  @Override
  public float getMinValue() {
    return minValue;
  }

  @Override
  public long getN() {
    return totalN;
  }

  /**
   * Gets the number of levels of compactors in the sketch.
   * @return the number of levels of compactors in the sketch.
   */
  int getNumLevels() {
    return compactors.size();
  }

  @Override
  public double[] getPMF(final float[] splitPoints) {
    if (isEmpty()) { return null; }
    final int numBkts = splitPoints.length + 1;
    final double[] outArr = new double[numBkts];
    final long[] buckets = getPMForCDF(splitPoints);
    outArr[0] = (double)buckets[0] / getN();
    for (int j = 1; j < numBkts; j++) {
      outArr[j] = (double)(buckets[j] - buckets[j - 1]) / getN();
    }
    return outArr;
  }

  /**
   * Gets a CDF in raw counts, which can be easily converted into a CDF or PMF.
   * @param splits the splitPoints array
   * @return a CDF in raw counts
   */
  private long[] getPMForCDF(final float[] splits) {
    validateSplits(splits);
    final int numSplits = splits.length;
    final long[] splitCounts = getCounts(splits);
    final int numBkts = numSplits + 1;
    final long[] bkts = Arrays.copyOf(splitCounts, numBkts);
    bkts[numBkts - 1] = getN();
    return bkts;
  }

  @Override
  public float getQuantile(final double normRank) {
    if (isEmpty()) { return Float.NaN; }
    if (normRank < 0 || normRank > 1.0) {
      throw new SketchesArgumentException(
        "Normalized rank must be in the range [0.0, 1.0]: " + normRank);
    }
    if (aux == null) {
      aux = new ReqAuxiliary(this);
    }
    return aux.getQuantile(normRank, ltEq);
  }

  @Override
  public float[] getQuantiles(final double[] normRanks) {
    if (isEmpty()) { return null; }
    final int len = normRanks.length;
    final float[] qArr = new float[len];
    for (int i = 0; i < len; i++) {
      qArr[i] = getQuantile(normRanks[i]);
    }
    return qArr;
  }

  @Override
  public double getRank(final float value) {
    if (isEmpty()) { return Double.NaN; }
    final long nnCount = getCount(value);
    return (double)nnCount / totalN;
  }

  @Override
  public double[] getRanks(final float[] values) {
    if (isEmpty()) { return null; }
    final long[] cumNnrArr = getCounts(values);
    final int numValues = values.length;
    final double[] rArr = new double[numValues];
    for (int i = 0; i < numValues; i++) {
      rArr[i] = (double)cumNnrArr[i] / totalN;
    }
    return rArr;
  }

  private static double getRankLB(final int k, final int levels, final double rank,
      final int numStdDev, final boolean hra, final long totalN) {
    if (exactRank(k, levels, rank, hra, totalN)) { return rank; }
    final double relative = relRseFactor / k * (hra ? 1.0 - rank : rank);
    final double fixed = fixRseFactor / k;
    final double lbRel = rank - numStdDev * relative;
    final double lbFix = rank - numStdDev * fixed;
    return Math.max(lbRel, lbFix);
  }

  @Override
  public double getRankLowerBound(final double rank, final int numStdDev) {
    return getRankLB(k, getNumLevels(), rank, numStdDev, hra, getN());
  }

  private static double getRankUB(final int k, final int levels, final double rank,
      final int numStdDev, final boolean hra, final long totalN) {
    if (exactRank(k, levels, rank, hra, totalN)) { return rank; }
    final double relative = relRseFactor / k * (hra ? 1.0 - rank : rank);
    final double fixed = fixRseFactor / k;
    final double ubRel = rank + numStdDev * relative;
    final double ubFix = rank + numStdDev * fixed;
    return Math.min(ubRel, ubFix);
  }

  private static boolean exactRank(final int k, final int levels, final double rank,
      final boolean hra, final long totalN) {
    final int baseCap = k * INIT_NUMBER_OF_SECTIONS;
    if (levels == 1 || totalN <= baseCap) { return true; }
    final double exactRankThresh = (double)baseCap / totalN;
    return hra && rank >= 1.0 - exactRankThresh || !hra && rank <= exactRankThresh;
  }

  @Override
  public double getRankUpperBound(final double rank, final int numStdDev) {
    return getRankUB(k, getNumLevels(), rank, numStdDev, hra, getN());
  }

  @Override
  public int getRetainedItems() { return retItems; }

  @Override
  public double getRSE(final int k, final double rank, final boolean hra, final long totalN) {
    return getRankUB(k, 2, rank, 1, hra, totalN); //more conservative to assume > 1 level
  }

  @Override
  public int getSerializationBytes() {
    final ReqSerDe.SerDeFormat serDeFormat = ReqSerDe.getSerFormat(this);
    return ReqSerDe.getSerBytes(this, serDeFormat);
  }

  private void grow() {
    final byte lgWeight = (byte)getNumLevels();
    if (lgWeight == 0 && reqDebug != null) { reqDebug.emitStart(this); }
    compactors.add(new ReqCompactor(lgWeight, hra, k, reqDebug));
    maxNomSize = computeMaxNomSize();
    if (reqDebug != null) { reqDebug.emitNewCompactor(lgWeight); }
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
  public boolean isLessThanOrEqual() {
    return ltEq;
  }

  @Override
  public ReqIterator iterator() {
    return new ReqIterator(this);
  }

  @Override
  public ReqSketch merge(final ReqSketch other) {
    if (other == null || other.isEmpty()) { return this; }
    if (other.hra != hra) {
      throw new SketchesArgumentException(
          "Both sketches must have the same HighRankAccuracy setting.");
    }
    totalN += other.totalN;
    //update min, max values, n
    if (Float.isNaN(minValue) || other.minValue < minValue) { minValue = other.minValue; }
    if (Float.isNaN(maxValue) || other.maxValue > maxValue) { maxValue = other.maxValue; }
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
    aux = null;
    return this;
  }

  @Override
  public ReqSketch reset() {
    totalN = 0;
    retItems = 0;
    maxNomSize = 0;
    minValue = Float.NaN;
    maxValue = Float.NaN;
    aux = null;
    compactors = new ArrayList<>();
    grow();
    return this;
  }

  @Override
  public ReqSketch setLessThanOrEqual(final boolean ltEq) {
    this.ltEq = ltEq;
    return this;
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
    sb.append("  Min Value       : " + minValue).append(LS);
    sb.append("  Max Value       : " + maxValue).append(LS);
    sb.append("  Estimation Mode : " + isEstimationMode()).append(LS);
    sb.append("  LtEQ            : " + ltEq).append(LS);
    sb.append("  High Rank Acc   : " + hra).append(LS);
    sb.append("  Levels          : " + compactors.size()).append(LS);
    sb.append("************************End Summary************************").append(LS);
    return sb.toString();
  }

  @Override
  public void update(final float item) {
    if (Float.isNaN(item)) { return; }
    if (isEmpty()) {
      minValue = item;
      maxValue = item;
    } else {
      if (item < minValue) { minValue = item; }
      if (item > maxValue) { maxValue = item; }
    }
    final FloatBuffer buf = compactors.get(0).getBuffer();
    buf.append(item);
    retItems++;
    totalN++;
    if (retItems >= maxNomSize) {
      buf.sort();
      compress();
    }
    aux = null;
  }

  /**
   * Computes a new bound for determining when to compress the sketch.
   */
  int computeMaxNomSize() {
    int cap = 0;
    for (final ReqCompactor c : compactors) { cap += c.getNomCapacity(); }
    return cap;
  }

  void setMaxNomSize(final int maxNomSize) {
    this.maxNomSize = maxNomSize;
  }

  /**
   * Computes the retItems for the sketch.
   */
  int computeTotalRetainedItems() {
    int count = 0;
    for (final ReqCompactor c : compactors) {
      count += c.getBuffer().getCount();
    }
    return count;
  }

  void setRetainedItems(final int retItems) {
    this.retItems = retItems;
  }

  /**
   * This checks the given float array to make sure that it contains only finite values
   * and is monotonically increasing in value.
   * @param splits the given array
   */
  static void validateSplits(final float[] splits) {
    final int len = splits.length;
    for (int i = 0; i < len; i++) {
      final float v = splits[i];
      if (!Float.isFinite(v)) {
        throw new SketchesArgumentException("Values must be finite");
      }
      if (i < len - 1 && v >= splits[i + 1]) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

  @Override
  public String viewCompactorDetail(final String fmt, final boolean allData) {
    final StringBuilder sb = new StringBuilder();
    sb.append("*********Relative Error Quantiles Compactor Detail*********").append(LS);
    sb.append("Compactor Detail: Ret Items: ").append(getRetainedItems())
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

  static void checkK(final int k) {
    if ((k & 1) > 0 || k < 4 || k > 1024) {
      throw new SketchesArgumentException(
          "<i>K</i> must be even and in the range [4, 1024], inclusive: " + k );
    }
  }

  static class CompactorReturn {
    int deltaRetItems;
    int deltaNomSize;
  }

}
