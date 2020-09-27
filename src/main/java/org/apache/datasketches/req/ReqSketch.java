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

import static java.lang.Math.max;
import static java.lang.Math.sqrt;
import static org.apache.datasketches.req.Criteria.GE;
import static org.apache.datasketches.req.Criteria.GT;
import static org.apache.datasketches.req.ReqHelper.LS;
import static org.apache.datasketches.req.ReqHelper.validateSplits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.SketchesArgumentException;


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
 * so far (variable numCompactions). Initially, the relative-compactor starts with 3 sections.
 * Each time the numCompactions exceeds 2^{numSections - 1}, we double numSections.</li>
 *
 * <li>The size of each section (variable k and sectionSize in the code and parameter k in
 * the paper) is initialized with a value set by the user via variable k.
 * When the number of sections doubles, we decrease sectionSize by a factor of sqrt(2).
 * This is applied at each level separately. Thus, when we double the number of sections, the
 * nominal compactor size increases by a factor of sqrt(2) (up to +-1 after rounding).</li>
 *
 * <li>The merge operation here does not perform "special compactions", which are used in the paper
 * to allow for a tight mathematical analysis of the sketch.</li>
 * </ul>
 *
 * <p>This implementation provides a number of capabilites not discussed in the paper or provided
 * in the Python prototype.</p>
 * <ul><li>The Python prototype only implemented high accuracy for low ranks. This implementation
 * provides the user with the ability to choose either high rank accuracy or low rank accuracy at
 * the time of construction.</li>
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
  static final int INIT_NUMBER_OF_SECTIONS = 3;
  static final int MIN_K = 4;
  private static final double relRseFactor = sqrt(0.0512 / INIT_NUMBER_OF_SECTIONS);
  private static final double fixRseFactor = .06;
  private long totalN; //total items offered to sketch
  private int k = 0;
  private int retItems = 0; //number of retained items in the sketch
  private int maxNomSize = 0; //sum of nominal capacities of all compactors
  private float minValue = Float.NaN;
  private float maxValue = Float.NaN;
  final boolean hra;
  private boolean compatible = true;
  private Criteria criterion = Criteria.LT; //default is less-than
  private ReqAuxiliary aux = null;

  private List<ReqCompactor> compactors = new ArrayList<>();

  ReqDebug reqDebug = null; //read by compactor as well

  /**
   * Constructor.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one. A value of 50 roughly corresponds to 1% relative error guarantee with
   * constant probability
   * @param highRankAccuracy if true, the high ranks are prioritized for better accuracy.
   * Otherwise the low ranks are prioritized for better accuracy.
   * @param reqDebug the debug signaling interface.
   */
  public ReqSketch(final int k, final boolean highRankAccuracy, final ReqDebug reqDebug) {
    this.k = max(k & -2, MIN_K);
    hra = highRankAccuracy;
    retItems = 0;
    maxNomSize = 0;
    totalN = 0;
    this.reqDebug = reqDebug;
    if (reqDebug != null) { reqDebug.emitStart(this); }
    grow();
  }

  /**
   * Copy Constructor
   * @param other the other sketch to be deep copied into this one.
   */
  ReqSketch(final ReqSketch other) {
    totalN = other.totalN;
    k = other.k;
    retItems = other.retItems;
    maxNomSize = other.maxNomSize;
    minValue = other.minValue;
    maxValue = other.maxValue;
    hra = other.hra;
    criterion = other.criterion;
    reqDebug = other.reqDebug;

    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.add(new ReqCompactor(other.compactors.get(i)));
    }
    aux = null;
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
      final int retCompItems = c.getBuffer().getLength();
      final int nomCap = c.getNomCapacity();

      if (retCompItems >= nomCap) {
        if (h + 1 >= getNumLevels()) { //at the top?
          if (reqDebug != null) { reqDebug.emitMustAddCompactor(); }
          grow(); //add a level, increases maxNomSize
        }
        final FloatBuffer promoted = c.compact();
        compactors.get(h + 1).getBuffer().mergeSortIn(promoted);
        updateRetainedItems();
        if (retItems < maxNomSize) { break; }
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
    if (isEmpty()) { return new double[0]; }
    final long[] buckets = getPMForCDF(splitPoints);
    final int numBkts = buckets.length;
    final double[] outArr = new double[numBkts];
    for (int j = 0; j < numBkts; j++) {
      outArr[j] = (double)buckets[j] / getN();
    }
    return outArr;
  }

  List<ReqCompactor> getCompactors() {
    return compactors;
  }

  private long getCount(final float value) {
    final int numComp = compactors.size();
    long cumNnr = 0;
    for (int i = 0; i < numComp; i++) { //cycle through compactors
      final ReqCompactor c = compactors.get(i);
      final int wt = 1 << c.getLgWeight();
      final FloatBuffer buf = c.getBuffer();
      cumNnr += buf.getCountWithCriterion(value, criterion) * wt;
    }
    if (criterion == GT || criterion == GE) {
      cumNnr = totalN - cumNnr;
    }
    return cumNnr;
  }

  private long[] getCounts(final float[] values) {
    final int numValues = values.length;
    final int numComp = compactors.size();
    final long[] cumNnrArr = new long[numValues];
    for (int i = 0; i < numComp; i++) { //cycle through compactors
      final ReqCompactor c = compactors.get(i);
      final int wt = 1 << c.getLgWeight();
      final FloatBuffer buf = c.getBuffer();
      for (int j = 0; j < numValues; j++) {
        cumNnrArr[j] += buf.getCountWithCriterion(values[j], criterion) * wt;
      }
    }
    if (criterion == GT || criterion == GE) {
      for (int j = 0; j < numValues; j++) {
        cumNnrArr[j] = totalN - cumNnrArr[j];
      }
    }
    return cumNnrArr;
  }

  Criteria getCriterion() {
    return criterion;
  }

  @Override
  public boolean getHighRankAccuracy() {
    return hra;
  }

  int getK() {
    return k;
  }

  @Override
  public boolean getLessThanOrEqual() {
    return criterion == Criteria.LE;
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
    if (isEmpty()) { return new double[0]; }
    final long[] buckets = getPMForCDF(splitPoints);
    final int numBkts = buckets.length;
    final double[] outArr = new double[numBkts];
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
    if (isEmpty()) {
      throw new SketchesArgumentException(
          "Sketch is empty.");
    }
    if (normRank < 0 || normRank > 1.0) {
      throw new SketchesArgumentException(
        "Normalized rank must be in the range [0.0, 1.0]: " + normRank);
    }
    if (aux == null) {
      aux = new ReqAuxiliary(this);
    }
    final float q = aux.getQuantile(normRank);
    if (Float.isNaN(q)) { //possible result from aux.getQuantile()
      if (compatible) {
        if (criterion == Criteria.LT || criterion == Criteria.LE) { return minValue; }
        else { return maxValue; }
      }
    }
    return q;
  }

  @Override
  public float[] getQuantiles(final double[] normRanks) {
    final int len = normRanks.length;
    final float[] qArr = new float[len];
    for (int i = 0; i < len; i++) {
      qArr[i] = getQuantile(normRanks[i]);
    }
    return qArr;
  }

  @Override
  public double getRank(final float value) {
    final long nnCount = getCount(value);
    return (double)nnCount / totalN;
  }

  @Override
  public double[] getRanks(final float[] values) {
    final long[] cumNnrArr = getCounts(values);
    final int numValues = values.length;
    final double[] rArr = new double[numValues];
    for (int i = 0; i < numValues; i++) {
      rArr[i] = (double)cumNnrArr[i] / totalN;
    }
    return rArr;
  }

  private static double getRankLB(final int k, final int levels, final double rank, final int numStdDev,
      final boolean hra, final long totalN) {
    if (levels == 1) { return rank; }
    final double thresh = (double)k * INIT_NUMBER_OF_SECTIONS / totalN;
    if ( hra && rank >= 1.0 - thresh) { return rank; }
    if (!hra && rank <= thresh) { return rank; }
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

  private static double getRankUB(final int k, final int levels, final double rank, final int numStdDev,
      final boolean hra, final long totalN) {
    if (levels == 1) { return rank; }
    final double thresh = (double)k * INIT_NUMBER_OF_SECTIONS / totalN;
    if ( hra && rank >= 1.0 - thresh) { return rank; }
    if (!hra && rank <= thresh) { return rank; }
    final double relative = relRseFactor / k * (hra ? 1.0 - rank : rank);
    final double fixed = fixRseFactor / k;
    final double ubRel = rank + numStdDev * relative;
    final double ubFix = rank + numStdDev * fixed;
    return Math.min(ubRel, ubFix);
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
  //Serialize totalN, k, minValue, maxValue.
  // In preamble Flags keep: hra-bit, compatible-bit, criterion-bit (maybe 2?)
  // plus compactors.
  public int getSerializationBytes() {
    int cBytes = 0;
    for (int i = 0; i < compactors.size(); i++) {
     cBytes += compactors.get(i).getSerializationBytes();
    }
    final int members = 20; //totalN, k, minValue, maxValue
    final int preamble = 8;
    return cBytes + members + preamble;
  }

  private void grow() {
    compactors.add(new ReqCompactor(this, k, getNumLevels()));
    updateMaxNomSize();
    final int lgWeight = compactors.size() - 1;
    if (reqDebug != null) { reqDebug.emitNewCompactor(lgWeight); }
  }

  @Override
  public boolean isCompatible() {
    return compatible;
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
  public ReqIterator iterator() {
    return new ReqIterator(this);
  }

  @Override
  public ReqSketch merge(final ReqSketch other) {
    if (other == null || other.isEmpty()) { return this; }
    totalN += other.totalN;
    //update min, max values, n
    if (Float.isNaN(minValue) || other.minValue < minValue) { minValue = other.minValue; }
    if (Float.isNaN(maxValue) || other.maxValue > maxValue) { maxValue = other.maxValue; }
    //Grow until self has at least as many compactors as other
    while (getNumLevels() < other.getNumLevels()) { grow(); }
    //Merge the items in all height compactors
    for (int i = 0; i < getNumLevels(); i++) {
      compactors.get(i).merge(other.compactors.get(i));
    }
    updateRetainedItems();
    if (retItems >= maxNomSize) { compress(); }

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
  public ReqSketch setCompatible(final boolean compatible) {
    this.compatible = compatible;
    return this;
  }

  /**
   * <b>NOTE:</b> This is public only to allow testing from another
   * package and is not intened for use by normal users of this class.
   * @param criterion one of LT, LE, GT, GE.
   * @return this
   */
  public ReqSketch setCriterion(final Criteria criterion) {
    this.criterion = criterion;
    return this;
  }

  @Override
  public ReqSketch setLessThanOrEqual(final boolean ltEq) {
    if (ltEq) { setCriterion(Criteria.LE); }
    else { setCriterion(Criteria.LT); }
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("**********Relative Error Quantiles Sketch Summary**********").append(LS);
    sb.append("  N               : " + totalN).append(LS);
    sb.append("  Retained Items  : " + retItems).append(LS);
    sb.append("  Max Nominal Size: " + maxNomSize).append(LS);
    sb.append("  Min Value       : " + minValue).append(LS);
    sb.append("  Max Value       : " + maxValue).append(LS);
    sb.append("  Estimation Mode : " + isEstimationMode()).append(LS);
    sb.append("  Criterion       : " + criterion).append(LS);
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
  void updateMaxNomSize() {
    int cap = 0;
    for (ReqCompactor c : compactors) { cap += c.getNomCapacity(); }
    maxNomSize = cap;
  }

  /**
   * Computes the retItems for the sketch.
   */
  private void updateRetainedItems() {
    int count = 0;
    for (ReqCompactor c : compactors) { count += c.getBuffer().getLength(); }
    retItems = count;
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

}
