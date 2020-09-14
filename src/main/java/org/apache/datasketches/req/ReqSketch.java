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
 * provides the user with the ability to choose either high rank accuracy or low rank accuracy at the
 * time of construction.</li>
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
  final static int INIT_NUMBER_OF_SECTIONS = 3;
  final static int MIN_K = 4;
  private final static int DEFAULT_K = 50;

  private long totalN; //total items offered to sketch
  private int k = 0;
  private int retItems = 0; //number of retained items in the sketch
  private int maxNomSize = 0; //sum of nominal capacities of all compactors
  private float minValue = Float.MAX_VALUE;
  private float maxValue = Float.MIN_VALUE;
  private final boolean hra;
  private boolean lteq = false; //default is less-than
  private ReqAuxiliary aux = null;

  private List<ReqCompactor> compactors = new ArrayList<>();

  private ReqDebug reqDebug = null;

  /**
   * Constructor with default k = 50, highRankAccuracy = true, reqDebug = null.
   * @see #ReqSketch(int, boolean, ReqDebug)
   */
  public ReqSketch() {
    this(DEFAULT_K);
  }

  /**
   * Constructor with given k, highRankAccuracy = true, and reqDebug = null.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one. A value of 50 roughly corresponds to 0.01-relative error guarantee with
   * constant probability.
   */
  public ReqSketch(final int k) {
    this(k, true);
  }

  /**
   * Constructor with given k and highRankAccuracy, and reqDebug = null.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one. A value of 50 roughly corresponds to 0.01-relative error guarantee with
   * constant probability
   * @param highRankAccuracy if true, the high ranks are prioritized for better accuracy. Otherwise,
   * the low ranks are prioritized for better accuracy.
   */
  public ReqSketch(final int k, final boolean highRankAccuracy) {
    this(k, highRankAccuracy, null);
  }

  /**
   * Constructor.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one. A value of 50 roughly corresponds to 0.01-relative error guarantee with
   * constant probability
   * @param highRankAccuracy if true, the high ranks are prioritized for better accuracy. Otherwise
   * the low ranks are prioritized for better accuracy.
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
    lteq = other.lteq;
    reqDebug = other.reqDebug;

    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.add(new ReqCompactor(other.compactors.get(i)));
    }
    aux = null;
  }

  private void compress() {
    if (reqDebug != null) { reqDebug.emitStartCompress(); }
    for (int h = 0; h < compactors.size(); h++) {
      final ReqCompactor c = compactors.get(h);
      final int retCompItems = c.getBuffer().getLength();
      final int nomCap = c.getNomCapacity();

      if (retCompItems >= nomCap) {
        if ((h + 1) >= getNumLevels()) { //at the top?
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

  @Override
  public boolean getHighRanksAccuracy() {
    return hra;
  }

  int getK() {
    return k;
  }

  @Override
  public boolean getLtEq() {
    return lteq;
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
  public double getMaxRSE(final int k) {
    return Math.sqrt(8.0 / INIT_NUMBER_OF_SECTIONS) / k;
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
    if ((normRank < 0) || (normRank > 1.0)) {
      throw new SketchesArgumentException(
        "Normalized rank must be in the range [0.0, 1.0]: " + normRank);
    }
    //if (normRank == 0.0) { return minValue; } //option for compat with other Q sketches
    //if (normRank == 1.0) { return maxValue; }
    if (aux == null) {
      aux = new ReqAuxiliary(this);
    }
    final float q = aux.getQuantile(normRank);
    //if (Float.isNaN(q)) { return minValue; }
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
    return getRanks( new float[] { value } )[0];
  }

  @Override
  public double getRankLowerBound(final double rank, final int numStdDev) {
    final float q = getQuantile(rank);
    final long nnRank = getCounts(new float[] { q })[0];
    if ((getNumLevels() == 1) || (nnRank <= (k * INIT_NUMBER_OF_SECTIONS))) {
      return nnRank / getN();
    }
    else { //TODO This doesn't work right
      final double nnLB = Math.floor( (1 - (numStdDev * getMaxRSE(k)) ) * nnRank);
      return nnLB / getN();
    }
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

  @Override
  public double getRankUpperBound(final double rank, final int numStdDev) {
    final float q = getQuantile(rank);
    final long nnRank = getCounts(new float[] { q })[0];
    if ((getNumLevels() == 1) || (nnRank <= (k * INIT_NUMBER_OF_SECTIONS))) {
      return nnRank / getN();
    }
    else { //TODO This doesn't work right
      final double nnUB = Math.ceil( (1 + (numStdDev * getMaxRSE(k)) ) * nnRank);
      return nnUB / getN();
    }
  }

  private long[] getCounts(final float[] values) {
    final int numValues = values.length;
    final int numComp = compactors.size();
    final long[] cumNnrArr = new long[numValues];
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final int wt = 1 << c.getLgWeight();
      final FloatBuffer buf = c.getBuffer();
      final int[] countsArr = buf.getCountsLtOrEq(values, lteq);
      for (int j = 0; j < numValues; j++) {
        cumNnrArr[j] += countsArr[j] * wt;
      }
    }
    return cumNnrArr;
  }

  @Override
  public int getRetainedItems() { return retItems; }

  private void grow() {
    compactors.add(new ReqCompactor(k, getNumLevels(), hra, reqDebug));
    updateMaxNomSize();
    final int lgWeight = compactors.size() - 1;
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
  public ReqIterator iterator() {
    return new ReqIterator(this);
  }

  @Override
  public ReqSketch merge(final ReqSketch other) {
    totalN += other.totalN;
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
  public void reset() {
    totalN = 0;
    retItems = 0;
    maxNomSize = 0;
    minValue = Float.MAX_VALUE;
    maxValue = Float.MIN_VALUE;
    aux = null;
    compactors = new ArrayList<>();
    grow();
  }

  @Override
  public void setLtEq(final boolean lteq) {
    this.lteq = lteq;
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
    sb.append("  LtEq Criterion  : " + lteq).append(LS);
    sb.append("  High Rank Acc   : " + hra).append(LS);
    sb.append("  Levels          : " + compactors.size()).append(LS);
    sb.append("************************End Summary************************").append(LS);
    return sb.toString();
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
      sb.append(c.toListPrefix());
      if (allData) { sb.append(c.getBuffer().toHorizList(fmt, 20, 16)).append(LS); }
      else { sb.append(LS); }
    }
    sb.append("************************End Detail*************************").append(LS);
    return sb.toString();
  }


  @Override
  public void update(final float item) {
    if (!Float.isFinite(item)) {
      throw new SketchesArgumentException("Input float values must be finite.");
    }
    final FloatBuffer buf = compactors.get(0).getBuffer();
    buf.append(item);
    retItems++;
    totalN++;
    minValue = (item < minValue) ? item : minValue;
    maxValue = (item > maxValue) ? item : maxValue;
    if (retItems >= maxNomSize) {
      buf.sort();
      compress();
    }
    aux = null;
  }

  /**
   * Computes a new bound for determining when to compress the sketch.
   */
  private void updateMaxNomSize() {
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

}
