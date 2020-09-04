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
import static org.apache.datasketches.req.ReqHelper.TAB;
import static org.apache.datasketches.req.ReqHelper.print;
import static org.apache.datasketches.req.ReqHelper.println;
import static org.apache.datasketches.req.ReqHelper.validateSplits;

import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.SketchesArgumentException;


/**
 * This Relative Error Quantiles Sketch is the Java implementation based on the paper
 * "Relative Error Streaming Quantiles", https://arxiv.org/abs/2004.01668.
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
 * @author Edo Liberty
 * @author Pavel Vesely
 * @author Lee Rhodes
 */
public class ReqSketch extends BaseReqSketch {
  final static int INIT_NUMBER_OF_SECTIONS = 3;
  final static int MIN_K = 4;
  private final static int DEFAULT_K = 50;

  private long totalN; //total items offered to sketch
  private int k;
  private int size; //number of retained items in the sketch
  private int maxNomSize; //sum of nominal capacities of all compactors
  private float minValue = Float.MAX_VALUE;
  private float maxValue = Float.MIN_VALUE;
  private final boolean hra;
  private boolean lteq = false;
  private boolean debug = false;

  private List<ReqCompactor> compactors = new ArrayList<>();
  private FloatBuffer updateBuf = null;
  private ReqAuxiliary aux = null;

  /**
   * Constructor with default k = 50, lteq = false, and debug = false.
   *
   */
  public ReqSketch() {
    this(DEFAULT_K, true, false, false);
  }

  /**
   * Constructor
   * @param k Controls the size and error of the sketch
   */
  public ReqSketch(final int k) {
    this(k, true, false, false);
  }

  /**
   * Constructor.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one. A value of 50 roughly corresponds to 0.01-relative error guarantee with
   * constant probability
   * @param highRankAccuracy if true, the high ranks are prioritized for better accuracy.
   * @param lteq if true, the compuation of rank and quantiles will be based on a less-than or equals
   * criteria. Otherwise the criteria is strictly less-than, which is consistent with the other
   * quantiles sketches in the library.
   * @param debug debug mode
   */
  public ReqSketch(final int k, final boolean highRankAccuracy, final boolean lteq, final boolean debug) {
    this.k = max(k & -2, MIN_K);
    hra = highRankAccuracy;
    this.lteq = lteq;
    this.debug = debug;
    size = 0;
    maxNomSize = 0;
    totalN = 0;
    if (debug) { println("START:"); }
    final int ncap = 2 * k * INIT_NUMBER_OF_SECTIONS;
    updateBuf = new FloatBuffer(ncap, ncap, hra);
    grow();
  }

  /**
   * Copy Constructor
   * @param other the other sketch to be deep copied into this one.
   */
  ReqSketch(final ReqSketch other) {
    k = other.k;
    hra = other.hra;
    lteq = other.lteq;
    debug = other.debug;
    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.add(new ReqCompactor(other.compactors.get(i)));
    }
    size = other.size;
    maxNomSize = other.maxNomSize;
    totalN = other.totalN;
    aux = null;
  }

  private void compress(final boolean lazy) {
    if (debug) { printStartCompress(); }
    if (debug) { printAllHorizList(); }
    //Choose the first compactor that is too large to compact
    //If lazy, we will compact more compactors that are too large.
    for (int h = 0; h < compactors.size(); h++) {
      final ReqCompactor c = compactors.get(h);
      final int retEnt = c.getBuffer().getLength();
      final int nomCap = c.getNomCapacity();

      if (retEnt >= nomCap) {
        if ((h + 1) >= getNumLevels()) { //at the top?
          if (debug) { printAddCompactor(h, retEnt, nomCap); }
          grow(); //add a level, increases maxNomSize
        }
        final FloatBuffer promoted = c.compact();
        compactors.get(h + 1).getBuffer().mergeSortIn(promoted);
        updateRetainedItems();
        if (lazy && (size < maxNomSize)) { break; }
      }
    }
    if (debug) { printAllHorizList(); }
    aux = null;
    if (debug) {
      println("COMPRESS: DONE: SketchSize: " + size + TAB + " MaxNomSize: " + maxNomSize + LS + LS);
    }
  }

  @Override
  public double[] getCDF(final float[] splitPoints) {
    if (isEmpty()) { return null; }
    validateSplits(splitPoints);
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

  boolean getHra() {
    return hra;
  }

  boolean getLtEq() {
    return lteq;
  }

  @Override
  public float getMin() {
    return minValue;
  }

  @Override
  public float getMax() {
    return maxValue;
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
    validateSplits(splitPoints);
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
    final int numComp = compactors.size();
    final int numBkts = splits.length + 1;
    final long[] buckets = new long[numBkts];
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer buf = c.getBuffer();
      final int weight = 1 << c.getLgWeight();
      for (int j = 0; j < (numBkts - 1); j++) {
        buckets[j] += buf.getCountLtOrEq(splits[j], lteq) * weight;
      }
    }
    buckets[numBkts - 1] = getN();
    return buckets;
  }

  @Override
  public float getQuantile(final float normRank) {
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
  public float[] getQuantiles(final float[] normRanks) {
    final int len = normRanks.length;
    final float[] qArr = new float[len];
    for (int i = 0; i < len; i++) {
      qArr[i] = getQuantile(normRanks[i]);
    }
    return qArr;
  }

  @Override
  public float getRank(final float value) {
    int nnRank = 0;
    final int numComp = compactors.size();
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer buf = c.getBuffer();
      final float v = value;
      final int count = buf.getCountLtOrEq(v, lteq);
      nnRank += count * (1 << c.getLgWeight());
    }
    return (float)nnRank / totalN;
  }

  @Override
  public float[] getRanks(final float[] values) {
    final int numValues = values.length;
    final int numComp = compactors.size();
    final float[] rArr = new float[numValues];
    final int[] cumNnrArr = new int[numValues];
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer buf = c.getBuffer();
      final int[] countsArr = buf.getCountsLtOrEq(values, lteq);
      for (int j = 0; j < numValues; j++) {
        cumNnrArr[j] += countsArr[j];
      }
    }
    for (int j = 0; j < numValues; j++) {
      rArr[j] = (float) cumNnrArr[j] / totalN;
    }
    return rArr;
  }

  @Override
  public int getRetainedEntries() { return size; }

  private void grow() {
    compactors.add(new ReqCompactor(k, getNumLevels(), hra, debug));
    updateMaxNomSize();
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
    //Grow until self has at least as many compactors as other
    while (getNumLevels() < other.getNumLevels()) { grow(); }
    //Merge the items in all height compactors
    for (int i = 0; i < getNumLevels(); i++) {
      compactors.get(i).merge(other.compactors.get(i));
    }
    updateRetainedItems();
    // After merging, we should not be lazy when compressing the sketch (as the maxNomSize bound may
    // be exceeded on many levels)
    if (size >= maxNomSize) { compress(false); }
    updateRetainedItems();
    assert size < maxNomSize;
    aux = null;
    return this;
  }

  @Override
  public String toString(final String fmt, final boolean dataDetail) {
    final StringBuilder sb = new StringBuilder();
    sb.append("**********Relative Error Quantiles Sketch Summary**********").append(LS);
    final int numC = compactors.size();
    sb.append("  N               : " + totalN).append(LS);
    sb.append("  Retained Entries: " + size).append(LS);
    sb.append("  Max Nominal Size: " + maxNomSize).append(LS);
    sb.append("  Min Value       : " + minValue).append(LS);
    sb.append("  Max Value       : " + maxValue).append(LS);
    sb.append("  LtEq Criterion  : " + lteq).append(LS);
    sb.append("  High Rank Acc   : " + hra).append(LS);
    sb.append("  Levels          : " + compactors.size()).append(LS);
    if (dataDetail) {
      for (int i = 0; i < numC; i++) {
        final ReqCompactor c = compactors.get(i);
        sb.append(c.toHorizontalList(fmt, 24, 16));
      }
    }
    sb.append("************************End Summary************************").append(LS + LS);
    return sb.toString();
  }

  @Override
  public void update(final float item) {
    if (!Float.isFinite(item)) {
      throw new SketchesArgumentException("Input float values must be finite.");
    }
    updateBuf.append(item);
    size++;
    totalN++;
    minValue = (item < minValue) ? item : minValue;
    maxValue = (item > maxValue) ? item : maxValue;
    if (size >= maxNomSize) {
      updateBuf.sort();
      final FloatBuffer buf = compactors.get(0).getBuffer();
      buf.mergeSortIn(updateBuf);
      updateBuf.trimLength(0);
      compress(true);
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
   * Computes the size for the sketch.
   */
  private void updateRetainedItems() {
    int count = 0;
    for (ReqCompactor c : compactors) { count += c.getBuffer().getLength(); }
    size = count;
  }

  //debug print functions

  private static void printAddCompactor(final int h, final int retEnt, final int nomCap) {
    final StringBuilder sb = new StringBuilder();
    sb.append("  ");
    sb.append("Must Add Compactor: len(c[").append(h).append("]): ");
    sb.append(retEnt).append(" >= c[").append(h).append("].nomCapacity(): ").append(nomCap);
    println(sb.toString());
  }

  private void printStartCompress() {
    final StringBuilder sb = new StringBuilder();
    sb.append("COMPRESS: ");
    sb.append("skSize: ").append(size).append(" >= ");
    sb.append("MaxNomSize: ").append(maxNomSize);
    sb.append("  N: ").append(totalN);
    println(sb.toString());
  }

  private void printAllHorizList() {
    for (int h = 0; h < compactors.size(); h++) {
      final ReqCompactor c = compactors.get(h);
      print(c.toHorizontalList("%4.0f", 24, 16));
    }
  }

}
