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
import static org.apache.datasketches.req.FloatBuffer.LS;
import static org.apache.datasketches.req.FloatBuffer.TAB;

import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;


/**
 * This Relative Error Quantiles Sketch is the Java implementation based on the paper
 * "Relative Error Streaming Quantiles", https://arxiv.org/abs/2004.01668.
 *
 * <p>This implementation differs from the algorithm described in the paper in the following:</p>
 * <ul><li>The algorithm requires no upper bound on the stream length.
 * Instead, each relative-compactor counts the number of compaction operations performed
 * so far (variable numCompactions). Initially, the relative-compactor starts with 3 sections.
 * Each time the numCompactions exceeds 2^{numSections - 1}, we double numSections.</li>
 * <li>The size of each section (variable k and sectionSize in the code and parameter k in
 * the paper) is initialized with a value set by the user via variable k.
 * When the number of sections doubles, we decrease sectionSize by a factor of sqrt(2).
 * This is applied at each level separately. Thus, when we double the number of sections, the
 * nominal compactor size increases by a factor of sqrt(2) (up to +-1 after rounding).</li>
 * <li>The merge operation here does not perform "special compactions", which are used in the paper
 * to allow for a tight mathematical analysis of the sketch.</li>
 * </ul>
 *
 * @author Edo Liberty
 * @author Pavel Vesely
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class ReqSketch {
  //An initial upper bound on log_2 (number of compactions) + 1 COMMMENT: Huh?
  final static int INIT_NUMBER_OF_SECTIONS = 3;
  final static int MIN_K = 4;
  //should be even; value of 50 roughly corresponds to 0.01-relative error guarantee with
  //constant probability
  final static int DEFAULT_K = 50;

  private int k;
  private boolean debug = false;
  List<ReqCompactor> compactors = new ArrayList<>();
  private ReqAuxiliary aux = null;
  int size; //number of retained items in the sketch
  private int maxNomSize; //sum of nominal capacities of all compactors
  private long totalN; //total items offered to sketch
  private float minValue = Float.MAX_VALUE;
  private float maxValue = Float.MIN_VALUE;
  //if lteq = true, the compuation of rank and quantiles will be based on a less-than or equals
  // criteria. Otherwise the criteria is strictly less-than, which is consistent with the other
  // quantiles sketches in the library.
  private boolean lteq = false;

  /**
   * Constructor with default k = 50, lteq = false, and debug = false.
   *
   */
  public ReqSketch() {
    this(DEFAULT_K, false, false);
  }

  /**
   * Constructor
   * @param k Controls the size and error of the sketch
   */
  public ReqSketch(final int k) {
    this(k, false, false);
  }

  /**
   * Constructor.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one.
   * @param lteq if true, the compuation of rank and quantiles will be based on a less-than or equals
   * criteria. Otherwise the criteria is strictly less-than, which is consistent with the other
   * quantiles sketches in the library.
   * @param debug debug mode
   */
  public ReqSketch(final int k, final boolean lteq, final boolean debug) {
    this.k = max(k & -2, MIN_K);
    this.lteq = lteq;
    this.debug = debug;
    size = 0;
    maxNomSize = 0;
    totalN = 0;
    if (debug) { println("START:"); }
    grow();
  }

  /**
   * Copy Constructor
   * @param other the other sketch to be deep copied into this one.
   */
  ReqSketch(final ReqSketch other) {
    k = other.k;
    debug = other.debug;
    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.add(new ReqCompactor(other.compactors.get(i)));
    }
    size = other.size;
    maxNomSize = other.maxNomSize;
    totalN = other.totalN;
    aux = null;
  }

  void compress(final boolean lazy) {
    updateMaxNomSize();
    if (debug) { printStartCompress(); }

    if (size < maxNomSize) { return; }
    for (int h = 0; h < compactors.size(); h++) {
      final ReqCompactor c = compactors.get(h);
      final int retEnt = c.getBuffer().getLength();
      final int nomCap = c.getNomCapacity();

      if (retEnt >= nomCap) {
        if ((h + 1) >= getNumLevels()) {
          if (debug) { printAddCompactor(h, retEnt, nomCap); }
          grow(); //add a level
        }
        final FloatBuffer promoted = c.compact();
        compactors.get(h + 1).getBuffer().mergeSortIn(promoted);
        updateRetainedItems();
        if (lazy && (size < maxNomSize)) { break; }
      }
    }
    if (debug) { printAllHorizList(); }
    aux = null;
  }

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of splitPoint (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(false) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these splitpoints.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the splitPoints. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   */
  public double[] getCDF(final float[] splitPoints) {
    if (isEmpty()) { return null; }
    Util.validateValues(splitPoints);
    final long[] buckets = getPMForCDF(splitPoints);
    final int numBkts = buckets.length;
    final double[] outArr = new double[numBkts];
    for (int j = 0; j < numBkts; j++) {
      outArr[j] = (double)buckets[j] / getN();
    }
    return outArr;
  }

  boolean getLtEq() {
    return lteq;
  }

  /**
   * Gets the smallest value seen by this sketch
   * @return the smallest value seen by this sketch
   */
  public float getMin() {
    return minValue;
  }

  /**
   * Gets the largest value seen by this sketch
   * @return the largest value seen by this sketch
   */
  public float getMax() {
    return maxValue;
  }

  /**
   * Gets the total number of items offered to the sketch.
   * @return the total number of items offered to the sketch.
   */
  public long getN() {
    return totalN;
  }

  /**
   * Gets the number of levels of compactors in the sketch.
   * @return the number of levels of compactors in the sketch.
   */
  public int getNumLevels() {
    return compactors.size();
  }

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that be obtained from the
   * getNormalizedRankError(true) function.
   *
   * <p>If the sketch is empty this returns null.</p>
   *
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing double values
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * The definition of an "interval" is inclusive of the left splitPoint (or minimum value) and
   * exclusive of the right splitPoint, with the exception that the last interval will include
   * the maximum value.
   * It is not necessary to include either the min or max values in these splitpoints.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values (the mass) that fall into one of those intervals.
   * The definition of an "interval" is inclusive of the left splitPoint and exclusive of the right
   * splitPoint, with the exception that the last interval will include maximum value.
   */
  public double[] getPMF(final float[] splitPoints) {
    if (isEmpty()) { return null; }
    Util.validateValues(splitPoints);
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
   * @param splitPoints the splitPoints array
   * @return a CDF in raw counts
   */
  private long[] getPMForCDF(final float[] splitPoints) {
    final int numComp = compactors.size();
    final int numBkts = splitPoints.length + 1;
    final long[] buckets = new long[numBkts];
    final double[] outArr = new double[numBkts];
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer buf = c.getBuffer();
      final int weight = 1 << c.getLgWeight();
      for (int j = 0; j < (numBkts - 1); j++) {
        buckets[j] += buf.getCountLtOrEq(splitPoints[j], lteq) * weight;
      }
    }
    buckets[numBkts - 1] = getN();
    return buckets;
  }

  /**
   * Gets the quantile of the largest normalized rank that is less-than the given normalized rank;
   * or, if lteq is true, this gets the quantile of the largest normalized rank that is less-than or
   * equal to the given normalized rank.
   * The normalized rank must be in the range [0.0, 1.0] (inclusive, inclusive).
   * A given normalized rank of 0.0 will return the minimum value from the stream.
   * A given normalized rank of 1.0 will return the maximum value from the stream.
   * @param normRank the given normalized rank
   * @return the largest quantile less than the given normalized rank.
   */
  public float getQuantile(final float normRank) {
    if ((normRank < 0) || (normRank > 1.0)) {
      throw new SketchesArgumentException(
          "Normalized rank must be in the range [0.0, 1.0]: " + normRank);
    }
    //if (normRank == 0.0) { return minValue; }
    //if (normRank == 1.0) { return maxValue; }
    if (aux == null) {
      aux = new ReqAuxiliary(this);
    }
    final float q = aux.getQuantile(normRank, lteq);
    //if (Float.isNaN(q)) { return minValue; }
    return q;
  }

  /**
   * Gets an array of quantiles that correspond to the given array of normalized ranks.
   * @param normRanks the given array of normalized ranks.
   * @return the array of quantiles that correspond to the given array of normalized ranks.
   * @see #getQuantile(float)
   */
  public float[] getQuantiles(final float[] normRanks) {
    final int len = normRanks.length;
    final float[] qArr = new float[len];
    for (int i = 0; i < len; i++) {
      qArr[i] = getQuantile(normRanks[i]);
    }
    return qArr;
  }

  /**
   * Computes the normalized rank of the given value in the stream.
   * The normalized rank is the fraction of values less than the given value;
   * or if lteq is true, the fraction of values less than or equal to the given value.
   * @param value the given value
   * @return the normalized rank of the given value in the stream.
   */
  public float getRank(final float value) {
    int nnRank = 0;
    final int numComp = compactors.size();
    for (int i = 0; i < numComp; i++) {
      final ReqCompactor c = compactors.get(i);
      final FloatBuffer buf = c.getBuffer();
      final int count = buf.getCountLtOrEq(value, lteq);
      nnRank += count * (1 << c.getLgWeight());
    }
    return (float)nnRank / totalN;
  }

  /**
   * Gets an array of normalized ranks that correspond to the given array of values.
   * @param values the given array of values.
   * @return the  array of normalized ranks that correspond to the given array of values.
   * @see #getRank(float)
   */
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

  /**
   * Gets the number of retained entries of this sketch
   * @return the number of retained entries of this sketch
   */
  public int getRetainedEntries() { return size; }

  void grow() {
    compactors.add( new ReqCompactor(k, getNumLevels(), debug));
    updateMaxNomSize();
  }

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  public boolean isEmpty() {
    return totalN == 0;
  }

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  public boolean isEstimationMode() {
    return getNumLevels() > 1;
  }

  /**
   * Returns an iterator for all the items in this sketch.
   * @return an iterator for all the items in this sketch.
   */
  public ReqIterator iterator() {
    return new ReqIterator(this);
  }

  /**
   * Merge other sketch into this one. The other sketch is not modified.
   * @param other sketch to be merged into this one.
   * @return this
   */
  public ReqSketch merge(final ReqSketch other) {
    //Grow until self has at least as many compactors as other
    while (getNumLevels() < other.getNumLevels()) { grow(); }
    //Append the items in same height compactors
    for (int i = 0; i < getNumLevels(); i++) {
      final ReqCompactor c = compactors.get(i);
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

  /**
   * Returns a summary of the sketch and the horizontal lists for all compactors.
   * @param fmt The format for each printed item.
   * @param dataDetail show all the retained data from all the compactors.
   * @return a summary of the sketch and the horizontal lists for all compactors.
   */
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

    sb.append("  Levels          : " + compactors.size()).append(LS);
    if (dataDetail) {
      for (int i = 0; i < numC; i++) {
        final ReqCompactor c = compactors.get(i);
        sb.append("  " + c.toHorizontalList(fmt, 24, 12));
      }
    }
    sb.append("************************End Summary************************").append(LS + LS);
    return sb.toString();
  }

  /**
   * Updates this sketch with the given item.
   * @param item the given item
   */
  public void update(final float item) {
    if (!Float.isFinite(item)) {
      throw new SketchesArgumentException("Input float values must be finite.");
    }
    final FloatBuffer buf = compactors.get(0).getBuffer().append(item);
    size++;
    totalN++;
    minValue = (item < minValue) ? item : minValue;
    maxValue = (item > maxValue) ? item : maxValue;
    if (size >= maxNomSize) {
      buf.sort();
      compress(true);
    }
    aux = null;
  }

/**
 * Computes a new bound for determining when to compress the sketch.
 * @return this
 */
ReqSketch updateMaxNomSize() {
  int cap = 0;
  for (ReqCompactor c : compactors) { cap += c.getNomCapacity(); }
  maxNomSize = cap;
  return this;
}

/**
 * Computes the size for the sketch.
 * @return this
 */
ReqSketch updateRetainedItems() {
  int count = 0;
  for (ReqCompactor c : compactors) { count += c.getBuffer().getLength(); }
  size = count;
  return this;
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
  sb.append("; N: ").append(totalN);
  println(sb.toString());
}

private void printAllHorizList() {
  for (int h = 0; h < compactors.size(); h++) {
    final ReqCompactor c = compactors.get(h);
    print(c.toHorizontalList("%4.0f", 24, 10));
  }
  println("COMPRESS: DONE: sKsize: " + size + TAB + "MaxNomSize: " + maxNomSize + LS);
}

  //temporary
  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }

}
