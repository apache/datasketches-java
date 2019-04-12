/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

import static com.yahoo.sketches.Util.MAX_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.simpleIntLog2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.yahoo.sketches.BinomialBoundsN;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSketch;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;

/**
 * A Frequent Distinct Tuples sketch.
 *
 * <p>Given a multiset of tuples with dimensions <i>{d1,d2, d3, ..., dN}</i>, and a primary subset of
 * dimensions <i>M < N</i>, the task is to identify the combinations of <i>M</i> subset dimensions
 * that have the most frequent number of distinct combinations of the <i>N-M</i> non-primary
 * dimensions.
 *
 * <p>For example, assume <i>N=3, M=2</i>, where the primary dimensions are <i>(d1, d2)</i>.
 * After populating the sketch with a stream of tuples, we wish to identify the primary dimension
 * combinations of <i>(d1, d2)</i> that have the most frequent number of distinct occurrences of
 * <i>d3</i>.
 *
 * <p>Alternatively, if we choose the primary dimension as <i>d1</i>, then we can identify the
 * <i>d1</i>s that have the most frequent distinct combinations of <i>(d2 and d3)</i>. The choice of
 * which dimensions to choose as the primary dimensions is performed in a post-processing phase
 * after the sketch has been populated.
 *
 * <p>As a simple concrete example, let's assume <i>N = 2</i> and let <i>d1 := IP address</i>, and
 * <i>d2 := User ID</i>.
 * Let's choose <i>d1</i> as the primary dimension, then the sketch allows the identification of the
 * <i>IP addresses</i> that have the largest populations of distinct <i>User IDs</i>. Conversely,
 * if we choose <i>d2</i> as the primary dimension, the sketch allows the identification of the
 * <i>User IDs</i> with the largest populations of distinct <i>IP addresses</i>.
 *
 * <p>An important caveat is that if the distribution is too flat, there may not be any
 * "most frequent" combinations of the primary keys above the threshold. Also, if one primary key
 * is too dominant, the sketch may be able to only report on the single most frequent primary
 * key combination, which means the possible existance of false negatives.
 *
 * <p>In this implementation the input tuples presented to the sketch are string arrays.
 *
 * @author Lee Rhodes
 */
public class FdtSketch {
  private final int lgK;
  private final ArrayOfStringsSketch sketch;

  /**
   * Create new instance of Frequent Distinct Tuples sketch with the given
   * Log-base2 of required nominal entries.
   * @param lgK Log-base2 of required nominal entries.
   */
  public FdtSketch(final int lgK) {
    this.lgK = lgK;
    sketch = new ArrayOfStringsSketch(lgK);
  }

  /**
   * Used by deserialization.
   * @param sketch a ArrayOfStringsSketch
   */
  FdtSketch(final ArrayOfStringsSketch sketch) {
    this.sketch = sketch;
    lgK = simpleIntLog2(sketch.getNominalEntries());
  }

  /**
   * Create a new instance of Frequent Distinct Tuples sketch with the given
   * threshold and rse.
   * @param threshold : the fraction, between zero and 1.0, of the total stream length that defines
   * a "Frequent" (or heavy) item.
   * @param rse the maximum Relative Standard Error for the estimate of the distinct population of a
   * reported tuple (selected with a primary key) at the threshold.
   */
  public FdtSketch(final double threshold, final double rse) {
    lgK = computeLgK(threshold, rse);
    sketch = new ArrayOfStringsSketch(lgK);
  }

  /**
   * Gets the Log_base2 of K for the sketch
   * @return the Log_base2 of K for the sketch
   */
  public int getLgK() {
    return lgK;
  }

  /**
   * Gets the estimate of the distinct population of tuples represented by the entire sketch.
   * @return the estimate of the distinct population represented by the entire sketch.
   */
  public double getEstimate() {
    return sketch.getEstimate();
  }

  /**
   * Gets the estimate of the distinct population of subset tuples represented by the count of
   * entries of that subset.
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the distinct population represented by the sketch subset.
   */
  public double getEstimate(final int numSubsetEntries) {
    if (!sketch.isEstimationMode()) { return numSubsetEntries; }
    return numSubsetEntries / sketch.getTheta();
  }

  /**
   * Gets the estimate of the lower bound of the distinct population of tuples represented by the
   * entire sketch, given the number of standard deviations.
   * given numStdDev.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the estimate of the lower bound of the distinct population represented by the entire
   * sketch given numStdDev.
   */
  public double getLowerBound(final int numStdDev) {
    return sketch.getLowerBound(numStdDev);
  }

  /**
   * Gets the estimate of the lower bound of the distinct population represented by a sketch subset,
   * given numStdDev and numSubsetEntries.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the lower bound of the distinct population represented by the sketch
   * subset given numStdDev and numSubsetEntries.
   */
  public double getLowerBound(final int numStdDev, final int numSubsetEntries) {
    if (!sketch.isEstimationMode()) { return numSubsetEntries; }
    return BinomialBoundsN.getLowerBound(numSubsetEntries, sketch.getTheta(), numStdDev, sketch.isEmpty());
  }

  /**
   * Gets the estimate of the upper bound of the distinct population represented by the entire sketch,
   * given numStdDev.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the estimate of the upper bound of the distinct population represented by the entire
   * sketch given numStdDev.
   */
  public double getUpperBound(final int numStdDev) {
    return sketch.getUpperBound(numStdDev);
  }

  /**
   * Gets the estimate of the upper bound of the distinct population represented by a sketch subset,
   * given numStdDev and numSubsetEntries.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the upper bound of the distinct population represented by the sketch
   * subset given numStdDev and numSubsetEntries.
   */
  public double getUpperBound(final int numStdDev, final int numSubsetEntries) {
    if (!sketch.isEstimationMode()) { return numSubsetEntries; }
    return BinomialBoundsN.getUpperBound(numSubsetEntries, sketch.getTheta(), numStdDev, sketch.isEmpty());
  }

  /**
   * Returns the sketch iterator over the summaries retained by the sketch.
   * @return the iterator over the summaries retained by the sketch.
   */
  public SketchIterator<ArrayOfStringsSummary> iterator() {
    return sketch.iterator();
  }

  /**
   * Update the sketch with the given string array tuple.
   * @param tuple the given string array tuple.
   */
  public void update(final String[] tuple) {
    sketch.update(tuple, tuple);
  }

  /**
   * Returns a byte array representing the contents of this sketch.
   * @return a byte array representing the contents of this sketch.
   */
  public byte[] toByteArray() {
    return sketch.toByteArray();
  }

  //Post processing

  /**
   * blah
   * @param priKeyIndices blah
   */
  @SuppressWarnings("unchecked")
  public void prepare(final int[] priKeyIndices) {
    final int entries = sketch.getRetainedEntries();
    final int tableSize = (int) (entries / 0.75);
    final Map<String, Integer> map = new HashMap<>(tableSize);
    final SketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
    while (it.next()) {
      final String[] arr = it.getSummary().getValue();
      final String priKey = getPrimaryKey(arr, priKeyIndices);
      map.compute(priKey, (k, v) -> (v == null) ? 1 : v + 1);
    }
    final Object[] entryArr = map.entrySet().toArray();
    //reverse order
    Arrays.sort(entryArr, (a, b) ->
      ((Map.Entry<String, Integer>)b).getValue() - ((Map.Entry<String, Integer>)a).getValue() );
    final int len = entryArr.length;
    System.out.println(Row.getRowHeader());
    for (int i = 0; i < len; i++) {
      final String s = ((Map.Entry<String, Integer>)entryArr[i]).getKey();
      final int est =  ((Map.Entry<String, Integer>)entryArr[i]).getValue();
      final double ub = getUpperBound(2, est);
      final double lb = getLowerBound(2, est);
      final Row<String> row = new Row<>(s, est, ub, lb);
      System.out.println(row.toString());
    }
  }



  static String getPrimaryKey(final String[] arr, final int[] priKeyIndices) {
    assert priKeyIndices.length < arr.length;
    final StringBuilder sb = new StringBuilder();
    final int keys = priKeyIndices.length;
    for (int i = 0; i < keys; i++) {
      final int idx = priKeyIndices[i];
      sb.append(arr[idx]);
      if ((i + 1) < keys) { sb.append(","); }
    }
    return sb.toString();
  }

  /**
   * Computes LgK given the threshold and RSE.
   * @param threshold the fraction, between zero and 1.0, of the total stream length that defines
   * a "Frequent" (or heavy) tuple.
   * @param rse the maximum Relative Standard Error for the estimate of the distinct population of a
   * reported tuple (selected with a primary key) at the threshold.
   * @return LgK
   */
  static int computeLgK(final double threshold, final double rse) {
    final double v = Math.ceil(1.0 / (threshold * rse * rse));
    final int lgK = (int) Math.ceil(Math.log(v) / Math.log(2));
    if (lgK > MAX_LG_NOM_LONGS) {
      throw new SketchesArgumentException("Requested Sketch (LgK = " + lgK + ") is too large, "
          + "either increase the threshold, the rse or both.");
    }
    return lgK;
  }

}
