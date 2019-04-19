/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

import static com.yahoo.sketches.Util.MAX_LG_NOM_LONGS;

import java.util.List;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.BinomialBoundsN;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSketch;

/**
 * A Frequent Distinct Tuples sketch.
 *
 * <p>Given a multiset of tuples with dimensions <i>{d1,d2, d3, ..., dN}</i>, and a primary subset of
 * dimensions <i>M &lt; N</i>, the task is to identify the combinations of <i>M</i> subset dimensions
 * that have the most frequent number of distinct combinations of the <i>N-M</i> non-primary
 * dimensions.
 *
 * <p>We define a specific combination of the <i>M</i> primary dimensions as a <i>Primary Key</i>
 * and all combinations of the <i>M</i> primary dimensions as the set of <i>Primary Keys</i>.
 *
 * <p>We define the set of all combinations of <i>N-M</i> non-primary dimensions associated with a
 * single primary key as a <i>Group</i>.
 *
 * <p>For example, assume <i>N=3, M=2</i>, where the set of Primary Keys are defined by
 * <i>{d1, d2}</i>. After populating the sketch with a stream of tuples all of size <i>N</i>,
 * we wish to identify the Primary Keys that have the most frequent number of distinct occurrences
 * of <i>{d3}</i>. Equivalently, we want to identify the Primary Keys with the largest Groups.
 *
 * <p>Alternatively, if we choose the Primary Key as <i>{d1}</i>, then we can identify the
 * <i>{d1}</i>s that have the largest groups of <i>{d2, d3}</i>. The choice of
 * which dimensions to choose for the Primary Keys is performed in a post-processing phase
 * after the sketch has been populated. Thus, multiple queries can be performed against the
 * populated sketch with different selections of Primary Keys.
 *
 * <p>As a simple concrete example, let's assume <i>N = 2</i> and let <i>d1 := IP address</i>, and
 * <i>d2 := User ID</i>.
 * Let's choose <i>{d1}</i> as the Primary Keys, then the sketch allows the identification of the
 * <i>IP addresses</i> that have the largest populations of distinct <i>User IDs</i>. Conversely,
 * if we choose <i>{d2}</i> as the Primary Keys, the sketch allows the identification of the
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
public class FdtSketch extends ArrayOfStringsSketch {

  /**
   * Create new instance of Frequent Distinct Tuples sketch with the given
   * Log-base2 of required nominal entries.
   * @param lgK Log-base2 of required nominal entries.
   */
  public FdtSketch(final int lgK) {
    super(lgK);
  }

  /**
   * Used by deserialization.
   * @param mem the image of a FdtSketch
   */
  FdtSketch(final Memory mem) {
    super(mem);
  }

  /**
   * Create a new instance of Frequent Distinct Tuples sketch with a size determined by the given
   * threshold and rse.
   * @param threshold : the fraction, between zero and 1.0, of the total distinct stream length
   * that defines a "Frequent" (or heavy) item.
   * @param rse the maximum Relative Standard Error for the estimate of the distinct population of a
   * reported tuple (selected with a primary key) at the threshold.
   * @throws SketchArguementException if the choices of threshold and rse would require a sketch
   * larger than 2^26.
   */
  public FdtSketch(final double threshold, final double rse) {
    super(computeLgK(threshold, rse));
  }

  /**
   * Update the sketch with the given string array tuple.
   * @param tuple the given string array tuple.
   */
  public void update(final String[] tuple) {
    super.update(tuple, tuple);
  }

  /**
   * Returns an ordered List of Groups of the most frequent distinct population of subset tuples
   * represented by the count of entries of each group.
   * @param priKeyIndices these indices define the dimensions used for the Primary Keys.
   * @param limit the maximum number of groups to return. If this value is &le; 0, all
   * groups will be returned.
   * @param numStdDev the number of standard deviations for the error bounds, this value is an
   * integer and must be one of 1, 2, or 3.
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return an ordered List of Groups of the most frequent distinct population of subset tuples
   * represented by the count of entries of each group.
   */
  public List<Group> getResult(final int[] priKeyIndices, final int limit, final int numStdDev) {
    final PostProcessor proc = new PostProcessor(this, new Group());
    return proc.getGroupList(priKeyIndices, numStdDev, limit);
  }

  /**
   * Returns the PostProcessor that enables multiple queries against the sketch results.
   * @param group the Group class to use during post processing.
   * @return the PostProcessor
   */
  public PostProcessor getPostProcessor(final Group group) {
    return new PostProcessor(this, group);
  }

  /**
   * Gets the estimate of the true distinct population of subset tuples represented by the count
   * of entries in a group. This is primarily used internally.
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the true distinct population of subset tuples represented by the count
   * of entries in a group.
   */
  public double getEstimate(final int numSubsetEntries) {
    if (!isEstimationMode()) { return numSubsetEntries; }
    return numSubsetEntries / getTheta();
  }

  /**
   * Gets the estimate of the lower bound of the true distinct population represented by the count
   * of entries in a group.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the lower bound of the true distinct population represented by the count
   * of entries in a group.
   */
  public double getLowerBound(final int numStdDev, final int numSubsetEntries) {
    if (!isEstimationMode()) { return numSubsetEntries; }
    return BinomialBoundsN.getLowerBound(numSubsetEntries, getTheta(), numStdDev, isEmpty());
  }

  /**
   * Gets the estimate of the upper bound of the true distinct population represented by the count
   * of entries in a group.
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param numSubsetEntries number of entries for a chosen subset of the sketch.
   * @return the estimate of the upper bound of the true distinct population represented by the count
   * of entries in a group.
   */
  public double getUpperBound(final int numStdDev, final int numSubsetEntries) {
    if (!isEstimationMode()) { return numSubsetEntries; }
    return BinomialBoundsN.getUpperBound(numSubsetEntries, getTheta(), numStdDev, isEmpty());
  }

  // Restricted

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
      throw new SketchesArgumentException("Requested Sketch (LgK = " + lgK + " &gt; 2^26), "
          + "either increase the threshold, the rse or both.");
    }
    return lgK;
  }

}
