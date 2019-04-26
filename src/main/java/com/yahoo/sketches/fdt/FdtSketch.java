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
 * <p>Suppose our data is a stream of pairs {IP address, User ID} and we want to identify the
 * IP addresses that have the most distinct User IDs.  Or conversely, we would like to identify
 * the User IDs that have the most distinct IP addresses. This is a common challenge in the
 * analysis of big data and the FDT sketch helps solve this problem using probabilistic techniques.
 *
 * <p>More generally, given a multiset of tuples with dimensions <i>{d1,d2, d3, ..., dN}</i>,
 * and a primary subset of dimensions <i>M &lt; N</i>, our task is to identify the combinations of
 * <i>M</i> subset dimensions that have the most frequent number of distinct combinations of
 * the <i>N-M</i> non-primary dimensions.
 *
 * <p>Please refer to the web page
 * <a href="https://datasketches.github.io/docs/Frequency/FrequentDistinctTuplesSketch.html">
 * https://datasketches.github.io/docs/Frequency/FrequentDistinctTuplesSketch.html</a> for a more
 * complete discussion about this sketch.
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
   * @param numStdDev the number of standard deviations for the upper and lower error bounds,
   * this value is an integer and must be one of 1, 2, or 3.
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param sep the separator character
   * @return an ordered List of Groups of the most frequent distinct population of subset tuples
   * represented by the count of entries of each group.
   */
  public List<Group> getResult(final int[] priKeyIndices, final int limit, final int numStdDev,
      final char sep) {
    final PostProcessor proc = new PostProcessor(this, new Group(), sep);
    return proc.getGroupList(priKeyIndices, numStdDev, limit);
  }

  /**
   * Returns the PostProcessor that enables multiple queries against the sketch results.
   * This assumes the default Group and the default separator character '|'.
   * @return the PostProcessor
   */
  public PostProcessor getPostProcessor() {
    return getPostProcessor(new Group(), '|');
  }

  /**
   * Returns the PostProcessor that enables multiple queries against the sketch results.
   * @param group the Group class to use during post processing.
   * @param sep the separator character.
   * @return the PostProcessor
   */
  public PostProcessor getPostProcessor(final Group group, final char sep) {
    return new PostProcessor(this, group, sep);
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
