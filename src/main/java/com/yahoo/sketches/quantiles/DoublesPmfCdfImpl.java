/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

/**
 * The PMF and CDF algorithms for quantiles.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DoublesPmfCdfImpl {

  static double[] getPMFOrCDF(final DoublesSketch sketch, final double[] splitPoints, final boolean isCDF) {
    final double[] buckets = internalBuildHistogram(sketch, splitPoints);
    final long n = sketch.getN();
    if (isCDF) {
      double subtotal = 0;
      for (int j = 0; j < buckets.length; j++) {
        subtotal += buckets[j];
        buckets[j] = subtotal / n; //normalize by n
      }
    } else { // PMF
      for (int j = 0; j < buckets.length; j++) {
        buckets[j] /= n; //normalize by n
      }
    }
    return buckets;
  }

  /**
   * Shared algorithm for both PMF and CDF functions. The splitPoints must be unique, monotonically
   * increasing values.
   * @param sketch the given quantiles DoublesSketch
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing doubles
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * @return the unnormalized, accumulated counts of <i>m + 1</i> intervals.
   */
  private static double[] internalBuildHistogram(final DoublesSketch sketch, final double[] splitPoints) {
    final DoublesSketchAccessor sketchAccessor = DoublesSketchAccessor.wrap(sketch);
    Util.checkSplitPointsOrder(splitPoints);

    final int numSplitPoints = splitPoints.length;
    final int numCounters = numSplitPoints + 1;
    final double[] counters = new double[numCounters];

    long weight = 1;
    sketchAccessor.setLevel(DoublesSketchAccessor.BB_LVL_IDX);
    if (numSplitPoints < 50) { // empirically determined crossover
      // sort not worth it when few split points
      DoublesPmfCdfImpl.bilinearTimeIncrementHistogramCounters(
              sketchAccessor, weight, splitPoints, counters);
    } else {
      sketchAccessor.sort();
      // sort is worth it when many split points
      DoublesPmfCdfImpl.linearTimeIncrementHistogramCounters(
              sketchAccessor, weight, splitPoints, counters);
    }

    long myBitPattern = sketch.getBitPattern();
    final int k = sketch.getK();
    assert myBitPattern == sketch.getN() / (2L * k); // internal consistency check
    for (int lvl = 0; myBitPattern != 0L; lvl++, myBitPattern >>>= 1) {
      weight <<= 1; // double the weight
      if ((myBitPattern & 1L) > 0L) { //valid level exists
        // the levels are already sorted so we can use the fast version
        sketchAccessor.setLevel(lvl);
        DoublesPmfCdfImpl.linearTimeIncrementHistogramCounters(
                sketchAccessor, weight, splitPoints, counters);
      }
    }
    return counters;

  }

  /**
   * Because of the nested loop, cost is O(numSamples * numSplitPoints), which is bilinear.
   * This method does NOT require the samples to be sorted.
   * @param samples DoublesBufferAccessor holding an array of samples
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints + 1 == counters.length.
   * @param counters array of counters
   */
  static void bilinearTimeIncrementHistogramCounters(final DoublesBufferAccessor samples, final long weight,
      final double[] splitPoints, final double[] counters) {
    assert (splitPoints.length + 1 == counters.length);
    for (int i = 0; i < samples.numItems(); i++) {
      final double sample = samples.get(i);
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        final double splitpoint = splitPoints[j];
        if (sample < splitpoint) {
          break;
        }
      }
      assert j < counters.length;
      counters[j] += weight;
    }
  }


  /**
   * This one does a linear time simultaneous walk of the samples and splitPoints. Because this
   * internal procedure is called multiple times, we require the caller to ensure these 3 properties:
   * <ol>
   * <li>samples array must be sorted.</li>
   * <li>splitPoints must be unique and sorted</li>
   * <li>number of SplitPoints + 1 == counters.length</li>
   * </ol>
   * @param samples DoublesBufferAccessor holding an array of samples
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints + 1 = counters.length.
   * @param counters array of counters
   */
  static void linearTimeIncrementHistogramCounters(final DoublesBufferAccessor samples, final long weight,
      final double[] splitPoints, final double[] counters) {
    int i = 0;
    int j = 0;
    while (i < samples.numItems() && j < splitPoints.length) {
      if (samples.get(i) < splitPoints[j]) {
        counters[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket. move on the next bucket.
      }
    }

    // now either i == numSamples(we are out of samples), or
    // j == numSplitPoints(out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case.
    if (j == splitPoints.length) {
      counters[j] += (weight * (samples.numItems() - i));
    }
  }

}
