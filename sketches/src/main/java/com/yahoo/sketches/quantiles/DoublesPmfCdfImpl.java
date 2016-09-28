/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Arrays;

public class DoublesPmfCdfImpl {

  static double[] getPMFOrCDF(DoublesSketch sketch, double[] splitPoints, boolean isCDF) {
    long[] counters = internalBuildHistogram(sketch, splitPoints);
    int numCounters = counters.length;
    double[] result = new double[numCounters];
    double n = sketch.getN();
    long subtotal = 0;
    if (isCDF) {
      for (int j = 0; j < numCounters; j++) {
        long count = counters[j];
        subtotal += count;
        result[j] = subtotal / n; //normalize by n
      }
    } else { // PMF
      for (int j = 0; j < numCounters; j++) {
        long count = counters[j];
        subtotal += count;
        result[j] = count / n; //normalize by n
      }
    }
    assert subtotal == n; //internal consistency check
    return result;
  }
  
  /**
   * Shared algorithm for both PMF and CDF functions. The splitPoints must be unique, monotonically
   * increasing values.
   * @param sketch the given quantiles DoublesSketch
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing doubles
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * @return the unnormalized, accumulated counts of <i>m + 1</i> intervals.
   */
  static long[] internalBuildHistogram(final DoublesSketch sketch, final double[] splitPoints) {
    final double[] levelsArr  = sketch.getCombinedBuffer();
    final double[] baseBuffer = levelsArr;
    final int bbCount = sketch.getBaseBufferCount();
    Util.validateValues(splitPoints);

    final int numSplitPoints = splitPoints.length;
    final int numCounters = numSplitPoints + 1;
    final long[] counters = new long[numCounters];

    long weight = 1;
    if (numSplitPoints < 50) { // empirically determined crossover
      // sort not worth it when few split points
      DoublesPmfCdfImpl.bilinearTimeIncrementHistogramCounters(
          baseBuffer, 0, bbCount, weight, splitPoints, counters);
    } else {
      Arrays.sort(baseBuffer, 0, bbCount);
      // sort is worth it when many split points
      DoublesPmfCdfImpl.linearTimeIncrementHistogramCounters(
          baseBuffer, 0, bbCount, weight, splitPoints, counters);
    }

    long myBitPattern = sketch.getBitPattern();
    final int k = sketch.getK();
    assert myBitPattern == sketch.getN() / (2L * k); // internal consistency check
    for (int lvl = 0; myBitPattern != 0L; lvl++, myBitPattern >>>= 1) {
      weight += weight; // *= 2
      if ((myBitPattern & 1L) > 0L) { //valid level exists
        // the levels are already sorted so we can use the fast version
        DoublesPmfCdfImpl.linearTimeIncrementHistogramCounters(
            levelsArr, (2 + lvl) * k, k, weight, splitPoints, counters);
      }
    }
    return counters;
  }
  
  /**
   * Because of the nested loop, cost is O(numSamples * numSplitPoints), which is bilinear.
   * This method does NOT require the samples to be sorted.
   * @param samples array of samples
   * @param offset into samples array
   * @param numSamples number of samples in samples array
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints + 1 == counters.length.
   * @param counters array of counters
   */
  static void bilinearTimeIncrementHistogramCounters(final double[] samples, final int offset, 
      final int numSamples, final long weight, final double[] splitPoints, final long[] counters) {
    assert (splitPoints.length + 1 == counters.length);
    for (int i = 0; i < numSamples; i++) { 
      final double sample = samples[i + offset];
      int j = 0;
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
   * @param samples sorted array of samples
   * @param offset into samples array
   * @param numSamples number of samples in samples array
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints + 1 = counters.length.
   * @param counters array of counters
   */
  static void linearTimeIncrementHistogramCounters(final double[] samples, final int offset, 
      final int numSamples, final long weight, final double[] splitPoints, final long[] counters) {
    int i = 0;
    int j = 0;
    while (i < numSamples && j < splitPoints.length) {
      if (samples[i + offset] < splitPoints[j]) {
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
      counters[j] += (weight * (numSamples - i));
    }
  }
  
}
