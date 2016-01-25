/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static java.lang.System.arraycopy;

import java.util.Arrays;
import java.util.Random;

/**
 * Utility class for quantiles sketches.
 * 
 * <p>This class contains a highly specialized sort called blockyTandemMergeSort().
 * It also contains methods that are used while building histograms and other common
 * functions.</p>
 */
final class Util {
  /**
   * The java line separator character as a String.
   */
  public static String LS = System.getProperty("line.separator");
  
  /**
   * The tab character
   */
  public static final char TAB = '\t';
  
  static Random rand = new Random();
  
  /**
   * Returns the current element capacity of the combined data buffer given <i>k</i> and <i>n</i>.
   * 
   * @param k sketch parameter. This determines the accuracy of the sketch and the 
   * size of the updatable data structure, which is a function of k.
   * 
   * @param n The number of elements in the input stream
   * @return the current element capacity of the combined data buffer
   */
  static int bufferElementCapacity(int k, long n) {
    int maxLevels = computeNumLevelsNeeded(k, n);
    int minBB = QuantilesSketch.MIN_BASE_BUF_SIZE;
    if (n <= minBB) return minBB;
    int minBBCnt = Math.max(computeBaseBufferCount(k, n), minBB);
    int maxBBcap = 2*k;
    int bbCap = (maxLevels > 0)? maxBBcap : Math.min(maxBBcap, ceilingPowerOf2(minBBCnt));

  
    return bbCap + maxLevels*k;
  }

  /**
   * Computes the number of valid levels above the base buffer
   * @return the number of valid levels above the base buffer
   */
  static int numValidLevels(long bitPattern) {
    return Long.bitCount(bitPattern);
  }

  /**
   * Computes the base buffer count given k, n
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch
   * @return the base buffer count
   */
  static int computeBaseBufferCount(int k, long n) {
    return (int) (n % (2L * k));
  }

  /**
   * Computes the levels bit pattern given k, n.
   * This is computed as <i>n / (2*k)</i>.
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the levels bit pattern
   */
  static long computeBitPattern(int k, long n) {
    return n / (2L * k);
  }

  //used by HeapQS
  static double lg(double x) {
    return ( Math.log(x)) / (Math.log(2.0) );
  }
  
  /**
   * Computes the number of logarithmic levels needed given k and n.
   * This is equivalent to max(floor(lg(n/k), 0).
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the number of levels needed.
   */
  static int computeNumLevelsNeeded(int k, long n) {
    return 1 + hiBitPos(n / (2L * k));
  }
  
  /**
   * Zero based position of the highest one-bit of the given long
   * @param num the given long
   * @return Zero based position of the highest one-bit of the given long
   */
  static int hiBitPos(long num) {
    return 63 - Long.numberOfLeadingZeros(num);
  }

  //used by HeapQS
  static int positionOfLowestZeroBitStartingAt(long numIn, int startingPos) {
    long num = numIn >>> startingPos;
    int pos = 0;
    while ((num & 1L) != 0) {
      num = num >>> 1;
      pos++;
    }
    return (pos + startingPos);
  }

  //Used by HeapQS
  static double sumOfDoublesInSubArray(double[] arr, int subArrayStart, int subArrayLength) {
    double total = 0.0;
    int subArrayStop = subArrayStart + subArrayLength;
    for (int i = subArrayStart; i < subArrayStop; i++) {
      total += arr[i];
    }
    return total;
  }
  
  /**
   * Because of the nested loop, cost is O(numSamples * numSplitPoints), which is bilinear.
   * This method does NOT require the samples to be sorted.
   * @param samples array of samples
   * @param offset into samples array
   * @param numSamples number of samples in samples array
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints +1 == counters.length.
   * @param counters array of counters
   */ //used by HeapQS and MQS
  static void bilinearTimeIncrementHistogramCounters(double[] samples, int offset, int numSamples, 
      long weight, double[] splitPoints, long[] counters) {
    assert (splitPoints.length + 1 == counters.length);
    for (int i = 0; i < numSamples; i++) {
      double sample = samples[i+offset];
      int j = 0;

      for (j = 0; j < splitPoints.length; j++) {
        double splitpoint = splitPoints[j];
        if (sample < splitpoint) { 
          break;
        }
      }
      assert j < counters.length;
      // System.out.printf("%.2f in bucket %d\n", sample, j);
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
   * @param splitPoints must be unique and sorted. Number of splitPoints +1 = counters.length.
   * @param counters array of counters
   */ //used by HeapQS and MQS
  static void linearTimeIncrementHistogramCounters(double[] samples, int offset, int numSamples, 
      long weight, double[] splitPoints, long[] counters) {
    int numSplitPoints = splitPoints.length;

    int i = 0;
    int j = 0;

    while (i < numSamples && j < numSplitPoints) {
      if (samples[i+offset] < splitPoints[j]) {
        counters[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      }
      else {
        j++; // no more samples for this bucket. move on the next bucket.
      }
    }

    // now either i == numSamples(we are out of samples), or
    // j == numSplitPoints(out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case.
    if (j == numSplitPoints) {
      counters[numSplitPoints] += (weight * (numSamples - i));
    }
  }
  
  //****************************************************
  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed above.
   * Used by Aux constructors for both Heap QS and MQS
   */ //used by Auxiliary, so far
  static void blockyTandemMergeSort(double[] keyArr, long[] valArr, int arrLen, int blkSize) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) return;
    int numblks = arrLen / blkSize;
    if (numblks * blkSize < arrLen) numblks += 1;
    assert (numblks * blkSize >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy. 
    double[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    long[] valTmp   = Arrays.copyOf(valArr, arrLen);

    blockyTandemMergeSortRecursion(keyTmp, valTmp,
                                   keyArr, valArr,
                                   0, numblks,
                                   blkSize, arrLen);

    /* verify sorted order */
    for (int i = 0; i < arrLen-1; i++) {
      assert keyArr[i] <= keyArr[i+1];
    }
  }

  /**
   *  blockyTandemMergeSortRecursion() is called by blockyTandemMergeSort().
   *  In addition to performing the algorithm's top down recursion,
   *  it manages the buffer swapping that eliminates most copying.
   *  It also maps the input's pre-sorted blocks into the subarrays 
   *  that are processed by tandemMerge().
   * @param keySrc key source
   * @param valSrc value source
   * @param keyDst key destination
   * @param valDst value destination
   * @param grpStart group start, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param grpLen group length, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param blkSize block size
   * @param arrLim array limit
   */
  private static void blockyTandemMergeSortRecursion(double[] keySrc, long[] valSrc,
      double[] keyDst, long[] valDst, int grpStart, int grpLen, /* indices of blocks */
      int blkSize, int arrLim) {
    // Important note: grpStart and grpLen do NOT refer to positions in the underlying array.
    // Instead, they refer to the pre-sorted blocks, such as block 0, block 1, etc.

    assert (grpLen > 0);
    if (grpLen == 1) return;
    int grpLen1 = grpLen / 2;
    int grpLen2 = grpLen - grpLen1;
    assert (grpLen1 >= 1);
    assert (grpLen2 >= grpLen1);

    int grpStart1 = grpStart;
    int grpStart2 = grpStart + grpLen1;

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart1, grpLen1, blkSize, arrLim);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart2, grpLen2, blkSize, arrLim);

    // here we convert indices of blocks into positions in the underlying array.
    int arrStart1 = grpStart1 * blkSize;
    int arrStart2 = grpStart2 * blkSize;
    int arrLen1   = grpLen1   * blkSize;
    int arrLen2   = grpLen2   * blkSize;

    // special code for the final block which might be shorter than blkSize.
    if (arrStart2 + arrLen2 > arrLim) { arrLen2 = arrLim - arrStart2; } 
 
    tandemMerge(keySrc, valSrc,
                arrStart1, arrLen1, 
                arrStart2, arrLen2,
                keyDst, valDst,
                arrStart1); // which will be arrStart3
  }
  
  /**
   *  Performs two merges in tandem. One of them provides the sort keys
   *  while the other one passively undergoes the same data motion.
   * @param keySrc key source
   * @param valSrc value source
   * @param arrStart1 Array 1 start offset
   * @param arrLen1 Array 1 length
   * @param arrStart2 Array 2 start offset
   * @param arrLen2 Array 2 length
   * @param keyDst key destination
   * @param valDst value destination
   * @param arrStart3 Array 3 start offset
   */
  private static void tandemMerge(double[] keySrc, long[] valSrc,
                                  int arrStart1, int arrLen1,
                                  int arrStart2, int arrLen2,
                                  double[] keyDst, long[] valDst,
                                  int arrStart3) {
    int arrStop1 = arrStart1 + arrLen1;
    int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;

    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc[i2] < keySrc[i1]) { 
        keyDst[i3] = keySrc[i2];
        valDst[i3] = valSrc[i2];
        i3++; i2++;
      }     
      else { 
        keyDst[i3] = keySrc[i1];
        valDst[i3] = valSrc[i1];
        i3++; i1++;
      } 
    }

    if (i1 < arrStop1) {
      arraycopy(keySrc, i1, keyDst, i3, arrStop1 - i1);
      arraycopy(valSrc, i1, valDst, i3, arrStop1 - i1);
    }
    else {
      assert i2 < arrStop2;
      arraycopy(keySrc, i2, keyDst, i3, arrStop2 - i2);
      arraycopy(valSrc, i2, valDst, i3, arrStop2 - i2);
    }
  }
  
  //************************************************************
  /**
   * Computes epsilon from K. The following table are examples.
   * <code><pre>
   *           eps      eps from inverted
   *     K   empirical  adjusted formula
   *  -------------------------------------
   *    16   0.121094   0.121454102233560
   *    32   0.063477   0.063586601346532
   *    64   0.033081   0.033169048393679
   *   128   0.017120   0.017248096847308
   *   256   0.008804   0.008944835012965
   *   512   0.004509   0.004627803568920
   *  1024   0.002303   0.002389303789572
   *
   *  these could be used in a unit test
   *  2   0.821714930853465
   *  16   0.12145410223356
   *  1024   0.00238930378957284
   *  1073741824   3.42875166500824e-09
   * </pre></code>
   */ //used by Heap QS
  static class EpsilonFromK {
    /**
     *  Used while crunching down the empirical results.  If this value is changed the adjustKForEps
     *  value will be incorrect and must also be recomputed.  Don't touch this!
     */
    private static final double deltaForEps = 0.01;  

    /**
     *  A heuristic fudge factor that causes the inverted formula to better match the empirical.
     *  The value of 4/3 is directly associated with the deltaForEps value of 0.01. Don't touch this!
     */
    private static final double adjustKForEps = 4.0 / 3.0;  // fudge factor

    /**
     *  Ridiculously fine tolerance given the fudge factor; 1e-3 would probably suffice
     */
    private static final double bracketedBinarySearchForEpsTol = 1e-15; 

    /**
     * From extensive empirical testing we recommend most users use this method for deriving 
     * epsilon. This uses a fudge factor of 4/3 times the theoretical calculation of epsilon.
     * @param k the given k that must be greater than one.
     * @return the resulting epsilon
     */ //used by HeapQS, so far
    static double getAdjustedEpsilon(int k) {
      return getTheoreticalEpsilon(k, adjustKForEps);
    }
    
    /**
     * Finds the epsilon given K and a fudge factor.
     * See Cormode's Mergeable Summaries paper, Journal version, Theorem 3.6. 
     * This has a good fit between values of k between 16 and 1024. 
     * Beyond that has not been empirically tested.
     * @param k The given value of k
     * @param ff The given fudge factor. No fudge factor = 1.0. 
     * @return the resulting epsilon
     */ //used only by getAdjustedEpsilon()
    private static double getTheoreticalEpsilon(int k, double ff) {
      if (k < 2) throw new IllegalArgumentException("K must be greater than one.");
      // don't need to check in the other direction because an int is very small
      double kf = k*ff;
      assert kf >= 2.15; // ensures that the bracketing succeeds
      assert kf < 1e12;  // ditto, but could actually be bigger
      double lo = 1e-16;
      double hi = 1.0 - 1e-16;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      return bracketedBinarySearchForEps(kf, lo, hi);
    }
    
    private static double kOfEpsFormula(double eps) {
      return (1.0 / eps) * (Math.sqrt(Math.log(1.0 / (eps * deltaForEps))));
    }

    private static boolean epsForKPredicate(double eps, double kf) {
      return kOfEpsFormula(eps) >= kf;
    }

    private static double bracketedBinarySearchForEps(double kf, double lo, double hi) {
      assert lo < hi;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      if ((hi - lo) / lo < bracketedBinarySearchForEpsTol) {
        return lo;
      }
      double mid = (lo + hi) / 2.0;
      assert mid > lo;
      assert mid < hi;
      if (epsForKPredicate(mid, kf)) {
        return bracketedBinarySearchForEps(kf, mid, hi);
      }
      else {
        return bracketedBinarySearchForEps(kf, lo, mid);
      }
    }
  } //End of EpsilonFromK

//  public static void main(String[] args) {
//    long v = 1;
//    for (int i=0; i<64; i++) {
//      long w = v << i;
//      long w2 = w -1;
//      System.out.println(i+"\t"+Long.toBinaryString(w2)+"\t"+hiBitPos(w2)+"\t"+w2);
//    }
//  }
  
}