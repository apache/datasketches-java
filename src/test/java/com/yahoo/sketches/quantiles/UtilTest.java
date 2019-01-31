/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

public class UtilTest {

  @Test
  public void checkCombBufItemCapacity() {
    int k = 227;
    int capEl = Util.computeCombinedBufferItemCapacity(k, 0);
    assertEquals(capEl, 2 * DoublesSketch.MIN_K);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPreLongsFlagsCap() {
    Util.checkPreLongsFlagsCap(2, 0, 15);
  }

  @Test
  public void checkLg() {
    int lgbase2 = (int) Util.lg(4096);
    assertEquals(lgbase2, 12);
  }

  @Test
  public void checkHiBitPos() {
    int bitPos = Util.hiBitPos(4096);
    assertEquals(bitPos, 12);
  }

  @Test
  public void checkNumValidLevels() {
    long v = (1L << 32)-1L;
    int ones = Util.computeValidLevels(v);
    assertEquals(ones, 32);
  }

  @Test
  public void testPositionOfLowestZeroBitStartingAt() {
    int [] answers = {9, 8, 7, 7, 7, 4, 4, 4, 1, 1};
    long v = 109L;
    //println("IN: " + Long.toBinaryString(v));
    for (int i = 0, j = 9; i < 10; i++, j--) {
      int result = Util.lowestZeroBitStartingAt(v, i);
      //System.out.printf ("%d %d %d%n", i, result, answers[j]);
      assertTrue (answers[j] == result);
    }
  }

  @Test
  public void testPositionOfLowestZeroBitStartingAt2() {
    long bits = -1L;
    int startingBit = 70; //only low 6 bits are used
    int result = Util.lowestZeroBitStartingAt(bits, startingBit);
    assertEquals(result, 64);
  }

  // a couple of basic unit tests for the histogram construction helper functions.
  @Test
  public void testQuadraticTimeIncrementHistogramCounters () {
    final double[] samples = {0.1, 0.2, 0.3, 0.4, 0.5};
    final DoublesArrayAccessor accessor = DoublesArrayAccessor.wrap(samples);
    {
      final double[] splitPoints = {0.25, 0.4};
      final double[] counters = {0, 0, 0};
      final long[] answers = {200, 100, 200};
      DoublesPmfCdfImpl.bilinearTimeIncrementHistogramCounters(accessor, 100, splitPoints, counters);
      for (int j = 0; j < counters.length; j++) {
        assertEquals(counters[j], answers[j], 0.00001);
        // System.out.printf ("counter[%d] = %d%n", j, counters[j]);
      }
      // System.out.printf ("%n");
    }

    {
      final double[] splitPoints = {0.01, 0.02};
      final double[] counters = {0, 0, 0};
      final long[] answers = {0, 0, 500};
      DoublesPmfCdfImpl.bilinearTimeIncrementHistogramCounters(accessor, 100, splitPoints, counters);
      for (int j = 0; j < counters.length; j++) {
        assertEquals(counters[j], answers[j], 0.00001);
        // System.out.printf ("counter[%d] = %d%n", j, counters[j]);
      }
      // System.out.printf ("%n");
    }

    {
      final double[] splitPoints = {0.8, 0.9};
      final double[] counters = {0, 0, 0};
      final long[] answers = {500, 0, 0};
      DoublesPmfCdfImpl.bilinearTimeIncrementHistogramCounters(accessor, 100, splitPoints, counters);
      for (int j = 0; j < counters.length; j++) {
        assertEquals(counters[j], answers[j], 0.00001);
        // System.out.printf ("counter[%d] = %d%n", j, counters[j]);
      }
      // System.out.printf ("%n");
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkValidateValuesNullException() {
    Util.checkSplitPointsOrder(null);
  }

  @Test
  public void testLinearTimeIncrementHistogramCounters () {
    final double[] samples = {0.1, 0.2, 0.3, 0.4, 0.5};
    final DoublesArrayAccessor accessor = DoublesArrayAccessor.wrap(samples);
    {
      final double[] splitPoints = {0.25, 0.4};
      final double[] counters = {0, 0, 0};
      final long[] answers = {200, 100, 200};
      DoublesPmfCdfImpl.linearTimeIncrementHistogramCounters(accessor, 100, splitPoints, counters);
      for (int j = 0; j < counters.length; j++) {
        assertEquals(counters[j], answers[j], 0.00001);
        // System.out.printf ("counter[%d] = %d%n", j, counters[j]);
      }
      // System.out.printf ("%n");
    }

    {
      final double[] splitPoints = {0.01, 0.02};
      final double[] counters = {0, 0, 0};
      final long[] answers = {0, 0, 500};
      DoublesPmfCdfImpl.linearTimeIncrementHistogramCounters(accessor, 100, splitPoints, counters);
      for (int j = 0; j < counters.length; j++) {
        assertEquals(counters[j], answers[j], 0.00001);
        // System.out.printf ("counter[%d] = %d%n", j, counters[j]);
      }
      // System.out.printf ("%n");
    }

    {
      final double[] splitPoints = {0.8, 0.9};
      final double[] counters = {0, 0, 0};
      final long[] answers = {500, 0, 0};
      DoublesPmfCdfImpl.linearTimeIncrementHistogramCounters(accessor, 100, splitPoints, counters);
      for (int j = 0; j < counters.length; j++) {
        assertEquals(counters[j], answers[j], 0.00001);
        // System.out.printf ("counter[%d] = %d%n", j, counters[j]);
      }
      // System.out.printf ("%n");
    }
  }

//The remainder of this file is a brute force test of corner cases
 // for blockyTandemMergeSort.

 private static void assertMergeTestPrecondition(double [] arr, long [] brr, int arrLen, int blkSize) {
   int violationsCount = 0;
   for (int i = 0; i < (arrLen-1); i++) {
     if (((i+1) % blkSize) == 0) {
      continue;
    }
     if (arr[i] > arr[i+1]) { violationsCount++; }
   }

   for (int i = 0; i < arrLen; i++) {
     if (brr[i] != (long) (1e12 * (1.0 - arr[i]))) {
      violationsCount++;
    }
   }
   if (brr[arrLen] != 0) { violationsCount++; }

   assertEquals(violationsCount, 0);
 }

 private static void  assertMergeTestPostcondition(double [] arr, long [] brr, int arrLen) {
   int violationsCount = 0;
   for (int i = 0; i < (arrLen-1); i++) {
     if (arr[i] > arr[i+1]) { violationsCount++; }
   }

   for (int i = 0; i < arrLen; i++) {
     if (brr[i] != (long) (1e12 * (1.0 - arr[i]))) {
      violationsCount++;
    }
   }
   if (brr[arrLen] != 0) { violationsCount++; }

   assertEquals(violationsCount, 0);
 }


 private static double[] makeMergeTestInput(int arrLen, int blkSize) {
   double[] arr = new double[arrLen];

   double pick = Math.random ();

   for (int i = 0; i < arrLen; i++) {
     if (pick < 0.01) { // every value the same
       arr[i] = 0.3;
     }
     else if (pick < 0.02) { // ascending values
       int j = i+1;
       int denom = arrLen+1;
       arr[i] = ((double) j) / ((double) denom);
     }
     else if (pick < 0.03) { // descending values
       int j = i+1;
       int denom = arrLen+1;
       arr[i] = 1.0 - (((double) j) / ((double) denom));
     }
     else { // random values
       arr[i] = Math.random ();
     }
   }

   for (int start = 0; start < arrLen; start += blkSize) {
     Arrays.sort (arr, start, Math.min (arrLen, start + blkSize));
   }

   return arr;
 }

 private static long [] makeTheTandemArray(double [] arr) {
   long [] brr = new long [arr.length + 1];  /* make it one longer, just like in the sketches */
   for (int i = 0; i < arr.length; i++) {
     brr[i] = (long) (1e12 * (1.0 - arr[i])); /* it's a better test with the order reversed */
   }
   brr[arr.length] = 0;
   return brr;
 }

 @Test
 public void checkBlockyTandemMergeSort() {
   testBlockyTandemMergeSort(10, 50);
 }

 /**
  *
  * @param numTries number of tries
  * @param maxArrLen maximum length of array size
  */
 private static void testBlockyTandemMergeSort(int numTries, int maxArrLen) {
   int arrLen = 0;
   double[] arr = null;
   for (arrLen = 0; arrLen <= maxArrLen; arrLen++) {
     for (int blkSize = 1; blkSize <= (arrLen + 100); blkSize++) {
       for (int tryno = 1; tryno <= numTries; tryno++) {
         arr = makeMergeTestInput(arrLen, blkSize);
         long [] brr = makeTheTandemArray(arr);
         assertMergeTestPrecondition(arr, brr, arrLen, blkSize);
         DoublesAuxiliary.blockyTandemMergeSort(arr, brr, arrLen, blkSize);
         /* verify sorted order */
         for (int i = 0; i < (arrLen-1); i++) {
           assert arr[i] <= arr[i+1];
         }
         assertMergeTestPostcondition(arr, brr, arrLen);
       }
     }
   }
   //System.out.printf ("Passed: testBlockyTandemMergeSort%n");
 }

// we are retaining this stand-alone test because it can be more exhaustive

//  @SuppressWarnings("unused")
//  private static void exhaustiveMain(String[] args) {
//    assert (args.length == 2);
//    int  numTries = Integer.parseInt(args[0]);
//    int maxArrLen = Integer.parseInt(args[1]);
//    System.out.printf("Testing blockyTandemMergeSort%n");
//    for (int arrLen = 0; arrLen < maxArrLen; arrLen++) {
//      for (int blkSize = 1; blkSize <= arrLen + 100; blkSize++) {
//        System.out.printf (
//            "Testing %d times with arrLen = %d and blkSize = %d%n", numTries, arrLen, blkSize);
//        for (int tryno = 1; tryno <= numTries; tryno++) {
//          double[] arr = makeMergeTestInput(arrLen, blkSize);
//          long[] brr = makeTheTandemArray(arr);
//          assertMergeTestPrecondition(arr, brr, arrLen, blkSize);
//          DoublesAuxiliary.blockyTandemMergeSort(arr, brr, arrLen, blkSize);
//          assertMergeTestPostcondition(arr, brr, arrLen);
//        }
//      }
//    }
//  }
//
//  public static void main(String[] args) {
//    exhaustiveMain(new String[] {"10", "100"});
//  }

 @Test
 public void checkKomologorovSmirnovStatistic1() {
   final int k = 256;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + 100);
     s2.update(x);
   }

   assertEquals(Util.computeKSDelta(s1, s2), 1.0, 1.0 + 1E-6);
   //println("D = " + Util.computeKSDelta(s1, s2));
 }

 @Test
 public void checkKomologorovSmirnovStatistic2() {
   final int k = 256;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x);
     s2.update(x);
   }

   assertEquals(Util.computeKSDelta(s1, s2), 0, .01);
   //println("D = " + Util.computeKSDelta(s1, s2));
 }

 @Test
 public void checkKomologorovSmirnovStatistic3() {
   final int k = 2048;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();
   final double tgtPvalue = .05;

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = Util.computeKSDelta(s1, s2);
   double thresh = Util.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = Util.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject);
   assertFalse(reject);
 }

 @Test
 public void checkKomologorovSmirnovStatistic4() {
   final int k = 8192;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();
   final double tgtPvalue = .05;

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = Util.computeKSDelta(s1, s2);
   double thresh = Util.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = Util.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject);
   assertTrue(reject);
 }


  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
