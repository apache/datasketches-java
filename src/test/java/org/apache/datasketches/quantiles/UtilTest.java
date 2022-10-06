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

package org.apache.datasketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import org.testng.annotations.Test;

import org.apache.datasketches.SketchesArgumentException;

public class UtilTest {

  @Test
  public void checkCombBufItemCapacity() {
    int k = 227;
    int capEl = ClassicUtil.computeCombinedBufferItemCapacity(k, 0);
    assertEquals(capEl, 2 * DoublesSketch.MIN_K);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPreLongsFlagsCap() {
    ClassicUtil.checkPreLongsFlagsCap(2, 0, 15);
  }

  @Test
  public void checkHiBitPos() {
    int bitPos = ClassicUtil.hiBitPos(4096);
    assertEquals(bitPos, 12);
  }

  @Test
  public void checkNumValidLevels() {
    long v = (1L << 32)-1L;
    int ones = ClassicUtil.computeValidLevels(v);
    assertEquals(ones, 32);
  }

  @Test
  public void testPositionOfLowestZeroBitStartingAt() {
    int [] answers = {9, 8, 7, 7, 7, 4, 4, 4, 1, 1};
    long v = 109L;
    //println("IN: " + Long.toBinaryString(v));
    for (int i = 0, j = 9; i < 10; i++, j--) {
      int result = ClassicUtil.lowestZeroBitStartingAt(v, i);
      //System.out.printf ("%d %d %d%n", i, result, answers[j]);
      assertTrue (answers[j] == result);
    }
  }

  @Test
  public void testPositionOfLowestZeroBitStartingAt2() {
    long bits = -1L;
    int startingBit = 70; //only low 6 bits are used
    int result = ClassicUtil.lowestZeroBitStartingAt(bits, startingBit);
    assertEquals(result, 64);
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
         DoublesSketchSortedView.blockyTandemMergeSort(arr, brr, arrLen, blkSize);
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
