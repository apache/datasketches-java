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

import static org.apache.datasketches.req.Criteria.GE;
import static org.apache.datasketches.req.Criteria.GT;
import static org.apache.datasketches.req.Criteria.LE;
import static org.apache.datasketches.req.Criteria.LT;
import static org.apache.datasketches.req.ReqHelper.binarySearch;
import static org.apache.datasketches.req.ReqHelper.binarySearchDouble;
import static org.apache.datasketches.req.ReqHelper.binarySearchFloat;
import static org.apache.datasketches.req.ReqHelper.validateSplits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Random;

import org.apache.datasketches.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ReqHelperTest {
  static Random rand = new Random(1);

  @Test
  public void checkBinSearchLTandLTEQ() {
    for (int len = 10; len < 13; len++) {
      float[] rarr = buildRandFloatArr(len);
      float top = rarr[len - 1] + .5f;

      for (float v = 0.5f; v <= top; v += 0.5f) {
        int idx1 = linearSearch(rarr, 0, len-1, v, false);
        int idx2 = binarySearch(rarr, 0, len-1, v, false);
        assertEquals(idx1, idx2);
      }
      for (float v = 0.5f; v <= top; v += 0.5f) {
        int idx1 = linearSearch(rarr, 0, len-1, v, true);
        int idx2 = binarySearch(rarr, 0, len-1, v, true);
        assertEquals(idx1, idx2);
      }
    }
  }

  @Test
  public void checkBinSearch() {
    float[] arr = new float[] { 2,2,2,2,2,2,2,2,2 };
    int len = arr.length;
    float top = arr[len - 1] + .5f;
    for (float v = 0.5f; v <= top; v += 0.5f) {
      int idx1 = linearSearch(arr, 0, len-1, v, true);
      int idx2 = binarySearch(arr, 0, len-1, v, true);
      assertEquals(idx1, idx2);
    }
    for (float v = 0.5f; v <= top; v += 0.5f) {
      int idx1 = linearSearch(arr, 0, len-1, v, false);
      int idx2 = binarySearch(arr, 0, len-1, v, false);
      assertEquals(idx1, idx2);
    }
  }

  @Test
  public void checkValidateSplits() {
    float[] arr = {1,2,3,4,5};
    validateSplits(arr);
    try {
      float[] arr1 = {1,2,4,3,5};
      validateSplits(arr1);
      fail();
    }
    catch (final SketchesArgumentException e) { }
  }

  //@Test //visual checking only
  @SuppressWarnings("unused")
  private static void checkLinearSearch() {
    // index        0  1  2  3, 4
    float[] arr1 = {1, 2, 2, 2, 3};
    boolean lteq = true;
    int len = arr1.length;
    for (int i = 0; i < len; i++) { print(arr1[i] + ", "); }
    println("\n LTEQ: " + lteq);
    float v;
    printf("%10s, %10s, %10s\n", "Value","Index", "Rank");
    for (v = .5f; v <= 3.5f; v += .5f) {
      int idx = linearSearch(arr1, 0, len - 1, v, lteq);
      double r = 0;
      if (idx == -1) { r = 0; } else { r = (double) (idx + 1) / len; }
      printf("%10.2f, %10d, %10.2f\n", v, idx, r);
    }
  }

  //This is a brute force search for comparison testing
  private static int linearSearch(final float[] arr, final int low, final int high,
      final float value, final boolean lteq) {
    if (lteq) {
      if (value >= arr[high]) { return high; } // mass == 1.0
      int i;
      for (i = high + 1; i-- > low; ) {
        if (value >= arr[i]) { return i; }
      }
      return -1; //when value <= arr[low].  Cannot return an index < low
    }
    else { //LT
      if (value <= arr[low]) { return -1; } //mass == 0, Cannot return an index < low
      for (int i = low + 1; i <= high; i++) {
        if (value <=  arr[i]) { return i - 1; }
      }
      return high; //when value is > arr[high]
    }
  }


  private static float[] buildRandFloatArr(int len) {
    float[] arr = new float[len];
    float v = 1.0f;
    for (int i = 0; i < len; i++) {
      arr[i] = v;
      v += randDelta();
    }
    return arr;
  }

  private static double[] buildRandDoubleArr(int len) {
    double[] arr = new double[len];
    double v = 1.0;
    for (int i = 0; i < len; i++) {
      arr[i] = v;
      v += randDelta();
    }
    return arr;
  }


  private static int randDelta() { return (rand.nextDouble() < 0.4) ? 0 : 1; }

  //@Test //visual testing only
  @SuppressWarnings("unused")
  private static void checkBuildRandArr() {
    int len = 10;
    for (int i = 0; i < 10; i++) {
      float[] tarr = buildRandFloatArr(len);
      for (int j = 0; j < len; j++) {
        printf("%4.1f,", tarr[j]);
      }
      println("");
    }
  }

  static Criteria critLT = LT;
  static Criteria critLE = LE;
  static Criteria critGT = GT;
  static Criteria critGE = GE;

  @Test
  public void checkBinSearchDblLimits() {
    for (int len = 10; len <= 13; len++) {
      double[] tarr = buildRandDoubleArr(len);
      int low = 2;
      int high = len - 2;
      checkBinarySearchDoubleLimits(tarr, low, high);
    }
  }

  private static void checkBinarySearchDoubleLimits(double[] arr, int low, int high) {
    double lowV = arr[low];
    double highV = arr[high];
    int res;
    res = binarySearchDouble(arr, low, high, lowV - 1, LT);
    assertEquals(res, -1);
    res = binarySearchDouble(arr, low, high, lowV, LT);
    assertEquals(res, -1);
    res = binarySearchDouble(arr, low, high, highV + 1, LT);
    assertEquals(res, high);

    res = binarySearchDouble(arr, low, high, lowV - 1, LE);
    assertEquals(res, -1);
    res = binarySearchDouble(arr, low, high, highV, LE);
    assertEquals(res, high);
    res = binarySearchDouble(arr, low, high, highV + 1, LE);
    assertEquals(res, high);

    res = binarySearchDouble(arr, low, high, lowV - 1, GT);
    assertEquals(res, low);
    res = binarySearchDouble(arr, low, high, highV, GT);
    assertEquals(res, -1);
    res = binarySearchDouble(arr, low, high, highV + 1, GT);
    assertEquals(res, -1);

    res = binarySearchDouble(arr, low, high, lowV - 1, GE);
    assertEquals(res, low);
    res = binarySearchDouble(arr, low, high, lowV, GE);
    assertEquals(res, low);
    res = binarySearchDouble(arr, low, high, highV + 1, GE);
    assertEquals(res, -1);
  }

  @Test
  public void checkBinSearchFltLimits() {
    for (int len = 10; len <= 13; len++) {
      float[] tarr = buildRandFloatArr(len);
      int low = 2;
      int high = len - 2;
      checkBinarySearchFloatLimits(tarr, low, high);
    }
  }

  private static void checkBinarySearchFloatLimits(float[] arr, int low, int high) {
    float lowV = arr[low];
    float highV = arr[high];
    int res;
    res = binarySearchFloat(arr, low, high, lowV - 1, LT);
    assertEquals(res, -1);
    res = binarySearchFloat(arr, low, high, lowV, LT);
    assertEquals(res, -1);
    res = binarySearchFloat(arr, low, high, highV + 1, LT);
    assertEquals(res, high);

    res = binarySearchFloat(arr, low, high, lowV - 1, LE);
    assertEquals(res, -1);
    res = binarySearchFloat(arr, low, high, highV, LE);
    assertEquals(res, high);
    res = binarySearchFloat(arr, low, high, highV + 1, LE);
    assertEquals(res, high);

    res = binarySearchFloat(arr, low, high, lowV - 1, GT);
    assertEquals(res, low);
    res = binarySearchFloat(arr, low, high, highV, GT);
    assertEquals(res, -1);
    res = binarySearchFloat(arr, low, high, highV + 1, GT);
    assertEquals(res, -1);

    res = binarySearchFloat(arr, low, high, lowV - 1, GE);
    assertEquals(res, low);
    res = binarySearchFloat(arr, low, high, lowV, GE);
    assertEquals(res, low);
    res = binarySearchFloat(arr, low, high, highV + 1, GE);
    assertEquals(res, -1);
  }

  //@Test visual only
  public void exerciseBinSearch2() {
    //                    0 1 2 3 4 5,6
    final double[] arr = {1,1,3,3,4,5,5};
    checkBinarySearchDouble(arr, LT);
    checkBinarySearchDouble(arr, LE);
    checkBinarySearchDouble(arr, GT);
    checkBinarySearchDouble(arr, GE);
  }

  private static void checkBinarySearchDouble(double[] arr, Criteria crit) {
    println("Criteria: " + crit.name());
    final int len = arr.length;
    for (double v = 0.5; v <= (arr[len - 1] + 0.5); v += .5)
    //final double v = 0.5;
    {
      final int low = 0;
      final int high = len - 1;
      final int idx = binarySearchDouble(arr, low, high, v, crit);
      if (idx == -1) {
        println(v + " Not resolved, return -1.");
      }
      else {
        println(crit.desc(arr, low, high, idx, v));
      }
    }
    println("");
  }


  private static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  private static final void print(final Object o) { System.out.print(o.toString()); }

  @SuppressWarnings("unused")
  private static final void println(final Object o) {
    System.out.println(o.toString());
  }
}
