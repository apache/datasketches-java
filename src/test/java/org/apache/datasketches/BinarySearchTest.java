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

package org.apache.datasketches;

import static org.apache.datasketches.BinarySearch.binarySearchDouble;
import static org.apache.datasketches.BinarySearch.binarySearchFloat;
import static org.apache.datasketches.Criteria.EQ;
import static org.apache.datasketches.Criteria.GE;
import static org.apache.datasketches.Criteria.GT;
import static org.apache.datasketches.Criteria.LE;
import static org.apache.datasketches.Criteria.LT;
import static org.testng.Assert.assertEquals;

import java.util.Random;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class BinarySearchTest {
  static Random rand = new Random(1);
  private static final String LS = System.getProperty("line.separator");

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

  private static int randDelta() { return rand.nextDouble() < 0.4 ? 0 : 1; }

  @Test //visual testing only
  //@SuppressWarnings("unused")
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

  @Test
  public void checkBinSearchDblLimits() {
    for (int len = 10; len <= 13; len++) {
      double[] tarr = buildRandDoubleArr(len);
      int low = 2;
      int high = len - 2;
      println(listDblArray(tarr, low, high));
      checkBinarySearchDoubleLimits(tarr, low, high);
    }
  }

  private static String listDblArray(double[] arr, int low, int high) {
    StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("arr: ");
    for (int i = 0; i < arr.length; i++) {
      if (i == low || i == high) { sb.append(String.format("(%.0f) ", arr[i])); }
      else { sb.append(String.format("%.0f ", arr[i])); }
    }
    return sb.toString();
  }

  private static void checkBinarySearchDoubleLimits(double[] arr, int low, int high) {
    double lowV = arr[low];
    double highV = arr[high];
    double v;
    int res;
    v = lowV - 1;
    res = binarySearchDouble(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV;
    res = binarySearchDouble(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = binarySearchDouble(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV - 1;
    res = binarySearchDouble(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = binarySearchDouble(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = highV + 1;
    res = binarySearchDouble(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV - 1;
    res = binarySearchDouble(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = binarySearchDouble(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(arr[res], v);

    v = highV + 1;
    res = binarySearchDouble(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = binarySearchDouble(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV;
    res = binarySearchDouble(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = binarySearchDouble(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = binarySearchDouble(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = lowV;
    res = binarySearchDouble(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV + 1;
    res = binarySearchDouble(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, -1);
  }

  @Test
  public void checkBinSearchFltLimits() {
    for (int len = 10; len <= 13; len++) {
      float[] tarr = buildRandFloatArr(len);
      int low = 2;
      int high = len - 2;
      println(listFltArray(tarr, low, high));
      checkBinarySearchFloatLimits(tarr, low, high);
    }
  }

  private static String listFltArray(float[] arr, int low, int high) {
    StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("arr: ");
    for (int i = 0; i < arr.length; i++) {
      if (i == low || i == high) { sb.append(String.format("(%.0f) ", arr[i])); }
      else { sb.append(String.format("%.0f ", arr[i])); }
    }
    return sb.toString();
  }

  private static void checkBinarySearchFloatLimits(float[] arr, int low, int high) {
    float lowV = arr[low];
    float highV = arr[high];
    float v;
    int res;
    v = lowV - 1;
    res = binarySearchFloat(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV;
    res = binarySearchFloat(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = binarySearchFloat(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV -1;
    res = binarySearchFloat(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = binarySearchFloat(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = highV + 1;
    res = binarySearchFloat(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV - 1;
    res = binarySearchFloat(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = binarySearchFloat(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(arr[res], v);

    v = highV + 1;
    res = binarySearchFloat(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = binarySearchFloat(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV;
    res = binarySearchFloat(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = binarySearchFloat(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = binarySearchFloat(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = lowV;
    res = binarySearchFloat(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV + 1;
    res = binarySearchFloat(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, -1);
  }

  @Test // visual only
  public void exerciseDblBinSearch() {
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
    for (double v = 0.5; v <= arr[len - 1] + 0.5; v += .5)
    //final double v = 0.5;
    {
      final int low = 0;
      final int high = len - 1;
      final int idx = binarySearchDouble(arr, low, high, v, crit);
      if (idx == -1) {
        println(v + " Not resolved, return -1.");
      }
      else {
        println(crit.desc(arr, low, high, v, idx));
      }
    }
    println("");
  }

  @Test // visual only
  public void exerciseFltBinSearch() {
    //                    0 1 2 3 4 5,6
    final float[] arr = {1,1,3,3,4,5,5};
    checkBinarySearchFloat(arr, LT);
    checkBinarySearchFloat(arr, LE);
    checkBinarySearchFloat(arr, GT);
    checkBinarySearchFloat(arr, GE);
  }

  private static void checkBinarySearchFloat(float[] arr, Criteria crit) {
    println("Criteria: " + crit.name());
    final int len = arr.length;
    for (float v = 0.5f; v <= arr[len - 1] + 0.5f; v += .5f)
    //final double v = 0.5;
    {
      final int low = 0;
      final int high = len - 1;
      final int idx = binarySearchFloat(arr, low, high, v, crit);
      if (idx == -1) {
        println(v + " Not resolved, return -1.");
      }
      else {
        println(crit.desc(arr, low, high, v, idx));
      }
    }
    println("");
  }


  @SuppressWarnings("unused")
  static final void printf(final String format, final Object ...args) {
    //System.out.printf(format, args);
  }

  @SuppressWarnings("unused")
  static final void println(final Object o) {
    //System.out.println(o.toString());
  }
}
