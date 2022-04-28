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

import static org.apache.datasketches.InequalitySearch.EQ;
import static org.apache.datasketches.InequalitySearch.GE;
import static org.apache.datasketches.InequalitySearch.GT;
import static org.apache.datasketches.InequalitySearch.LE;
import static org.apache.datasketches.InequalitySearch.LT;
import static org.testng.Assert.assertEquals;

import java.util.Random;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class BinarySearchTest {
  static Random rand = new Random(1);
  private static final String LS = System.getProperty("line.separator");

  private static int randDelta() { return rand.nextDouble() < 0.4 ? 0 : 1; }

  private static float[] buildRandFloatArr(final int len) {
    final float[] arr = new float[len];
    float v = 1.0f;
    for (int i = 0; i < len; i++) {
      arr[i] = v;
      v += randDelta();
    }
    return arr;
  }

  private static double[] buildRandDoubleArr(final int len) {
    final double[] arr = new double[len];
    double v = 1.0;
    for (int i = 0; i < len; i++) {
      arr[i] = v;
      v += randDelta();
    }
    return arr;
  }

  private static long[] buildRandLongArr(final int len) {
    final long[] arr = new long[len];
    long v = 1L;
    for (int i = 0; i < len; i++) {
      arr[i] = v;
      v += 2 * randDelta();
    }
    return arr;
  }

  //double array

  @Test
  public void checkBinSearchDblLimits() {
    for (int len = 10; len <= 13; len++) {
      final double[] tarr = buildRandDoubleArr(len);
      final int low = 2;
      final int high = len - 2;
      println(listDblArray(tarr, low, high));
      checkBinarySearchDoubleLimits(tarr, low, high);
    }
  }

  private static String listDblArray(final double[] arr, final int low, final int high) {
    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    final int len = arr.length;
    sb.append("double[" + len + "]: ");
    for (int i = 0; i < len; i++) {
      if (i == low || i == high) { sb.append(String.format("(%.0f) ", arr[i])); }
      else { sb.append(String.format("%.0f ", arr[i])); }
    }
    return sb.toString();
  }

  private static void checkBinarySearchDoubleLimits(final double[] arr, final int low, final int high) {
    final double lowV = arr[low];
    final double highV = arr[high];
    double v;
    int res;
    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(arr[res], v);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = lowV;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, -1);
  }

  //float array

  @Test
  public void checkBinSearchFltLimits() {
    for (int len = 10; len <= 13; len++) {
      final float[] tarr = buildRandFloatArr(len);
      final int low = 2;
      final int high = len - 2;
      println(listFltArray(tarr, low, high));
      checkBinarySearchFloatLimits(tarr, low, high);
    }
  }

  private static String listFltArray(final float[] arr, final int low, final int high) {
    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    final int len = arr.length;
    sb.append("float[" + len + "]: ");
    for (int i = 0; i < len; i++) {
      if (i == low || i == high) { sb.append(String.format("(%.0f) ", arr[i])); }
      else { sb.append(String.format("%.0f ", arr[i])); }
    }
    return sb.toString();
  }

  private static void checkBinarySearchFloatLimits(final float[] arr, final int low, final int high) {
    final float lowV = arr[low];
    final float highV = arr[high];
    float v;
    int res;
    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV -1;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(arr[res], v);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = lowV;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, -1);
  }

  //long array

  @Test
  public void checkBinSearchLongLimits() {
    for (int len = 10; len <= 13; len++) {
      final long[] tarr = buildRandLongArr(len);
      final int low = 2;
      final int high = len - 2;
      println(listLongArray(tarr, low, high));
      checkBinarySearchLongLimits(tarr, low, high);
    }
  }

  private static String listLongArray(final long[] arr, final int low, final int high) {
    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    final int len = arr.length;
    sb.append("long[" + len + "]: ");
    for (int i = 0; i < len; i++) {
      if (i == low || i == high) { sb.append(String.format("(%d) ", arr[i])); }
      else { sb.append(String.format("%d ", arr[i])); }
    }
    return sb.toString();
  }

  private static void checkBinarySearchLongLimits(final long[] arr, final int low, final int high) {
    final long lowV = arr[low];
    final long highV = arr[high];
    long v;
    int res;
    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, LT);
    println(LT.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV -1;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, LE);
    println(LE.desc(arr, low, high, v, res));
    assertEquals(res, high);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(arr[res], v);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, EQ);
    println(EQ.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, GT);
    println(GT.desc(arr, low, high, v, res));
    assertEquals(res, -1);

    v = lowV - 1;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = lowV;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, low);

    v = highV + 1;
    res = InequalitySearch.find(arr, low, high, v, GE);
    println(GE.desc(arr, low, high, v, res));
    assertEquals(res, -1);
  }

  /****************/

  @Test // visual only for doubles inequality
  public void exerciseDblBinSearch() {
    //                    0 1 2 3 4 5,6
    final double[] arr = {1,1,3,3,4,5,5};
    checkFindDouble(arr, LT);
    checkFindDouble(arr, LE);
    checkFindDouble(arr, GT);
    checkFindDouble(arr, GE);
  }

  private static void checkFindDouble(final double[] arr, final InequalitySearch crit) {
    println("InequalitySearch: " + crit.name());
    final int len = arr.length;
    for (double v = 0.5; v <= arr[len - 1] + 0.5; v += .5)
    //final double v = 0.5;
    {
      final int low = 0;
      final int high = len - 1;
      final int idx = InequalitySearch.find(arr, low, high, v, crit);
      if (idx == -1) {
        println(v + " Not resolved, return -1.");
      }
      else {
        println(crit.desc(arr, low, high, v, idx));
      }
    }
    println("");
  }

  @Test // visual only for floats inequality
  public void exerciseFltBinSearch() {
    //                   0 1 2 3 4 5 6 7 8 9
    final float[] arr = {5,5,5,6,6,6,7,8,8,8};
    checkFindFloat(arr, LT);
    checkFindFloat(arr, LE);
    checkFindFloat(arr, GT);
    checkFindFloat(arr, GE);
  }

  private static void checkFindFloat(final float[] arr, final InequalitySearch crit) {
    println("InequalitySearch: " + crit.name());
    final int len = arr.length;
    for (float v = 0.5f; v <= arr[len - 1] + 0.5f; v += 0.5f)
    //final double v = 0.5;
    {
      final int low = 0;
      final int high = len - 1;
      final int idx = InequalitySearch.find(arr, low, high, v, crit);
      if (idx == -1) {
        println("LT: " + v + " Not resolved, return -1.");
      }
      else {
        println(crit.desc(arr, low, high, v, idx));
      }
    }
    println("");
  }

  @Test // visual only for longs inequality
  public void exerciseLongBinSearch() {
    //                   0 1 2 3 4 5 6 7  8  9
    final long[] arr =  {5,5,5,7,7,7,9,11,11,11};
    checkFindLong(arr, LT);
    checkFindLong(arr, LE);
    checkFindLong(arr, GT);
    checkFindLong(arr, GE);
  }

  /****************/

  private static void checkFindLong(final long[] arr, final InequalitySearch crit) {
    println("InequalitySearch: " + crit.name());
    final int len = arr.length;
    for (long v = 1L; v <= arr[len - 1] + 1L; v++)
    {
      final int low = 0;
      final int high = len - 1;
      final int idx = InequalitySearch.find(arr, low, high, v, crit);
      if (idx == -1) {
        println("LT: " + v + " Not resolved, return -1.");
      }
      else {
        println(crit.desc(arr, low, high, v, idx));
      }
    }
    println("");
  }

  //test equality binary searches

  @Test
  public void checkSimpleFindFloat() {
    final int len = 10;
    final float[] arr = new float[len];
    for (int i = 0; i < len; i++) { arr[i] = i; }
    int idx;
    for (int i = 0; i < len; i++) {
      idx = BinarySearch.find(arr, 0, len - 1, i);
      assertEquals(idx, i);
    }
    idx = BinarySearch.find(arr, 0, len - 1, -1);
    assertEquals(idx, -1);
    idx = BinarySearch.find(arr, 0, len - 1, len);
    assertEquals(idx, -1);
  }

  @Test
  public void checkSimpleFindDouble() {
    final int len = 11;
    final double[] arr = new double[len];
    for (int i = 0; i < len; i++) { arr[i] = i; }
    int idx;
    for (int i = 0; i < len; i++) {
      idx = BinarySearch.find(arr, 0, len - 1, i);
      assertEquals(idx, i);
    }
    idx = BinarySearch.find(arr, 0, len - 1, -1);
    assertEquals(idx, -1);
    idx = BinarySearch.find(arr, 0, len - 1, len);
    assertEquals(idx, -1);
  }

  @Test
  public void checkSimpleFindLong() {
    final int len = 11;
    final long[] arr = new long[len];
    for (int i = 0; i < len; i++) { arr[i] = i; }
    int idx;
    for (int i = 0; i < len; i++) {
      idx = BinarySearch.find(arr, 0, len - 1, i);
      assertEquals(idx, i);
    }
    idx = BinarySearch.find(arr, 0, len - 1, -1);
    assertEquals(idx, -1);
    idx = BinarySearch.find(arr, 0, len - 1, len);
    assertEquals(idx, -1);
  }

  /****************/

  //@Test //visual testing only
  @SuppressWarnings("unused")
  private static void checkBuildRandFloatArr() {
    final int len = 10;
    for (int i = 0; i < 10; i++) {
      final float[] tarr = buildRandFloatArr(len);
      for (int j = 0; j < len; j++) {
        printf("%4.1f,", tarr[j]);
      }
      println("");
    }
  }

  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    //System.out.printf(format, args);
  }

  /**
   * @param o the Object to print
   */
  static final void println(final Object o) {
    //System.out.println(o.toString());
  }
}
