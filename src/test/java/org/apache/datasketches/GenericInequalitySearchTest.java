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

import static org.apache.datasketches.GenericInequalitySearch.find;
import static org.apache.datasketches.GenericInequalitySearch.Inequality.EQ;
import static org.apache.datasketches.GenericInequalitySearch.Inequality.GE;
import static org.apache.datasketches.GenericInequalitySearch.Inequality.GT;
import static org.apache.datasketches.GenericInequalitySearch.Inequality.LE;
import static org.apache.datasketches.GenericInequalitySearch.Inequality.LT;
import static org.testng.Assert.assertEquals;

import java.util.Comparator;
import java.util.Random;

import org.apache.datasketches.GenericInequalitySearch.Inequality;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class GenericInequalitySearchTest {
  static Random rand = new Random(1);
  private static final String LS = System.getProperty("line.separator");
  private static int randDelta() { return rand.nextDouble() < 0.4 ? 0 : 1; }

  private final Comparator<Float> comparator = Comparator.naturalOrder();


  private static Float[] buildRandFloatArr(final int len) {
    final Float[] arr = new Float[len];
    Float v = 1.0f; //lgtm [java/non-null-boxed-variable]
    for (int i = 0; i < len; i++) {
      arr[i] = v;
      v += randDelta();
    }
    return arr;
  }

  @Test //visual testing only
  //@SuppressWarnings("unused")
  private static void checkBuildRandArr() {
    final int len = 10;
    for (int i = 0; i < 10; i++) {
      final Float[] tarr = buildRandFloatArr(len);
      for (int j = 0; j < len; j++) {
        printf("%4.1f,", tarr[j]);
      }
      println("");
    }
  }

  private static String listFltArray(final Float[] arr, final int low, final int high) {
    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("arr: ");
    for (int i = 0; i < arr.length; i++) {
      if (i == low || i == high) { sb.append(String.format("(%.0f) ", arr[i])); }
      else { sb.append(String.format("%.0f ", arr[i])); }
    }
    return sb.toString();
  }

  @Test
  public void checkBinSearchFltLimits() {
    for (int len = 10; len <= 13; len++) {
      final Float[] tarr = buildRandFloatArr(len);
      final int low = 2;
      final int high = len - 2;
      println(listFltArray(tarr, low, high));
      checkBinarySearchFloatLimits(tarr, low, high);
    }
  }

  private void checkBinarySearchFloatLimits(final Float[] arr, final int low, final int high) {
    final Float lowV = arr[low];
    final Float highV = arr[high];
    Float v;
    int res;
    v = lowV - 1f;
    res = find(arr, low, high, v, LT, comparator);
    println(desc(arr, low, high, v, res, LT, comparator));
    assertEquals(res, -1);

    v = lowV;
    res = find(arr, low, high, v, LT, comparator);
    println(desc(arr, low, high, v, res, LT, comparator));
    assertEquals(res, -1);

    v = highV + 1;
    res = find(arr, low, high, v, LT, comparator);
    println(desc(arr, low, high, v, res, LT, comparator));
    assertEquals(res, high);

    v = lowV -1;
    res = find(arr, low, high, v, LE, comparator);
    println(desc(arr, low, high, v, res, LE, comparator));
    assertEquals(res, -1);

    v = highV;
    res = find(arr, low, high, v, LE, comparator);
    println(desc(arr, low, high, v, res, LE, comparator));
    assertEquals(res, high);

    v = highV + 1;
    res = find(arr, low, high, v, LE, comparator);
    println(desc(arr, low, high, v, res, LE, comparator));
    assertEquals(res, high);

    v = lowV - 1;
    res = find(arr, low, high, v, EQ, comparator);
    println(desc(arr, low, high, v, res, EQ, comparator));
    assertEquals(res, -1);

    v = highV;
    res = find(arr, low, high, v, EQ, comparator);
    println(desc(arr, low, high, v, res, EQ, comparator));
    assertEquals(arr[res], v);

    v = highV + 1;
    res = find(arr, low, high, v, EQ, comparator);
    println(desc(arr, low, high, v, res, EQ, comparator));
    assertEquals(res, -1);

    v = lowV - 1;
    res = find(arr, low, high, v, GT, comparator);
    println(desc(arr, low, high, v, res, GT, comparator));
    assertEquals(res, low);

    v = highV;
    res = find(arr, low, high, v, GT, comparator);
    println(desc(arr, low, high, v, res, GT, comparator));
    assertEquals(res, -1);

    v = highV + 1;
    res = find(arr, low, high, v, GT, comparator);
    println(desc(arr, low, high, v, res, GT, comparator));
    assertEquals(res, -1);

    v = lowV - 1;
    res = find(arr, low, high, v, GE, comparator);
    println(desc(arr, low, high, v, res, GE, comparator));
    assertEquals(res, low);

    v = lowV;
    res = find(arr, low, high, v, GE, comparator);
    println(desc(arr, low, high, v, res, GE, comparator));
    assertEquals(res, low);

    v = highV + 1;
    res = find(arr, low, high, v, GE, comparator);
    println(desc(arr, low, high, v, res, GE, comparator));
    assertEquals(res, -1);
  }



  @Test // visual only
  public void exerciseFltBinSearch() {
    checkFindFloat(LT);
    checkFindFloat(LE);
    checkFindFloat(GE);
    checkFindFloat(GT);
  }

  private void checkFindFloat(final GenericInequalitySearch.Inequality inequality) {
    final String ie = inequality.name();
    println("Inequality: " + ie);
    //                   0  1  2  3  4  5  6  7  8  9
    final Float[] arr = {5f,5f,5f,6f,6f,6f,7f,8f,8f,8f};
    final int len = arr.length;
    print("Index:    ");
    for (int i = 0; i < len; i++) { printf("%d,   ", i); }
    print(LS + "Value:  ");
    for (int i = 0; i < len; i++) { printf("%.1f, ", arr[i]); }
    println("");
    for (float v = 0.5f; v <= arr[len - 1] + 0.5f; v += .5f) {
      final int low = 0;
      final int high = len - 1;
      final int idx = find(arr, low, high, v, inequality, comparator);
      if (idx == -1) {
        println(ie +": " + v + " Not resolved, return -1.");
      }
      else {
        println(desc(arr, low, high, v, idx, inequality, comparator));
      }
    }
    println("");
  }

  /**
   * Optional call that describes the details of the results of the search.
   * Used primarily for debugging.
   * @param arr The underlying sorted array of generic values
   * @param low the low index of the range
   * @param high the high index of the range
   * @param v the generic value to search for
   * @param idx the resolved index from the search
   * @param inequality one of LT, LE, EQ, GE, GT.
   * @param comparator for the type T
   * @param <T> The generic type of value to be used in the search process.
   * @return the descriptive string.
   */
  public static <T> String desc(final T[] arr, final int low, final int high, final T v, final int idx,
      final Inequality inequality, final Comparator<T> comparator) {
    switch (inequality) {
      case LT: {
        if (idx == -1) {
          return "LT: " + v + " <= arr[" + low + "]=" + arr[low] + "; return -1";
        }
        if (idx == high) {
          return "LT: " + v + " > arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
        } //idx < high
        return "LT: " + v
          + ": arr[" + idx + "]=" + arr[idx] + " < " + v
          + " <= arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
      }
      case LE: {
        if (idx == -1) {
          return "LE: " + v + " < arr[" + low + "]=" + arr[low] + "; return -1";
        }
        if (idx == high) {
          return "LE: " + v + " >= arr[" + high + "]=" + arr[high]
            + "; return arr[" + high + "]=" + arr[high];
        }
        return "LE: " + v
          + ": arr[" + idx + "]=" + arr[idx] + " <= " + v
          + " < arr[" + (idx + 1) + "]=" + arr[idx + 1]
          + "; return arr[" + idx + "]=" + arr[idx];
      }
      case EQ: {
        if (idx == -1) {
          if (comparator.compare(v, arr[high]) > 0) {
            return "EQ: " + v + " > arr[" + high + "]; return -1";
          }
          if (comparator.compare(v, arr[low]) < 0) {
            return "EQ: " + v + " < arr[" + low + "]; return -1";
          }
          return "EQ: " + v + " Cannot be found within arr[" + low + "], arr[" + high + "]; return -1";
        }
        return "EQ: " + v + " == arr[" + idx + "]; return " + idx;
      }
      case GE: {
        if (idx == -1) {
          return "GE: " + v + " > arr[" + high + "]=" + arr[high] + "; return -1";
        }
        if (idx == low) {
          return "GE: " + v + " <= arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
        } //idx > low
        return "GE: " + v
          + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " < " + v + " <= arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
      }
      case GT: {
        if (idx == -1) {
          return "GT: " + v + " >= arr[" + high + "]=" + arr[high] + "; return -1";
        }
        if (idx == low) {
          return "GT: " + v + " < arr[" + low + "]=" + arr[low]
            + "; return arr[" + low + "]=" + arr[low];
        } //idx > low
        return "GT: " + v
          + ": arr[" + (idx - 1) + "]=" + arr[idx - 1] + " <= " + v + " < arr[" + idx + "]=" + arr[idx]
          + "; return arr[" + idx + "]=" + arr[idx];
      }
    }
    return "";
  }


  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    //System.out.printf(format, args);
  }

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    //System.out.println(o.toString());
  }

  /**
   * @param o the Object to print
   */
  static final void print(final Object o) {
    //System.out.print(o.toString());
  }

}
