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

import static org.apache.datasketches.req.ReqHelper.binarySearch;
import static org.testng.Assert.assertEquals;

import java.util.Random;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ReqHelperTest {
  static Random rand = new Random(1);

  static int randDelta() { return (rand.nextDouble() < 0.4) ? 0 : 1; }

  static float[] buildRandArr(int len) {
    float[] arr = new float[len];
    float v = 1.0f;
    for (int i = 0; i < len; i++) {
      arr[i] = v;
      v += randDelta();
    }
    return arr;
  }


  //@Test //visual testing only
  public void checkBuildRandArr() {
    int len = 10;
    for (int i = 0; i < 10; i++) {
      float[] tarr = buildRandArr(len);
      for (int j = 0; j < len; j++) {
        printf("%4.1f,", tarr[j]);
      }
      println("");
    }
  }

  @Test
  public void checkBinSearchLT() {
    for (int len = 10; len < 13; len++) {
      float[] rarr = buildRandArr(len);
      float top = rarr[len - 1] + .5f;

      for (float v = 0.5f; v <= top; v += 0.5f) {
        int idx1 = linearSearch(rarr, 0, len-1, v, false);
        int idx2 = binarySearch(rarr, 0, len-1, v, false);
        assertEquals(idx1, idx2);
      }
    }
  }

  @Test
  public void checkBinSearchLTEQ() {
    for (int len = 10; len < 13; len++) {
      float[] rarr = buildRandArr(len);
      float top = rarr[len - 1] + .5f;

      for (float v = 0.5f; v <= top; v += 0.5f) {
        int idx1 = linearSearch(rarr, 0, len-1, v, true);
        int idx2 = binarySearch(rarr, 0, len-1, v, true);
        assertEquals(idx1, idx2);
      }
    }
  }

  //@Test //visual checking only
  public void checkLinearSearch() {
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

  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }
}
