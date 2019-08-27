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

import java.util.Random;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class AccuracyTest {
  static Random rand = new Random();

  @Test
  public void baseTest() {
    int n = 1 << 20;
    int k = 1 << 5;

    double[] seqArr = new double[n];
    //build sequential array
    for (int i = 1; i <= n; i++) {
      seqArr[i - 1] = i;
    }
    double[] randArr = seqArr.clone();
    shuffle(randArr);
    UpdateDoublesSketch sketch = DoublesSketch.builder().setK(k).build();
    for (int i = 0; i < n; i++) {
      sketch.update(randArr[i]);
    }
    double[] ranks = sketch.getCDF(seqArr);
    double maxDelta = 0;
    for (int i = 0; i < n; i++) {
      double actRank = (double)i/n;
      double estRank = ranks[i];
      double delta = actRank - estRank;
      maxDelta = Math.max(maxDelta, delta);
      //println("Act: " +  + " \tEst: " + ranks[i]);
    }
    println("Max delta: " + maxDelta);
    println(sketch.toString());

  }

  public static void shuffle(double[] arr) {
    int n = arr.length;
    for (int i = 0; i < n; i++) {
      int j = i + rand.nextInt(n - i);
      swap(arr, i, j);
    }
  }

  public static void swap(double[] arr, int i, int j) {
    double t = arr[i];
    arr[i] = arr[j];
    arr[j] = t;
  }


  //@Test
  public void getEpsilon() {
    for (int lgK = 4; lgK < 15; lgK++) {
      int k = 1 << lgK;
      double eps = Util.getNormalizedRankError(k, false);
      println(k + "\t" + eps);
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
