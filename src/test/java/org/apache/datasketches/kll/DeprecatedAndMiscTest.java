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

package org.apache.datasketches.kll;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class DeprecatedAndMiscTest {

  @SuppressWarnings("deprecation")
  @Test
  public void checkDeprecatedRankError() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    int k = KllFloatsSketch.DEFAULT_K;
    double eps1 = sketch.getNormalizedRankError();
    double eps2 = KllFloatsSketch.getNormalizedRankError(k);
    double expectEps = KllFloatsSketch.getNormalizedRankError(k, true);
    assertEquals(eps1, expectEps);
    assertEquals(eps2, expectEps);
  }

  @Test
  public void checkGetKFromEps() {
    int k = KllFloatsSketch.DEFAULT_K;
    double eps = KllFloatsSketch.getNormalizedRankError(k, false);
    double epsPmf = KllFloatsSketch.getNormalizedRankError(k, true);
    int kEps = KllFloatsSketch.getKFromEpsilon(eps, false);
    int kEpsPmf = KllFloatsSketch.getKFromEpsilon(epsPmf, true);
    assertEquals(kEps, k);
    assertEquals(kEpsPmf, k);
  }

  @Test
  public void checkBounds() {
    KllFloatsSketch kll = new KllFloatsSketch(); //default k = 200
    for (int i = 0; i < 1000; i++) {
      kll.update(i);
    }
    double eps = kll.getNormalizedRankError(false);
    double est = kll.getQuantile(0.5);
    double ub = kll.getQuantileUpperBound(0.5);
    double lb = kll.getQuantileLowerBound(0.5);
    assertEquals(ub, (double)kll.getQuantile(.5 + eps));
    assertEquals(lb, (double)kll.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
  }

  //@Test //requires visual check
  public void checkNumRetainedAboveLevelZero() {
    KllFloatsSketch sketch = new KllFloatsSketch(20);
    for (int i = 0; i < 10; i++) { sketch.update(i + 1); }
    String s1 = sketch.toString(true, true);
    println(s1);
    KllFloatsSketch sketch2 = new KllFloatsSketch(20);
    for (int i = 0; i < 400; i++) {
      sketch2.update(i + 1);
    }
    sketch2.merge(sketch);
    String s2 = sketch2.toString(true, true);
    println(s2);
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
