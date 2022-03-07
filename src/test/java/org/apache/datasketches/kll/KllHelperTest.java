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

import static org.apache.datasketches.kll.KllHelper.getAllLevelStatsGivenN;
import static org.apache.datasketches.kll.KllHelper.getLevelStats;
import static org.apache.datasketches.kll.KllPreambleUtil.SketchType.DOUBLE_SKETCH;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.kll.KllHelper.LevelStats;
import org.apache.datasketches.kll.KllPreambleUtil.SketchType;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class KllHelperTest {

  @Test //convert two false below to true for visual checking
  public void testGetAllLevelStats() {
    long n = 1L << 30;
    int k = 200;
    int m = 8;
    LevelStats lvlStats = getAllLevelStatsGivenN(k, m, n, false, false, DOUBLE_SKETCH);
    assertEquals(lvlStats.getCompactBytes(), 5708);
  }

  @Test //convert two false below to true for visual checking
  public void getStatsAtNumLevels() {
    int k = 200;
    int m = 8;
    int numLevels = 23;
    LevelStats lvlStats = getLevelStats(k, m, numLevels, false, false, DOUBLE_SKETCH);
    assertEquals(lvlStats.getCompactBytes(), 5708);
  }

  @Test
  public void checkUpdatableSerDe() {
    KllDoublesSketch sk = new KllDoublesSketch(200);
    for (int i = 1; i <= 533; i++) { sk.update(i); }
    int retained = sk.getNumRetained();
    int numLevels = ((KllHeapSketch)sk).getNumLevels();
    println("NumLevels: " + numLevels);
    println("NumRetained: " + retained);

    byte[] compByteArr1 = sk.toByteArray();
    int compBytes1 = compByteArr1.length;
    println("compBytes1: " + compBytes1);

    byte[] upByteArr1 = sk.toUpdatableByteArray();
    int upBytes1 = upByteArr1.length;
    println("upBytes1: " + upBytes1);

    Memory mem;
    KllDoublesSketch sk2;

    mem = Memory.wrap(compByteArr1);
    sk2 = KllDoublesSketch.heapify(mem);
    byte[] compByteArr2 = sk2.toByteArray();
    int compBytes2 = compByteArr2.length;
    println("compBytes2: " + compBytes2);
    assertEquals(compBytes1, compBytes2);
    assertEquals(sk2.getNumRetained(), retained);

    mem = Memory.wrap(compByteArr2);
    sk2 = KllDoublesSketch.heapify(mem);
    byte[] upByteArr2 = sk2.toUpdatableByteArray();
    int upBytes2 = upByteArr2.length;
    println("upBytes2: " + upBytes2);
    assertEquals(upBytes1, upBytes2);
    assertEquals(sk2.getNumRetained(), retained);
  }

  //Experimental

  //@Test //convert two false below to true for visual checking
  public void testGetAllLevelStats2() {
    long n = 533;
    int k = 200;
    int m = 8;
    LevelStats lvlStats = getAllLevelStatsGivenN(k, m, n, true, true, DOUBLE_SKETCH);
  }

  //@Test
  public void getStatsAtNumLevels2() {
    int k = 20;
    int m = 8;
    int numLevels = 2;
    LevelStats lvlStats = getLevelStats(k, m, numLevels, true, true, DOUBLE_SKETCH);
  }

  /**
   * Println Object o
   * @param o object to print
   */
  static void println(Object o) {
    //System.out.println(o.toString());
  }
}
