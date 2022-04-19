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

import static org.apache.datasketches.kll.KllHelper.checkM;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class KllHelperTest {

  /**
   * Println Object o
   * @param o object to print
   */
  static void println(Object o) {
    //System.out.println(o.toString());
  }

  @Test
  public void checkCheckM() {
    try {
      checkM(0);
      fail();
    } catch (SketchesArgumentException e) {}
    try {
      checkM(3);
      fail();
    } catch (SketchesArgumentException e) {}
    try {
      checkM(10);
      fail();
    } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkGetKFromEps() {
    final int k = KllSketch.DEFAULT_K;
    final double eps = KllHelper.getNormalizedRankError(k, false);
    final double epsPmf = KllHelper.getNormalizedRankError(k, true);
    final int kEps = KllSketch.getKFromEpsilon(eps, false);
    final int kEpsPmf = KllSketch.getKFromEpsilon(epsPmf, true);
    assertEquals(kEps, k);
    assertEquals(kEpsPmf, k);
  }

  @Test
  public void checkIntCapAux() {
    int lvlCap = KllHelper.levelCapacity(10, 61, 0, 8);
    assertEquals(lvlCap, 8);
    lvlCap = KllHelper.levelCapacity(10, 61, 60, 8);
    assertEquals(lvlCap, 10);
  }

  @Test
  public void checkSuperLargeKandLevels() {
    //This is beyond what the sketch can be configured for.
    final int size = KllHelper.computeTotalItemCapacity(1 << 29, 8, 61);
    assertEquals(size, 1_610_612_846);
  }

  @Test
  public void checkUbOnNumLevels() {
    assertEquals(KllHelper.ubOnNumLevels(0), 1);
  }

  @Test
  public void checkUpdatableSerDe() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(200);
    for (int i = 1; i <= 533; i++) { sk.update(i); }
    int retained = sk.getNumRetained();
    int numLevels = ((KllSketch)sk).getNumLevels();
    println("NumLevels: " + numLevels);
    println("NumRetained: " + retained);

    byte[] compByteArr1 = sk.toByteArray();
    int compBytes1 = compByteArr1.length;
    println("compBytes1: " + compBytes1);

    byte[] upByteArr1 = KllHelper.toUpdatableByteArrayImpl(sk);
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
    byte[] upByteArr2 = KllHelper.toUpdatableByteArrayImpl(sk2);
    int upBytes2 = upByteArr2.length;
    println("upBytes2: " + upBytes2);
    assertEquals(upBytes1, upBytes2);
    assertEquals(sk2.getNumRetained(), retained);
  }


  @Test
  public void getMaxCompactDoublesSerializedSizeBytes() {
    final int sizeBytes =
        KllSketch.getMaxSerializedSizeBytes(KllSketch.DEFAULT_K, 1L << 30, DOUBLES_SKETCH, false);
    assertEquals(sizeBytes, 5704);
  }

  @Test
  public void getMaxCompactFloatsSerializedSizeBytes() {
    final int sizeBytes =
        KllSketch.getMaxSerializedSizeBytes(KllSketch.DEFAULT_K, 1L << 30, FLOATS_SKETCH, false);
    assertEquals(sizeBytes, 2908);
  }

  @Test
  public void getMaxUpdatableDoubleSerializedSizeBytes() {
    final int sizeBytes =
        KllSketch.getMaxSerializedSizeBytes(KllSketch.DEFAULT_K, 1L << 30, DOUBLES_SKETCH, true);
    assertEquals(sizeBytes, 5708);
  }

  @Test
  public void getMaxUpdatableFloatsSerializedSizeBytes() {
    final int sizeBytes =
        KllSketch.getMaxSerializedSizeBytes(KllSketch.DEFAULT_K, 1L << 30, FLOATS_SKETCH, true);
    assertEquals(sizeBytes, 2912);
  }

  @Test
  public void getStatsAtNumLevels() {
    int k = 200;
    int m = 8;
    int numLevels = 23;
    KllHelper.LevelStats lvlStats =
        KllHelper.getFinalSketchStatsAtNumLevels(k, m, numLevels, true);
    assertEquals(lvlStats.items, 697);
    assertEquals(lvlStats.n, 1257766904);
  }

  @Test
  public void getStatsAtNumLevels2() {
    int k = 20;
    int m = 8;
    int numLevels = 2;
    KllHelper.LevelStats lvlStats =
        KllHelper.getFinalSketchStatsAtNumLevels(k, KllSketch.DEFAULT_M, numLevels, true);
    assertEquals(lvlStats.numLevels, 2);
    assertEquals(lvlStats.items, 33);
  }

  @Test
  public void testGetAllLevelStats() {
    long n = 1L << 30;
    int k = 200;
    int m = 8;
    KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, m, n, DOUBLES_SKETCH, true);
    assertEquals(gStats.compactBytes, 5704);
  }

  @Test
  public void testGetAllLevelStats2() {
    long n = 533;
    int k = 200;
    int m = 8;
    KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, KllSketch.DEFAULT_M, n, DOUBLES_SKETCH, true);
    assertEquals(gStats.numLevels, 2);
    assertEquals(gStats.maxItems, 333);

  }
}
