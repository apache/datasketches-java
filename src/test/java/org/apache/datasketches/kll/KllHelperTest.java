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
import static org.apache.datasketches.kll.KllSketch.getMaxSerializedSizeBytes;
import static org.apache.datasketches.kll.KllSketch.SketchType.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.testng.annotations.Test;

public class KllHelperTest {
  public ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();


  @Test
  public void checkConvertToCumulative() {
    final long[] array = {1,2,3,2,1};
    final long out = KllHelper.convertToCumulative(array);
    assertEquals(out, 9);
  }

  @Test
  public void checkCheckM() {
    try {
      checkM(0);
      fail();
    } catch (final SketchesArgumentException e) {}
    try {
      checkM(3);
      fail();
    } catch (final SketchesArgumentException e) {}
    try {
      checkM(10);
      fail();
    } catch (final SketchesArgumentException e) {}
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
  public void checkUpdatableSerDeDouble() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(200);
    for (int i = 1; i <= 533; i++) { sk.update(i); }
    final int retained = sk.getNumRetained();
    final int numLevels = ((KllSketch)sk).getNumLevels();
    println("NumLevels: " + numLevels);
    println("NumRetained: " + retained);

    final byte[] compByteArr1 = sk.toByteArray();
    final int compBytes1 = compByteArr1.length;
    println("compBytes1: " + compBytes1);

    final byte[] upByteArr1 = KllHelper.toByteArray(sk, true);
    final int upBytes1 = upByteArr1.length;
    println("upBytes1: " + upBytes1);

    MemorySegment seg;
    KllDoublesSketch sk2;

    seg = MemorySegment.ofArray(compByteArr1);
    sk2 = KllDoublesSketch.heapify(seg);
    final byte[] compByteArr2 = sk2.toByteArray();
    final int compBytes2 = compByteArr2.length;
    println("compBytes2: " + compBytes2);
    assertEquals(compBytes1, compBytes2);
    assertEquals(sk2.getNumRetained(), retained);

    seg = MemorySegment.ofArray(compByteArr2);
    sk2 = KllDoublesSketch.heapify(seg);
    final byte[] upByteArr2 = KllHelper.toByteArray(sk2, true);
    final int upBytes2 = upByteArr2.length;
    println("upBytes2: " + upBytes2);
    assertEquals(upBytes1, upBytes2);
    assertEquals(sk2.getNumRetained(), retained);
  }

  @Test
  public void checkUpdatableSerDeFloat() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(200);
    for (int i = 1; i <= 533; i++) { sk.update(i); }
    final int retained = sk.getNumRetained();
    final int numLevels = ((KllSketch)sk).getNumLevels();
    println("NumLevels: " + numLevels);
    println("NumRetained: " + retained);

    final byte[] compByteArr1 = sk.toByteArray();
    final int compBytes1 = compByteArr1.length;
    println("compBytes1: " + compBytes1);

    final byte[] upByteArr1 = KllHelper.toByteArray(sk, true);
    final int upBytes1 = upByteArr1.length;
    println("upBytes1: " + upBytes1);

    MemorySegment seg;
    KllFloatsSketch sk2;

    seg = MemorySegment.ofArray(compByteArr1);
    sk2 = KllFloatsSketch.heapify(seg);
    final byte[] compByteArr2 = sk2.toByteArray();
    final int compBytes2 = compByteArr2.length;
    println("compBytes2: " + compBytes2);
    assertEquals(compBytes1, compBytes2);
    assertEquals(sk2.getNumRetained(), retained);

    seg = MemorySegment.ofArray(compByteArr2);
    sk2 = KllFloatsSketch.heapify(seg);
    final byte[] upByteArr2 = KllHelper.toByteArray(sk2, true);
    final int upBytes2 = upByteArr2.length;
    println("upBytes2: " + upBytes2);
    assertEquals(upBytes1, upBytes2);
    assertEquals(sk2.getNumRetained(), retained);
  }

  @Test
  public void checkUpdatableSerDeItem() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(200, Comparator.naturalOrder(), serDe);
    final int n = 533;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }

    final int retained = sk.getNumRetained();
    final int numLevels = ((KllSketch)sk).getNumLevels();
    println("NumLevels: " + numLevels);
    println("NumRetained: " + retained);

    final byte[] compByteArr1 = sk.toByteArray();
    final int compBytes1 = compByteArr1.length;
    println("compBytes1: " + compBytes1);

    final byte[] upByteArr1 = KllHelper.toByteArray(sk, true);
    final int upBytes1 = upByteArr1.length;
    println("upBytes1: " + upBytes1);
    assertEquals(upBytes1, compBytes1); //only true for Items Sketch

    MemorySegment seg;
    KllItemsSketch<String> sk2;

    seg = MemorySegment.ofArray(compByteArr1);
    sk2 = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    final byte[] compByteArr2 = sk2.toByteArray();
    final int compBytes2 = compByteArr2.length;
    println("compBytes2: " + compBytes2);
    assertEquals(compBytes1, compBytes2);
    assertEquals(sk2.getNumRetained(), retained);

    seg = MemorySegment.ofArray(compByteArr2);
    sk2 = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    final byte[] upByteArr2 = KllHelper.toByteArray(sk2, true);
    final int upBytes2 = upByteArr2.length;
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
  public void getMaxUpdatableItemsSerializedSizeBytes() {
    try {
      KllSketch.getMaxSerializedSizeBytes(KllSketch.DEFAULT_K, 1L << 30, ITEMS_SKETCH, true);
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void getStatsAtNumLevels() {
    final int k = 200;
    final int m = 8;
    final int numLevels = 23;
    final KllHelper.LevelStats lvlStats =
        KllHelper.getFinalSketchStatsAtNumLevels(k, m, numLevels, true);
    assertEquals(lvlStats.numItems, 697);
    assertEquals(lvlStats.n, 1257766904);
  }

  @Test
  public void getStatsAtNumLevels2() {
    final int k = 20;
    final int m = KllSketch.DEFAULT_M;
    final int numLevels = 2;
    final KllHelper.LevelStats lvlStats =
        KllHelper.getFinalSketchStatsAtNumLevels(k, m, numLevels, true);
    assertEquals(lvlStats.numLevels, 2);
    assertEquals(lvlStats.numItems, 33);
  }

  @Test
  public void testGetAllLevelStatsDoubles() {
    final long n = 1L << 30;
    final int k = 200;
    final int m = KllSketch.DEFAULT_M;
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, m, n, DOUBLES_SKETCH, true);
    assertEquals(gStats.maxN, 1_257_766_904);
    assertEquals(gStats.numLevels, 23);
    assertEquals(gStats.maxItems, 697);
    assertEquals(gStats.compactBytes, 5704);
    assertEquals(gStats.updatableBytes, 5708);
  }

  @Test
  public void testGetAllLevelStatsFloats() {
    final long n = 1L << 30;
    final int k = 200;
    final int m = KllSketch.DEFAULT_M;
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, m, n, FLOATS_SKETCH, true);
    assertEquals(gStats.maxN, 1_257_766_904);
    assertEquals(gStats.numLevels, 23);
    assertEquals(gStats.maxItems, 697);
    assertEquals(gStats.compactBytes, 2908);
    assertEquals(gStats.updatableBytes, 2912);
  }

  @Test
  public void testGetAllLevelStatsDoubles2() {
    final long n = 533;
    final int k = 200;
    final int m = KllSketch.DEFAULT_M;
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, m, n, DOUBLES_SKETCH, true);
    assertEquals(gStats.maxN, 533);
    assertEquals(gStats.numLevels, 2);
    assertEquals(gStats.maxItems, 333);
    assertEquals(gStats.compactBytes, 2708);
    assertEquals(gStats.updatableBytes, 2712);
  }

  @Test
  public void testGetAllLevelStatsFloats2() {
    final long n = 533;
    final int k = 200;
    final int m = KllSketch.DEFAULT_M;
    final KllHelper.GrowthStats gStats =
        KllHelper.getGrowthSchemeForGivenN(k, m, n, FLOATS_SKETCH, true);
    assertEquals(gStats.maxN, 533);
    assertEquals(gStats.numLevels, 2);
    assertEquals(gStats.maxItems, 333);
    assertEquals(gStats.compactBytes, 1368);
    assertEquals(gStats.updatableBytes, 1372);
  }

  @Test
  public void testGetAllLevelStatsItems() {
    final long n = 533;
    final int k = 200;
    final int m = KllSketch.DEFAULT_M;
    try {
      KllHelper.getGrowthSchemeForGivenN(k, m, n, ITEMS_SKETCH, true);
    } catch (final SketchesArgumentException e) { }
  }

  /**
   * Println Object o
   * @param o object to print
   */
  static void println(final Object o) {
    //System.out.println(o.toString());
  }

}
