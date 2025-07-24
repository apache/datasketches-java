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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.bitAt;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDirectDoublesSketch;
import org.apache.datasketches.kll.KllDoublesHelper;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllHeapDoublesSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllPreambleUtil;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.DoublesSortedViewIterator;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class KllMiscDoublesTest {

  @Test
  public void checkSortedViewConstruction() {
    final KllDoublesSketch kll = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 20; i++) { kll.update(i); }
    final DoublesSortedView fsv = kll.getSortedView();
    final long[] cumWeights = fsv.getCumulativeWeights();
    final double[] values = fsv.getQuantiles();
    assertEquals(cumWeights.length, 20);
    assertEquals(values.length, 20);
    for (int i = 0; i < 20; i++) {
      assertEquals(cumWeights[i], i + 1);
      assertEquals(values[i], i + 1);
    }
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkBounds() {
    final KllDoublesSketch kll = KllDoublesSketch.newHeapInstance(); // default k = 200
    for (int i = 0; i < 1000; i++) {
      kll.update(i);
    }
    final double eps = kll.getNormalizedRankError(false);
    final double est = kll.getQuantile(0.5);
    final double ub = kll.getQuantileUpperBound(0.5);
    final double lb = kll.getQuantileLowerBound(0.5);
    assertEquals(ub, kll.getQuantile(.5 + eps));
    assertEquals(lb, kll.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
    final double rest = kll.getRank(est);
    final double restUB = kll.getRankUpperBound(rest);
    final double restLB = kll.getRankLowerBound(rest);
    assertTrue((restUB - rest) < (2 * eps));
    assertTrue((rest - restLB) < (2 * eps));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions1() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 6, (byte) 3); //corrupt with odd M
    KllDoublesSketch.heapify(wseg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions2() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 0, (byte) 1); //corrupt preamble ints, should be 2
    KllDoublesSketch.heapify(wseg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions3() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    sk.update(1.0);
    sk.update(2.0);
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 0, (byte) 1); //corrupt preamble ints, should be 5
    KllDoublesSketch.heapify(wseg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions4() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 1, (byte) 0); //corrupt SerVer, should be 1 or 2
    KllDoublesSketch.heapify(wseg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions5() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 2, (byte) 0); //corrupt FamilyID, should be 15
    KllDoublesSketch.heapify(wseg);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMisc() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(8);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {} //empty
    println(sk.toString(true, true));
    for (int i = 0; i < 20; i++) { sk.update(i); }
    println(sk.toString(true, true));
    sk.toByteArray();
    final double[] items = sk.getDoubleItemsArray();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray(sk.sketchStructure);
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  @Test //set static enablePrinting = true for visual checking
  public void visualCheckToString() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    int n = 21;
    for (int i = 1; i <= n; i++) { sk.update(i); }
    println(sk.toString(true, true));
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getMaxItem(), 21);
    assertEquals(sk.getNumRetained(), 11);

    final KllDoublesSketch sk2 = KllDoublesSketch.newHeapInstance(20);
    n = 400;
    for (int i = 101; i <= (n + 100); i++) { sk2.update(i); }
    println(LS + sk2.toString(true, true));
    assertEquals(sk2.getNumLevels(), 5);
    assertEquals(sk2.getMinItem(), 101);
    assertEquals(sk2.getMaxItem(), 500);
    assertEquals(sk2.getNumRetained(), 52);

    sk2.merge(sk);
    println(LS + sk2.toString(true, true));
    assertEquals(sk2.getNumLevels(), 5);
    assertEquals(sk2.getMinItem(), 1);
    assertEquals(sk2.getMaxItem(), 500);
    assertEquals(sk2.getNumRetained(), 56);
  }

  @Test //set static enablePrinting = true for visual checking
  public void viewHeapCompactions() {
    final int k = 20;
    final int n = 108;
    final boolean withLevels = false;
    final boolean withLevelsAndItems = true;
    int compaction = 0;
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) {
      sk.update(i);
      if (sk.levelsArr[0] == 0) {
        println(LS + "#<<< BEFORE COMPACTION # " + (++compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        sk.update(++i);
        println(LS + "#<<< AFTER COMPACTION  # " + (compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        assertEquals(sk.getDoubleItemsArray()[sk.levelsArr[0]], i);
      }
    }
    println(LS + "#<<< END STATE # >>>");
    println(sk.toString(withLevels, withLevelsAndItems));
    println("");
  }

  @Test //set static enablePrinting = true for visual checking
  public void viewDirectCompactions() {
    final int k = 20;
    final int n = 108;
    final boolean withLevels = false;
    final boolean withLevelsAndItems = true;
    int compaction = 0;
    final int sizeBytes = KllSketch.getMaxSerializedSizeBytes(k, n, DOUBLES_SKETCH, true);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[sizeBytes]);
    final KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(k, wseg, null);
    for (int i = 1; i <= n; i++) {
      sk.update(i);
      if (sk.levelsArr[0] == 0) {
        println(LS + "#<<< BEFORE COMPACTION # " + (++compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        sk.update(++i);
        println(LS + "#<<< AFTER COMPACTION  # " + (compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        assertEquals(sk.getDoubleItemsArray()[sk.levelsArr[0]], i);
      }
    }
    println(LS + "#<<< END STATE # >>>");
    println(sk.toString(withLevels, withLevelsAndItems));
    println("");
  }

  @Test //set static enablePrinting = true for visual checking
  public void viewCompactionAndSortedView() {
    final int n = 43;
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    println(sk.toString(true, true));
    final DoublesSortedView sv = sk.getSortedView();
    final DoublesSortedViewIterator itr = sv.iterator();
    println("### SORTED VIEW");
    printf("%6s %12s %12s" + LS, "Idx", "Value", "CumWeight");
    int i = 0;
    while (itr.next()) {
      final double v = itr.getQuantile();
      final long wt = itr.getWeight();
      printf("%6d %12.1f %12d" + LS, i, v, wt);
      i++;
    }
    assertEquals(sv.getMinItem(), 1.0);
    assertEquals(sv.getMaxItem(), n * 1.0);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkWeightedUpdates1() {
    final int k = 20;
    final int weight = 127;
    final double item = 10.0;
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    println(sk.toString(true, true));
    sk.update(item, weight);
    println(sk.toString(true, true));
    assertEquals(sk.getNumRetained(), 7);
    assertEquals(sk.getN(), weight);
    sk.update(item, weight);
    println(sk.toString(true, true));
    assertEquals(sk.getNumRetained(), 14);
    assertEquals(sk.getN(), 254);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkWeightedUpdates2() {
    final int k = 20;
    final int initial = 1000;
    final int weight = 127;
    final double item = 10.0;
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= initial; i++) { sk.update(i + 1000); }
    println(sk.toString(true, true));
    sk.update(item, weight);
    println(sk.toString(true, true));
    assertEquals(sk.getNumRetained(), 65);
    assertEquals(sk.getN(), 1127);

    final DoublesSortedViewIterator itr = sk.getSortedView().iterator();
    println("### SORTED VIEW");
    printf("%12s %12s %12s" + LS, "Value", "Weight", "NaturalRank");
    long cumWt = 0;
    while (itr.next()) {
      final double v = itr.getQuantile();
      final long wt = itr.getWeight();
      final long natRank = itr.getNaturalRank(INCLUSIVE);
      cumWt += wt;
      assertEquals(cumWt, natRank);
      printf("%12.1f %12d %12d" + LS, v, wt, natRank);
    }
    assertEquals(cumWt, sk.getN());
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkCreateItemsArray() { //used with weighted updates
    final double item = 10.0;
    final int weight = 108;
    final double[] itemsArr = KllDoublesHelper.createItemsArray(item, weight);
    assertEquals(itemsArr.length, 4);
    Arrays.fill(itemsArr, item);
    outputItems(itemsArr);
  }

  private static void outputItems(final double[] itemsArr) {
    final String[] hdr2 = {"Index", "Value"};
    final String hdr2fmt = "%6s %15s" + LS;
    final String d2fmt = "%6d %15f" + LS;
    println("ItemsArr");
    printf(hdr2fmt, (Object[]) hdr2);
    for (int i = 0; i < itemsArr.length; i++) {
      printf(d2fmt, i, itemsArr[i]);
    }
    println("");
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkCreateLevelsArray() { //used with weighted updates
    final int weight = 108;
    final int[] levelsArr = KllHelper.createLevelsArray(weight);
    assertEquals(levelsArr.length, 8);
    final int[] correct = {0,0,0,1,2,2,3,4};
    for (int i = 0; i < levelsArr.length; i++) {
      assertEquals(levelsArr[i], correct[i]);
    }
    outputLevels(weight, levelsArr);
  }

  private static void outputLevels(final int weight, final int[] levelsArr) {
    final String[] hdr = {"Lvl", "StartAdr", "BitPattern", "Weight"};
    final String hdrfmt = "%3s %9s %10s %s" + LS;
    final String dfmt   = "%3d %9d %10d %d" + LS;
    final String dfmt_2 = "%3d %9d %s" + LS;
    println("Count = " + weight + " => " + (Integer.toBinaryString(weight)));
    println("LevelsArr");
    printf(hdrfmt, (Object[]) hdr);
    for (int i = 0; i < levelsArr.length; i++) {
      if (i == (levelsArr.length - 1)) { printf(dfmt_2, i, levelsArr[i], "ItemsArr.length"); }
      else {
        final int j = bitAt(weight, i);
        printf(dfmt, i, levelsArr[i], j, 1 << (i));
      }
    }
    println("");
  }

  @Test
  public void viewMemorySketchData() {
    final int k = 20;
    final int n = 109;
    final boolean withLevels = true;
    final boolean withLevelsAndItems = true;
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final KllDoublesSketch ddSk = KllDoublesSketch.wrap(seg);
    println(ddSk.toString(withLevels, withLevelsAndItems));
    assertEquals(ddSk.getN(), n);
  }

  @Test //set static enablePrinting = true for visual checking
    public void checkIntCapAux() {
      final String[] hdr = {"level", "depth", "wt", "cap", "(end)", "MaxN"};
      final String hdrFmt =  "%6s %6s %28s %10s %10s %34s" + LS;
      final String dataFmt = "%6d %6d %,28d %,10d %,10d %,34.0f" + LS;
      final int k = 1000;
      final int m = 8;
      final int numLevels = 20;
      println("k=" + k + ", m=" + m + ", numLevels=" + numLevels);
      printf(hdrFmt, (Object[]) hdr);
      double maxN = 0;
      final double[] correct = {0,1,1,2,2,3,5,8,12,17,26,39,59,88,132,198,296,444,667,1000};
      for (int i = 0; i < numLevels; i++) {
        final int depth = numLevels - i - 1;
        final long cap = KllHelper.intCapAux(k, depth);
        final long end = Math.max(m, cap);
        final long wt = 1L << i;
        maxN += (double)wt * (double)end;
        printf(dataFmt, i, depth, wt, cap, end, maxN);
        assertEquals(cap, correct[i]);
      }
    }

  @Test //set static enablePrinting = true for visual checking
  public void checkIntCapAuxAux() {
    final String[] hdr = {"d","twoK","2k*2^d","3^d","tmp=2k*2^d/3^d","(tmp + 1)/2", "(end)"};
    final String hdrFmt =  "%6s %10s %20s %20s %15s %12s %10s" + LS;
    final String dataFmt = "%6d %10d %,20d %,20d %15d %12d %10d" + LS;
    final long k = (1L << 16) - 1L;
    final long m = 8;
    println("k = " + k + ", m = " + m);
    printf(hdrFmt, (Object[]) hdr);
    final long[] correct =
 {65535,43690,29127,19418,12945,8630,5753,3836,2557,1705,1136,758,505,337,224,150,100,67,44,30,20,13,9,6,4,3,2,1,1,1,0};
    for (int i = 0; i < 31; i++) {
      final long twoK = k << 1;
      final long twoKxtwoD = twoK << i;
      final long threeToD = KllHelper.powersOfThree[i];
      final long tmp = twoKxtwoD / threeToD;
      final long result = (tmp + 1L) >>> 1;
      final long end = Math.max(m, result); //performed later
      printf(dataFmt, i, twoK, twoKxtwoD, threeToD, tmp, result, end);
      assertEquals(result,correct[i]);
      assertEquals(result, KllHelper.intCapAuxAux(k, i));
    }
  }

  @Test
  public void checkGrowLevels() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getDoubleItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure)[2], 33);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkSketchInitializeDoubleHeap() {
    final int k = 20; //don't change this
    KllDoublesSketch sk;

    println("#### CASE: DOUBLE FULL HEAP");
    sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= (k + 1); i++) { sk.update(i); }
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), 21.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE HEAP EMPTY");
    sk = KllDoublesSketch.newHeapInstance(k);
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (final SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE HEAP SINGLE");
    sk = KllDoublesSketch.newHeapInstance(k);
    sk.update(1);
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), 1.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test // set static enablePrinting = true for visual checking
  public void checkSketchInitializeDoubleHeapifyCompactMemorySegment() {
    final int k = 20; // don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] compBytes;
    MemorySegment wseg;

    println("#### CASE: DOUBLE FULL HEAPIFIED FROM COMPACT");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= (k + 1); i++) { sk2.update(i); }
    // println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wseg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true));
    sk = KllDoublesSketch.heapify(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), 21.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM COMPACT");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    // println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    wseg = MemorySegment.ofArray(compBytes);
    //println(KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true));
    sk = KllDoublesSketch.heapify(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (final SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM COMPACT");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    sk2.update(1);
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wseg = MemorySegment.ofArray(compBytes);
    //println(KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true));
    sk = KllDoublesSketch.heapify(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), 1.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test // set static enablePrinting = true for visual checking
  public void checkSketchInitializeDoubleHeapifyUpdatableMemorySegment() {
    final int k = 20; // don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] compBytes;
    MemorySegment wseg;

    println("#### CASE: DOUBLE FULL HEAPIFIED FROM UPDATABLE");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= (k + 1); i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true));
    sk = KllHeapDoublesSketch.heapifyImpl(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), 21.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    // println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(compBytes);
    //println(KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true));
    sk = KllHeapDoublesSketch.heapifyImpl(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (final SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = KllDoublesSketch.newHeapInstance(k);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(compBytes);
    //println(KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true));
    sk = KllHeapDoublesSketch.heapifyImpl(wseg);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), 1.0);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMemoryToStringDoubleCompact() {
    final int k = 20; //don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] compBytes;
    byte[] compBytes2;
    MemorySegment wseg;
    String s;

    println("#### CASE: DOUBLE FULL COMPACT");
    sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= (k + 1); i++) { sk.update(i); }
    compBytes = sk.toByteArray();
    wseg = MemorySegment.ofArray(compBytes);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/segory & analyze segory");
    println(s);
    sk2 = KllDoublesSketch.heapify(wseg);
    compBytes2 = sk2.toByteArray();
    wseg = MemorySegment.ofArray(compBytes2);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 2: segorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: DOUBLE EMPTY COMPACT");
    sk = KllDoublesSketch.newHeapInstance(20);
    compBytes = sk.toByteArray();
    wseg = MemorySegment.ofArray(compBytes);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllDoublesSketch.heapify(wseg);
    compBytes2 = sk2.toByteArray();
    wseg = MemorySegment.ofArray(compBytes2);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: DOUBLE SINGLE COMPACT");
    sk = KllDoublesSketch.newHeapInstance(20);
    sk.update(1);
    compBytes = sk.toByteArray();
    wseg = MemorySegment.ofArray(compBytes);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllDoublesSketch.heapify(wseg);
    compBytes2 = sk2.toByteArray();
    wseg = MemorySegment.ofArray(compBytes2);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMemoryToStringDoubleUpdatable() {
    final int k = 20; //don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] upBytes;
    byte[] upBytes2;
    MemorySegment wseg;
    String s;

    println("#### CASE: DOUBLE FULL UPDATABLE");
    sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= (k + 1); i++) { sk.update(i); }
    upBytes = KllHelper.toByteArray(sk, true);
    wseg = MemorySegment.ofArray(upBytes);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllHeapDoublesSketch.heapifyImpl(wseg);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(upBytes2);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s); //note: heapify does not copy free space, while toUpdatableByteArray does
    assertEquals(sk.getN(), sk2.getN());
    assertEquals(sk.getMinItem(), sk2.getMinItem());
    assertEquals(sk.getMaxItem(), sk2.getMaxItem());
    assertEquals(sk.getNumRetained(), sk2.getNumRetained());

    println("#### CASE: DOUBLE EMPTY UPDATABLE");
    sk = KllDoublesSketch.newHeapInstance(k);
    upBytes = KllHelper.toByteArray(sk, true);
    wseg = MemorySegment.ofArray(upBytes);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllHeapDoublesSketch.heapifyImpl(wseg);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(upBytes2);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: DOUBLE SINGLE UPDATABL");
    sk = KllDoublesSketch.newHeapInstance(k);
    sk.update(1);
    upBytes = KllHelper.toByteArray(sk, true);
    wseg = MemorySegment.ofArray(upBytes);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllHeapDoublesSketch.heapifyImpl(wseg);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wseg = MemorySegment.ofArray(upBytes2);
    s = KllPreambleUtil.toString(wseg, DOUBLES_SKETCH, true);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);
  }

  @Test
  public void checkSimpleMerge() {
    final int k = 20;
    final int m = 8;
    final int n1 = 21;
    final int n2 = 43;
    final MemorySegment wseg = MemorySegment.ofArray(new byte[3000]);
    final MemorySegment wseg2 = MemorySegment.ofArray(new byte[3000]);

    final KllDoublesSketch sk1 = KllDirectDoublesSketch.newDirectUpdatableInstance(k, m, wseg, null);
    final KllDoublesSketch sk2 = KllDirectDoublesSketch.newDirectUpdatableInstance(k, m, wseg2, null);
    for (int i = 1; i <= n1; i++) {
      sk1.update(i);
    }
    for (int i = 1; i <= n2; i++) {
      sk2.update(i + 100);
    }
    sk1.merge(sk2);
    assertEquals(sk1.getMinItem(), 1.0);
    assertEquals(sk1.getMaxItem(), 143.0);
  }

  @Test
  public void checkGetSingleItem() {
    final int k = 20;
    final KllDoublesSketch skHeap = KllDoublesSketch.newHeapInstance(k);
    skHeap.update(1);
    assertTrue(skHeap instanceof KllHeapDoublesSketch);
    assertEquals(skHeap.getDoubleSingleItem(), 1.0);

    final MemorySegment srcSeg = MemorySegment.ofArray(KllHelper.toByteArray(skHeap, true));
    final KllDoublesSketch skDirect = KllDoublesSketch.wrap(srcSeg);
    assertTrue(skDirect instanceof KllDirectDoublesSketch);
    assertEquals(skDirect.getDoubleSingleItem(), 1.0);

    final MemorySegment srcSeg2 = MemorySegment.ofArray(skHeap.toByteArray());
    final KllDoublesSketch skCompact = KllDoublesSketch.wrap(srcSeg2);
    assertEquals(skCompact.getDoubleSingleItem(), 1.0);
  }

  @Test
  public void printlnTest() {
    final String s = "PRINTING:  printf in " + this.getClass().getName();
    println(s);
    printf("%s" + LS, s);
  }

  private final static boolean enablePrinting = false;

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ... args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
