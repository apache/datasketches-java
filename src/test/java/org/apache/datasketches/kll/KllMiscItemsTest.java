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
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfBooleansSerDe2;
import org.apache.datasketches.common.ArrayOfStringsSerDe2;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.kll.KllDirectCompactItemsSketch;
import org.apache.datasketches.kll.KllHeapItemsSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllItemsHelper;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.kll.KllPreambleUtil;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.ItemsSketchSortedView;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class KllMiscItemsTest {
  public ArrayOfStringsSerDe2 serDe = new ArrayOfStringsSerDe2();

  @Test
  public void checkSortedViewConstruction() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 20;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) {
      sk.update(Util.longToFixedLengthString(i, digits));
    }
    final ItemsSketchSortedView<String> sv = sk.getSortedView();
    final long[] cumWeights = sv.getCumulativeWeights();
    final String[] values = sv.getQuantiles();
    assertEquals(cumWeights.length, 20);
    assertEquals(values.length, 20);
    for (int i = 0; i < 20; i++) {
      assertEquals(cumWeights[i], i + 1);
      assertEquals(values[i], Util.longToFixedLengthString(i + 1, digits));
    }
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkBounds() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final int n = 1000;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) {
      sk.update(Util.longToFixedLengthString(i, digits));
    }
    final double eps = sk.getNormalizedRankError(false);
    final String est = sk.getQuantile(0.5);
    final String ub = sk.getQuantileUpperBound(0.5);
    final String lb = sk.getQuantileLowerBound(0.5);
    assertEquals(ub, sk.getQuantile(.5 + eps));
    assertEquals(lb, sk.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
    final double rest = sk.getRank(est);
    final double restUB = sk.getRankUpperBound(rest);
    final double restLB = sk.getRankLowerBound(rest);
    assertTrue((restUB - rest) < (2 * eps));
    assertTrue((rest - restLB) < (2 * eps));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions1() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 6, (byte)3); //corrupt with odd M
    KllItemsSketch.heapify(wseg, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions2() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 0, (byte)1); //corrupt preamble ints, should be 2
    KllItemsSketch.heapify(wseg, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions3() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sk.update("1");
    sk.update("2");
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 0, (byte)1); //corrupt preamble ints, should be 5
    KllItemsSketch.heapify(wseg, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions4() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 1, (byte)0); //corrupt SerVer, should be 1 or 2
    KllItemsSketch.heapify(wseg, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions5() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    wseg.set(JAVA_BYTE, 2, (byte)0); //corrupt FamilyID, should be 15
    KllItemsSketch.heapify(wseg, Comparator.naturalOrder(), serDe);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMisc() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {} //empty
    println(sk.toString(true, true));
    final int n = 21;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) {
      sk.update(Util.longToFixedLengthString(i, digits));
    }
    println(sk.toString(true, true));
    sk.toByteArray();
    final String[] items = sk.getTotalItemsArray();
    assertEquals(items.length, 33);
    final int[] levels = sk.getLevelsArray(sk.sketchStructure);
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  //@Test //set static enablePrinting = true for visual checking
  public void visualCheckToString() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    int n = 21;
    int digits = 3;
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    println(sk.toString(true, true));
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getMinItem(),"  1");
    assertEquals(sk.getMaxItem()," 21");
    assertEquals(sk.getNumRetained(), 11);

    final KllItemsSketch<String> sk2 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    n = 400;
    digits = 3;
    for (int i = 101; i <= (n + 100); i++) { sk2.update(Util.longToFixedLengthString(i, digits)); }
    println(LS + sk2.toString(true, true));
    assertEquals(sk2.getNumLevels(), 5);
    assertEquals(sk2.getMinItem(), "101");
    assertEquals(sk2.getMaxItem(), "500");
    assertEquals(sk2.getNumRetained(), 52);

    sk2.merge(sk);
    println(LS + sk2.toString(true, true));
    assertEquals(sk2.getNumLevels(), 5);
    assertEquals(sk2.getMinItem(),"  1");
    assertEquals(sk2.getMaxItem(), "500");
    assertEquals(sk2.getNumRetained(), 56);
  }

  @Test //set static enablePrinting = true for visual checking
  public void viewHeapCompactions() {
    final int k = 20;
    final int n = 108;
    final boolean withLevels = false;
    final boolean withLevelsAndItems = true;
    final int digits = Util.numDigits(n);
    int compaction = 0;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) {
      sk.update(Util.longToFixedLengthString(i, digits));
      if (sk.levelsArr[0] == 0) {
        println(LS + "#<<< BEFORE COMPACTION # " + (++compaction) + " >>>");
        println(sk.toString(true, true));
        sk.update(Util.longToFixedLengthString(++i, digits));
        println(LS + "#<<< AFTER COMPACTION  # " + (compaction) + " >>>");
        println(sk.toString(true, true));
        assertEquals(sk.getTotalItemsArray()[sk.levelsArr[0]], Util.longToFixedLengthString(i, digits));
      }
    }
    println(LS + "#<<< END STATE # >>>");
    println(sk.toString(withLevels, withLevelsAndItems));
    println("");
  }

  @Test //set static enablePrinting = true for visual checking
  public void viewCompactionAndSortedView() {
    final int n = 43;
    final int digits = Util.numDigits(n);
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    println(sk.toString(true, true));
    final ItemsSketchSortedView<String> sv = sk.getSortedView();
    final GenericSortedViewIterator<String> itr = sv.iterator();
    println("### SORTED VIEW");
    printf("%6s %12s %12s" + LS, "Idx", "Value", "CumWeight");
    int i = 0;
    while (itr.next()) {
      final String v = itr.getQuantile();
      final long wt = itr.getWeight();
      printf("%6d %12s %12d" + LS,i, v, wt);
      i++;
    }
    assertEquals(sv.getMinItem(), " 1");
    assertEquals(sv.getMaxItem(), Integer.toString(n));
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkWeightedUpdates1() {
    final int k = 20;
    final int weight = 127;
    final String item = "10";
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
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
    final int digits = 4;
    final int weight = 127;
    final String item = "  10";
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= initial; i++) { sk.update(Util.longToFixedLengthString(i + 1000, digits)); }
    println(sk.toString(true, true));
    sk.update(item, weight);
    println(sk.toString(true, true));
    assertEquals(sk.getNumRetained(), 65);
    assertEquals(sk.getN(), 1127);

    final GenericSortedViewIterator<String> itr = sk.getSortedView().iterator();
    println("### SORTED VIEW");
    printf("%12s %12s %12s" + LS, "Value", "Weight", "NaturalRank");
    long cumWt = 0;
    while (itr.next()) {
      final String v = itr.getQuantile();
      final long wt = itr.getWeight();
      final long natRank = itr.getNaturalRank(INCLUSIVE);
      cumWt += wt;
      assertEquals(cumWt, natRank);
      printf("%12s %12d %12d" + LS, v, wt, natRank);
    }
    assertEquals(cumWt, sk.getN());
  }


  @Test
  public void checkGrowLevels() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 21;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getTotalItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure)[2], 33);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkSketchInitializeItemsHeap() {
    final int k = 20; //don't change this
    final int n = 21;
    final int digits = Util.numDigits(n);
    KllItemsSketch<String> sk;

    println("#### CASE: ITEM FULL HEAP");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), n);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getTotalItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), "21");
    assertEquals(sk.getMinItem(), " 1");
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: ITEM HEAP EMPTY");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getTotalItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (final SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: ITEM HEAP SINGLE");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    sk.update("1");
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getTotalItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), "1");
    assertEquals(sk.getMinItem(), "1");
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkSketchInitializeItemsHeapifyCompactMemorySegment() {
    final int k = 20; //don't change this
    final int n = 21;
    final int digits = Util.numDigits(n);
    KllItemsSketch<String> sk;
    KllItemsSketch<String> sk2;
    byte[] compBytes;
    MemorySegment seg;

    println("#### CASE: ITEM FULL HEAPIFIED FROM COMPACT");
    sk2 = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk2.update(Util.longToFixedLengthString(i, digits)); }
    println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    seg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe));
    sk = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getTotalItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), "21");
    assertEquals(sk.getMinItem(), " 1");
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: ITEM EMPTY HEAPIFIED FROM COMPACT");
    sk2 =  KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    //println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    seg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe));
    sk = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getTotalItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (final SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (final SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: ITEM SINGLE HEAPIFIED FROM COMPACT");
    sk2 = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    sk2.update("1");
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    seg = MemorySegment.ofArray(compBytes);
    println(KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe));
    sk = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getTotalItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), "1");
    assertEquals(sk.getMinItem(), "1");
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  //public void checkSketchInitializeItemHeapifyUpdatableMemorySegment() Not Supported

  @Test //set static enablePrinting = true for visual checking
  public void checkMemoryToStringItemsCompact() {
    final int k = 20; //don't change this
    final int n = 21;
    final int digits = Util.numDigits(n);
    KllItemsSketch<String> sk;
    KllItemsSketch<String> sk2;
    byte[] compBytes;
    byte[] compBytes2;
    MemorySegment seg;
    String s;

    println("#### CASE: ITEM FULL COMPACT");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    compBytes = sk.toByteArray();
    seg = MemorySegment.ofArray(compBytes);
    s = KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    compBytes2 = sk2.toByteArray();
    seg = MemorySegment.ofArray(compBytes2);
    s = KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: ITEM EMPTY COMPACT");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    compBytes = sk.toByteArray();
    seg = MemorySegment.ofArray(compBytes);
    s = KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    compBytes2 = sk2.toByteArray();
    seg = MemorySegment.ofArray(compBytes2);
    s = KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: ITEM SINGLE COMPACT");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    sk.update("1");
    compBytes = sk.toByteArray();
    seg = MemorySegment.ofArray(compBytes);
    s = KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe);
    println("step 1: sketch to byte[]/MemorySegment & analyze MemorySegment");
    println(s);
    sk2 = KllItemsSketch.heapify(seg, Comparator.naturalOrder(), serDe);
    compBytes2 = sk2.toByteArray();
    seg = MemorySegment.ofArray(compBytes2);
    s = KllPreambleUtil.toString(seg, ITEMS_SKETCH, true, serDe);
    println("step 2: MemorySegment to heap sketch, to byte[]/MemorySegment & analyze MemorySegment. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkCreateItemsArray() { //used with weighted updates
    final String item = "10";
    final int weight = 108;
    final String[] itemsArr = KllItemsHelper.createItemsArray(String.class, item, weight);
    assertEquals(itemsArr.length, 4);
    Arrays.fill(itemsArr, item);
    outputItems(itemsArr);
  }

  private static void outputItems(final String[] itemsArr) {
    final String[] hdr2 = {"Index", "Value"};
    final String hdr2fmt = "%6s %15s" + LS;
    final String d2fmt = "%6d %15s" + LS;
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
  public void checkGetSingleItem() {
    final int k = 20;
    final KllItemsSketch<String> skHeap = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    skHeap.update("1");
    assertTrue(skHeap instanceof KllHeapItemsSketch);
    assertEquals(skHeap.getSingleItem(), "1");

    final MemorySegment srcSeg = MemorySegment.ofArray(KllHelper.toByteArray(skHeap, true)); //true is ignored
    final KllItemsSketch<String> skDirect = KllItemsSketch.wrap(srcSeg, Comparator.naturalOrder(), serDe);
    assertTrue(skDirect instanceof KllDirectCompactItemsSketch);
    assertEquals(skDirect.getSingleItem(), "1");

    final MemorySegment srcSeg2 = MemorySegment.ofArray(skHeap.toByteArray());
    final KllItemsSketch<String> skCompact = KllItemsSketch.wrap(srcSeg2, Comparator.naturalOrder(), serDe);
    assertTrue(skCompact instanceof KllDirectCompactItemsSketch);
    assertEquals(skCompact.getSingleItem(), "1");
  }

  @Test
  public void checkIssue484() {
    final int k = 20;
    final Boolean[] items = { true,false,true,false,true,false,true,false,true,false };
    final KllItemsSketch<Boolean> sketch = KllItemsSketch.newHeapInstance(k, Boolean::compareTo, new ArrayOfBooleansSerDe2());
    for (int i = 0; i < items.length; i++) { sketch.update(items[i]); }
    final byte[] serialized = sketch.toByteArray();
    final KllItemsSketch<Boolean> deserialized =
        KllItemsSketch.wrap(MemorySegment.ofArray(serialized), Boolean::compareTo, new ArrayOfBooleansSerDe2());
    checkSketchesEqual(sketch, deserialized);
  }

  private static <T> void checkSketchesEqual(final KllItemsSketch<T> expected, final KllItemsSketch<T> actual) {
    final ItemsSketchSortedView<T> expSV = expected.getSortedView();
    final ItemsSketchSortedView<T> actSV = actual.getSortedView();
    final int N = (int)actSV.getN();
    final long[] expCumWts = expSV.getCumulativeWeights();
    final Boolean[] expItemsArr = (Boolean[])expSV.getQuantiles();
    final long[] actCumWts = actSV.getCumulativeWeights();
    final Boolean[] actItemsArr = (Boolean[])actSV.getQuantiles();
    printf("%3s %8s %8s" + LS, "i","Actual", "Expected");
    for (int i = 0; i < N; i++) {
      printf("%3d %8s %8s" + LS, i, actItemsArr[i].toString(), expItemsArr[i].toString());
    }
    assertEquals(actCumWts, expCumWts);
    assertEquals(actItemsArr, expItemsArr);
    assertEquals(actual.getMinItem(), expected.getMinItem());
    assertEquals(actual.getMaxItem(), expected.getMaxItem());
  }

  private final static boolean enablePrinting = false;

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
