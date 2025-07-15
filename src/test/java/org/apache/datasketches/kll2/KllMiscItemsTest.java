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

package org.apache.datasketches.kll2;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.bitAt;
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.ItemsSketchSortedView;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class KllMiscItemsTest {
  public ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test
  public void checkSortedViewConstruction() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 20;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) {
      sk.update(Util.longToFixedLengthString(i, digits));
    }
    ItemsSketchSortedView<String> sv = sk.getSortedView();
    long[] cumWeights = sv.getCumulativeWeights();
    String[] values = sv.getQuantiles();
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
    assertTrue(restUB - rest < (2 * eps));
    assertTrue(rest - restLB < (2 * eps));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions1() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(6, (byte)3); //corrupt with odd M
    KllItemsSketch.heapify(wmem, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions2() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte)1); //corrupt preamble ints, should be 2
    KllItemsSketch.heapify(wmem, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions3() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sk.update("1");
    sk.update("2");
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte)1); //corrupt preamble ints, should be 5
    KllItemsSketch.heapify(wmem, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions4() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(1, (byte)0); //corrupt SerVer, should be 1 or 2
    KllItemsSketch.heapify(wmem, Comparator.naturalOrder(), serDe);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions5() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(2, (byte)0); //corrupt FamilyID, should be 15
    KllItemsSketch.heapify(wmem, Comparator.naturalOrder(), serDe);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMisc() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) {} //empty
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
    for (int i = 101; i <= n + 100; i++) { sk2.update(Util.longToFixedLengthString(i, digits)); }
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
    int k = 20;
    int n = 108;
    boolean withLevels = false;
    boolean withLevelsAndItems = true;
    int digits = Util.numDigits(n);
    int compaction = 0;
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
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
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    println(sk.toString(true, true));
    ItemsSketchSortedView<String> sv = sk.getSortedView();
    GenericSortedViewIterator<String> itr = sv.iterator();
    println("### SORTED VIEW");
    printf("%6s %12s %12s" + LS, "Idx", "Value", "CumWeight");
    int i = 0;
    while (itr.next()) {
      String v = itr.getQuantile();
      long wt = itr.getWeight();
      printf("%6d %12s %12d" + LS,i, v, wt);
      i++;
    }
    assertEquals(sv.getMinItem(), " 1");
    assertEquals(sv.getMaxItem(), Integer.toString(n));
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkWeightedUpdates1() {
    int k = 20;
    int weight = 127;
    String item = "10";
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
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
    int k = 20;
    int initial = 1000;
    final int digits = 4;
    int weight = 127;
    String item = "  10";
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= initial; i++) { sk.update(Util.longToFixedLengthString(i + 1000, digits)); }
    println(sk.toString(true, true));
    sk.update(item, weight);
    println(sk.toString(true, true));
    assertEquals(sk.getNumRetained(), 65);
    assertEquals(sk.getN(), 1127);

    GenericSortedViewIterator<String> itr = sk.getSortedView().iterator();
    println("### SORTED VIEW");
    printf("%12s %12s %12s" + LS, "Value", "Weight", "NaturalRank");
    long cumWt = 0;
    while (itr.next()) {
      String v = itr.getQuantile();
      long wt = itr.getWeight();
      long natRank = itr.getNaturalRank(INCLUSIVE);
      cumWt += wt;
      assertEquals(cumWt, natRank);
      printf("%12s %12d %12d" + LS, v, wt, natRank);
    }
    assertEquals(cumWt, sk.getN());
  }


  @Test
  public void checkGrowLevels() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final int n = 21;
    final int digits = Util.numDigits(n);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getTotalItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure)[2], 33);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkSketchInitializeItemsHeap() {
    int k = 20; //don't change this
    int n = 21;
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
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
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
  public void checkSketchInitializeItemsHeapifyCompactMem() {
    int k = 20; //don't change this
    int n = 21;
    final int digits = Util.numDigits(n);
    KllItemsSketch<String> sk;
    KllItemsSketch<String> sk2;
    byte[] compBytes;
    Memory mem;

    println("#### CASE: ITEM FULL HEAPIFIED FROM COMPACT");
    sk2 = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk2.update(Util.longToFixedLengthString(i, digits)); }
    println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    mem = Memory.wrap(compBytes);
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
    sk = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
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
    mem = Memory.wrap(compBytes);
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
    sk = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getTotalItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: ITEM SINGLE HEAPIFIED FROM COMPACT");
    sk2 = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    sk2.update("1");
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    mem = Memory.wrap(compBytes);
    println(KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe));
    sk = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
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

  //public void checkSketchInitializeItemHeapifyUpdatableMem() Not Supported

  @Test //set static enablePrinting = true for visual checking
  public void checkMemoryToStringItemsCompact() {
    int k = 20; //don't change this
    int n = 21;
    final int digits = Util.numDigits(n);
    KllItemsSketch<String> sk;
    KllItemsSketch<String> sk2;
    byte[] compBytes;
    byte[] compBytes2;
    Memory mem;
    String s;

    println("#### CASE: ITEM FULL COMPACT");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    compBytes = sk.toByteArray();
    mem = Memory.wrap(compBytes);
    s = KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
    compBytes2 = sk2.toByteArray();
    mem = Memory.wrap(compBytes2);
    s = KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: ITEM EMPTY COMPACT");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    compBytes = sk.toByteArray();
    mem = Memory.wrap(compBytes);
    s = KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
    compBytes2 = sk2.toByteArray();
    mem = Memory.wrap(compBytes2);
    s = KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: ITEM SINGLE COMPACT");
    sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    sk.update("1");
    compBytes = sk.toByteArray();
    mem = Memory.wrap(compBytes);
    s = KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllItemsSketch.heapify(mem, Comparator.naturalOrder(), serDe);
    compBytes2 = sk2.toByteArray();
    mem = Memory.wrap(compBytes2);
    s = KllPreambleUtil.toString(mem, ITEMS_SKETCH, true, serDe);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkCreateItemsArray() { //used with weighted updates
    String item = "10";
    int weight = 108;
    String[] itemsArr = KllItemsHelper.createItemsArray(String.class, item, weight);
    assertEquals(itemsArr.length, 4);
    for (int i = 0; i < itemsArr.length; i++) { itemsArr[i] = item; }
    outputItems(itemsArr);
  }

  private static void outputItems(String[] itemsArr) {
    String[] hdr2 = {"Index", "Value"};
    String hdr2fmt = "%6s %15s" + LS;
    String d2fmt = "%6d %15s" + LS;
    println("ItemsArr");
    printf(hdr2fmt, (Object[]) hdr2);
    for (int i = 0; i < itemsArr.length; i++) {
      printf(d2fmt, i, itemsArr[i]);
    }
    println("");
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkCreateLevelsArray() { //used with weighted updates
    int weight = 108;
    int[] levelsArr = KllHelper.createLevelsArray(weight);
    assertEquals(levelsArr.length, 8);
    int[] correct = {0,0,0,1,2,2,3,4};
    for (int i = 0; i < levelsArr.length; i++) {
      assertEquals(levelsArr[i], correct[i]);
    }
    outputLevels(weight, levelsArr);
  }

  private static void outputLevels(int weight, int[] levelsArr) {
    String[] hdr = {"Lvl", "StartAdr", "BitPattern", "Weight"};
    String hdrfmt = "%3s %9s %10s %s" + LS;
    String dfmt   = "%3d %9d %10d %d" + LS;
    String dfmt_2 = "%3d %9d %s" + LS;
    println("Count = " + weight + " => " + (Integer.toBinaryString(weight)));
    println("LevelsArr");
    printf(hdrfmt, (Object[]) hdr);
    for (int i = 0; i < levelsArr.length; i++) {
      if (i == levelsArr.length - 1) { printf(dfmt_2, i, levelsArr[i], "ItemsArr.length"); }
      else {
        int j = bitAt(weight, i);
        printf(dfmt, i, levelsArr[i], j, 1 << (i));
      }
    }
    println("");
  }

  @Test
  public void checkGetSingleItem() {
    int k = 20;
    KllItemsSketch<String> skHeap = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    skHeap.update("1");
    assertTrue(skHeap instanceof KllHeapItemsSketch);
    assertEquals(skHeap.getSingleItem(), "1");

    Memory srcMem = Memory.wrap(KllHelper.toByteArray(skHeap, true)); //true is ignored
    KllItemsSketch<String> skDirect = KllItemsSketch.wrap(srcMem, Comparator.naturalOrder(), serDe);
    assertTrue(skDirect instanceof KllDirectCompactItemsSketch);
    assertEquals(skDirect.getSingleItem(), "1");

    Memory srcMem2 = Memory.wrap(skHeap.toByteArray());
    KllItemsSketch<String> skCompact = KllItemsSketch.wrap(srcMem2, Comparator.naturalOrder(), serDe);
    assertTrue(skCompact instanceof KllDirectCompactItemsSketch);
    assertEquals(skCompact.getSingleItem(), "1");
  }

  @Test
  public void checkIssue484() {
    final int k = 20;
    Boolean[] items = { true,false,true,false,true,false,true,false,true,false };
    KllItemsSketch<Boolean> sketch = KllItemsSketch.newHeapInstance(k, Boolean::compareTo, new ArrayOfBooleansSerDe());
    for (int i = 0; i < items.length; i++) { sketch.update(items[i]); }
    byte[] serialized = sketch.toByteArray();
    KllItemsSketch<Boolean> deserialized =
        KllItemsSketch.wrap(Memory.wrap(serialized), Boolean::compareTo, new ArrayOfBooleansSerDe());
    checkSketchesEqual(sketch, deserialized);
  }

  private static <T> void checkSketchesEqual(KllItemsSketch<T> expected, KllItemsSketch<T> actual) {
    ItemsSketchSortedView<T> expSV = expected.getSortedView();
    ItemsSketchSortedView<T> actSV = actual.getSortedView();
    int N = (int)actSV.getN();
    long[] expCumWts = expSV.getCumulativeWeights();
    Boolean[] expItemsArr = (Boolean[])expSV.getQuantiles();
    long[] actCumWts = actSV.getCumulativeWeights();
    Boolean[] actItemsArr = (Boolean[])actSV.getQuantiles();
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
