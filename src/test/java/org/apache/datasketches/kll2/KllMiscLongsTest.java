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

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDirectLongsSketch.KllDirectCompactLongsSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.LongsSortedView;
import org.apache.datasketches.quantilescommon.LongsSortedViewIterator;
import org.testng.annotations.Test;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.bitAt;
import static org.apache.datasketches.kll.KllSketch.SketchType.LONGS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * @author Lee Rhodes
 */
public class KllMiscLongsTest {
  private final MemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void checkSortedViewConstruction() {
    final KllLongsSketch kll = KllLongsSketch.newHeapInstance(20);
    for (int i = 1; i <= 20; i++) { kll.update(i); }
    LongsSortedView fsv = kll.getSortedView();
    long[] cumWeights = fsv.getCumulativeWeights();
    long[] values = fsv.getQuantiles();
    assertEquals(cumWeights.length, 20);
    assertEquals(values.length, 20);
    for (int i = 0; i < 20; i++) {
      assertEquals(cumWeights[i], i + 1);
      assertEquals(values[i], i + 1);
    }
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkBounds() {
    final KllLongsSketch kll = KllLongsSketch.newHeapInstance(); //default k = 200
    for (int i = 0; i < 1000; i++) {
      kll.update(i);
    }
    final double eps = kll.getNormalizedRankError(false);
    final long est = kll.getQuantile(0.5);
    final long ub = kll.getQuantileUpperBound(0.5);
    final long lb = kll.getQuantileLowerBound(0.5);
    assertEquals(ub, kll.getQuantile(.5 + eps));
    assertEquals(lb, kll.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
    final double rest = kll.getRank(est);
    final double restUB = kll.getRankUpperBound(rest);
    final double restLB = kll.getRankLowerBound(rest);
    assertTrue(restUB - rest < (2 * eps));
    assertTrue(rest - restLB < (2 * eps));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions1() {
    KllLongsSketch sk = KllLongsSketch.newHeapInstance();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(6, (byte) 3); //corrupt with odd M
    KllLongsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions2() {
    KllLongsSketch sk = KllLongsSketch.newHeapInstance();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte) 1); //corrupt preamble ints, should be 2
    KllLongsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions3() {
    KllLongsSketch sk = KllLongsSketch.newHeapInstance();
    sk.update(1);
    sk.update(2);
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte) 1); //corrupt preamble ints, should be 5
    KllLongsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions4() {
    KllLongsSketch sk = KllLongsSketch.newHeapInstance();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(1, (byte) 0); //corrupt SerVer, should be 1 or 2
    KllLongsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions5() {
    KllLongsSketch sk = KllLongsSketch.newHeapInstance();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(2, (byte) 0); //corrupt FamilyID, should be 15
    KllLongsSketch.heapify(wmem);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMisc() {
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(8);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) {} //empty
    println(sk.toString(true, true));
    for (int i = 0; i < 20; i++) { sk.update(i); }
    println(sk.toString(true, true));
    sk.toByteArray();
    final long[] items = sk.getLongItemsArray();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray(sk.sketchStructure);
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  @Test //set static enablePrinting = true for visual checking
  public void visualCheckToString() {
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(20);
    int n = 21;
    for (int i = 1; i <= n; i++) { sk.update(i); }
    println(sk.toString(true, true));
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getMaxItem(), 21);
    assertEquals(sk.getNumRetained(), 11);

    final KllLongsSketch sk2 = KllLongsSketch.newHeapInstance(20);
    n = 400;
    for (int i = 101; i <= n + 100; i++) { sk2.update(i); }
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
    int k = 20;
    int n = 108;
    boolean withLevels = false;
    boolean withLevelsAndItems = true;
    int compaction = 0;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) {
      sk.update(i);
      if (sk.levelsArr[0] == 0) {
        println(LS + "#<<< BEFORE COMPACTION # " + (++compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        sk.update(++i);
        println(LS + "#<<< AFTER COMPACTION  # " + (compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        assertEquals(sk.getLongItemsArray()[sk.levelsArr[0]], i);
      }
    }
    println(LS + "#<<< END STATE # >>>");
    println(sk.toString(withLevels, withLevelsAndItems));
    println("");
  }

  @Test //set static enablePrinting = true for visual checking
  public void viewDirectCompactions() {
    int k = 20;
    int n = 108;
    boolean withLevels = false;
    boolean withLevelsAndItems = true;
    int compaction = 0;
    int sizeBytes = KllSketch.getMaxSerializedSizeBytes(k, n, LONGS_SKETCH, true);
    WritableMemory wmem = WritableMemory.allocate(sizeBytes);
    KllLongsSketch sk = KllLongsSketch.newDirectInstance(k, wmem, memReqSvr);
    for (int i = 1; i <= n; i++) {
      sk.update(i);
      if (sk.levelsArr[0] == 0) {
        println(LS + "#<<< BEFORE COMPACTION # " + (++compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        sk.update(++i);
        println(LS + "#<<< AFTER COMPACTION  # " + (compaction) + " >>>");
        println(sk.toString(withLevels, withLevelsAndItems));
        assertEquals(sk.getLongItemsArray()[sk.levelsArr[0]], i);
      }
    }
    println(LS + "#<<< END STATE # >>>");
    println(sk.toString(withLevels, withLevelsAndItems));
    println("");
  }

  @Test //set static enablePrinting = true for visual checking
  public void viewCompactionAndSortedView() {
    int n = 43;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(20);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    println(sk.toString(true, true));
    LongsSortedView sv = sk.getSortedView();
    LongsSortedViewIterator itr = sv.iterator();
    println("### SORTED VIEW");
    printf("%6s %12s %12s" + LS, "Idx", "Value", "Weight");
    int i = 0;
    while (itr.next()) {
      long v = itr.getQuantile();
      long wt = itr.getWeight();
      printf("%6d %12d %12d" + LS, i, v, wt);
      i++;
    }
    assertEquals(sv.getMinItem(), 1L);
    assertEquals(sv.getMaxItem(), n);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkWeightedUpdates1() {
    int k = 20;
    int weight = 127;
    long item = 10;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
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
    int weight = 127;
    long item = 10;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= initial; i++) { sk.update(i + 1000); }
    println(sk.toString(true, true));
    sk.update(item, weight);
    println(sk.toString(true, true));
    assertEquals(sk.getNumRetained(), 65);
    assertEquals(sk.getN(), 1127);

    LongsSortedViewIterator itr = sk.getSortedView().iterator();
    println("### SORTED VIEW");
    printf("%12s %12s %12s" + LS, "Value", "Weight", "NaturalRank");
    long cumWt = 0;
    while (itr.next()) {
      long v = itr.getQuantile();
      long wt = itr.getWeight();
      long natRank = itr.getNaturalRank(INCLUSIVE);
      cumWt += wt;
      assertEquals(cumWt, natRank);
      printf("%12d %12d %12d" + LS, v, wt, natRank);
    }
    assertEquals(cumWt, sk.getN());
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkCreateItemsArray() { //used with weighted updates
    long item = 10;
    int weight = 108;
    long[] itemsArr = KllLongsHelper.createItemsArray(item, weight);
    assertEquals(itemsArr.length, 4);
    for (int i = 0; i < itemsArr.length; i++) { itemsArr[i] = item; }
    outputItems(itemsArr);
  }

  private static void outputItems(long[] itemsArr) {
    String[] hdr2 = {"Index", "Value"};
    String hdr2fmt = "%6s %15s" + LS;
    String d2fmt = "%6d %15d" + LS;
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
  public void viewMemorySketchData() {
    int k = 20;
    int n = 109;
    boolean withLevels = true;
    boolean withLevelsAndItems = true;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    byte[] byteArr = sk.toByteArray();
    Memory mem = Memory.wrap(byteArr);
    KllLongsSketch fltSk = KllLongsSketch.wrap(mem);
    println(fltSk.toString(withLevels, withLevelsAndItems));
    assertEquals(fltSk.getN(), n);
  }

  @Test //set static enablePrinting = true for visual checking
    public void checkIntCapAux() {
      String[] hdr = {"level", "depth", "wt", "cap", "(end)", "MaxN"};
      String hdrFmt =  "%6s %6s %28s %10s %10s %34s" + LS;
      String dataFmt = "%6d %6d %,28d %,10d %,10d %,34d" + LS;
      int k = 1000;
      int m = 8;
      int numLevels = 20;
      println("k=" + k + ", m=" + m + ", numLevels=" + numLevels);
      printf(hdrFmt, (Object[]) hdr);
      long maxN = 0;
      long[] correct = {0,1,1,2,2,3,5,8,12,17,26,39,59,88,132,198,296,444,667,1000};
      for (int i = 0; i < numLevels; i++) {
        int depth = numLevels - i - 1;
        long cap = KllHelper.intCapAux(k, depth);
        long end = Math.max(m, cap);
        long wt = 1L << i;
        maxN += wt * end;
        printf(dataFmt, i, depth, wt, cap, end, maxN);
        assertEquals(cap, correct[i]);
      }
    }

  @Test //set static enablePrinting = true for visual checking
  public void checkIntCapAuxAux() {
    String[] hdr = {"d","twoK","2k*2^d","3^d","tmp=2k*2^d/3^d","(tmp + 1)/2", "(end)"};
    String hdrFmt =  "%6s %10s %20s %20s %15s %12s %10s" + LS;
    String dataFmt = "%6d %10d %,20d %,20d %15d %12d %10d" + LS;
    long k = (1L << 16) - 1L;
    long m = 8;
    println("k = " + k + ", m = " + m);
    printf(hdrFmt, (Object[]) hdr);
    long[] correct =
 {65535,43690,29127,19418,12945,8630,5753,3836,2557,1705,1136,758,505,337,224,150,100,67,44,30,20,13,9,6,4,3,2,1,1,1,0};
    for (int i = 0; i < 31; i++) {
      long twoK = k << 1;
      long twoKxtwoD = twoK << i;
      long threeToD = KllHelper.powersOfThree[i];
      long tmp = twoKxtwoD / threeToD;
      long result = (tmp + 1L) >>> 1;
      long end = Math.max(m, result); //performed later
      printf(dataFmt, i, twoK, twoKxtwoD, threeToD, tmp, result, end);
      assertEquals(result,correct[i]);
      assertEquals(result, KllHelper.intCapAuxAux(k, i));
    }
  }

  @Test
  public void checkGrowLevels() {
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getLongItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure)[2], 33);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkSketchInitializeLongHeap() {
    int k = 20; //don't change this
    KllLongsSketch sk;

    println("#### CASE: LONG FULL HEAP");
    sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), 21L);
    assertEquals(sk.getMinItem(), 1L);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG HEAP EMPTY");
    sk = KllLongsSketch.newHeapInstance(k);
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG HEAP SINGLE");
    sk = KllLongsSketch.newHeapInstance(k);
    sk.update(1);
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), 1L);
    assertEquals(sk.getMinItem(), 1L);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkSketchInitializeLongHeapifyCompactMem() {
    int k = 20; //don't change this
    KllLongsSketch sk;
    KllLongsSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    println("#### CASE: LONG FULL HEAPIFIED FROM COMPACT");
    sk2 = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem, LONGS_SKETCH, true));
    sk = KllLongsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), 21);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG EMPTY HEAPIFIED FROM COMPACT");
    sk2 = KllLongsSketch.newHeapInstance(k);
    //println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem, LONGS_SKETCH, true));
    sk = KllLongsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG SINGLE HEAPIFIED FROM COMPACT");
    sk2 = KllLongsSketch.newHeapInstance(k);
    sk2.update(1);
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem, LONGS_SKETCH, true));
    sk = KllLongsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), 1L);
    assertEquals(sk.getMinItem(), 1L);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkSketchInitializeLongHeapifyUpdatableMem() {
    int k = 20; //don't change this
    KllLongsSketch sk;
    KllLongsSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    println("#### CASE: LONG FULL HEAPIFIED FROM UPDATABLE");
    sk2 = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem, LONGS_SKETCH, true));
    sk = KllHeapLongsSketch.heapifyImpl(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 33);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 3);
    assertEquals(sk.getMaxItem(), 21);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = KllLongsSketch.newHeapInstance(k);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem, LONGS_SKETCH, true));
    sk = KllHeapLongsSketch.heapifyImpl(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: LONG SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = KllLongsSketch.newHeapInstance(k);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem, LONGS_SKETCH, true));
    sk = KllHeapLongsSketch.heapifyImpl(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getLongItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    assertEquals(sk.getMaxItem(), 1L);
    assertEquals(sk.getMinItem(), 1L);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMemoryToStringLongCompact() {
    int k = 20; //don't change this
    KllLongsSketch sk;
    KllLongsSketch sk2;
    byte[] compBytes;
    byte[] compBytes2;
    WritableMemory wmem;
    String s;

    println("#### CASE: LONG FULL COMPACT");
    sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllLongsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: LONG EMPTY COMPACT");
    sk = KllLongsSketch.newHeapInstance(k);
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllLongsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: LONG SINGLE COMPACT");
    sk = KllLongsSketch.newHeapInstance(k);
    sk.update(1);
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllLongsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);
  }

  @Test //set static enablePrinting = true for visual checking
  public void checkMemoryToStringLongUpdatable() {
    int k = 20; //don't change this
    KllLongsSketch sk;
    KllLongsSketch sk2;
    byte[] upBytes;
    byte[] upBytes2;
    WritableMemory wmem;
    String s;

    println("#### CASE: LONG FULL UPDATABLE");
    sk = KllLongsSketch.newHeapInstance(20);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    upBytes = KllHelper.toByteArray(sk, true);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllHeapLongsSketch.heapifyImpl(wmem);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s); //note: heapify does not copy free space, while toUpdatableByteArray does
    assertEquals(sk.getN(), sk2.getN());
    assertEquals(sk.getMinItem(), sk2.getMinItem());
    assertEquals(sk.getMaxItem(), sk2.getMaxItem());
    assertEquals(sk.getNumRetained(), sk2.getNumRetained());

    println("#### CASE: LONG EMPTY UPDATABLE");
    sk = KllLongsSketch.newHeapInstance(k);
    upBytes = KllHelper.toByteArray(sk, true);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllHeapLongsSketch.heapifyImpl(wmem);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: LONG SINGLE UPDATABLE");
    sk = KllLongsSketch.newHeapInstance(k);
    sk.update(1);
    upBytes = KllHelper.toByteArray(sk, true);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllHeapLongsSketch.heapifyImpl(wmem);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, LONGS_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);
  }

  @Test
  public void checkSimpleMerge() {
    int k = 20;
    int m = 8;
    int n1 = 21;
    int n2 = 43;
    WritableMemory wmem = WritableMemory.allocate(3000);
    WritableMemory wmem2 = WritableMemory.allocate(3000);

    KllLongsSketch sk1 = KllDirectLongsSketch.newDirectUpdatableInstance(k, m, wmem, memReqSvr);
    KllLongsSketch sk2 = KllDirectLongsSketch.newDirectUpdatableInstance(k, m, wmem2, memReqSvr);
    for (int i = 1; i <= n1; i++) {
      sk1.update(i);
    }
    for (int i = 1; i <= n2; i++) {
      sk2.update(i + 100);
    }
    sk1.merge(sk2);
    assertEquals(sk1.getMinItem(), 1L);
    assertEquals(sk1.getMaxItem(), 143L);
  }

  @Test
  public void checkGetSingleItem() {
    int k = 20;
    KllLongsSketch skHeap = KllLongsSketch.newHeapInstance(k);
    skHeap.update(1);
    assertTrue(skHeap instanceof KllHeapLongsSketch);
    assertEquals(skHeap.getLongSingleItem(), 1L);

    WritableMemory srcMem = WritableMemory.writableWrap(KllHelper.toByteArray(skHeap, true));
    KllLongsSketch skDirect = KllLongsSketch.writableWrap(srcMem, memReqSvr);
    assertTrue(skDirect instanceof KllDirectLongsSketch);
    assertEquals(skDirect.getLongSingleItem(), 1L);

    Memory srcMem2 = Memory.wrap(skHeap.toByteArray());
    KllLongsSketch skCompact = KllLongsSketch.wrap(srcMem2);
    assertTrue(skCompact instanceof KllDirectCompactLongsSketch);
    assertEquals(skCompact.getLongSingleItem(), 1L);
  }

  @Test
  public void printlnTest() {
    String s = "PRINTING:  printf in " + this.getClass().getName();
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
