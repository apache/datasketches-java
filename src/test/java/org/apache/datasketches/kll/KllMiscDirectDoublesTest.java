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

import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.DoublesSortedViewIterator;
import org.testng.annotations.Test;

public class KllMiscDirectDoublesTest {
  static final String LS = System.getProperty("line.separator");
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void checkBounds() {
    final KllDoublesSketch kll = getDirectDoublesSketch(200, 0);
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
    assertTrue(restUB - rest < (2 * eps));
    assertTrue(rest - restLB < (2 * eps));
  }

  @Test
  public void checkMisc() {
    final int k = 8;
    final KllDoublesSketch sk = getDirectDoublesSketch(k, 0);
    try { sk.getPartitionBoundaries(10); fail(); } catch (SketchesArgumentException e) {}
    for (int i = 0; i < 20; i++) { sk.update(i); }
    final double[] items = sk.getDoubleItemsArray();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray(sk.sketchStructure);
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  //@Test //enable static println(..) for visual checking
  public void visualCheckToString() {
    final int k = 20;
    final KllDoublesSketch sk = getDirectDoublesSketch(k, 0);
    for (int i = 0; i < 10; i++) { sk.update(i + 1); }
    println(sk.toString(true, true));

    final KllDoublesSketch sk2 = getDirectDoublesSketch(k, 0);
    for (int i = 0; i < 400; i++) { sk2.update(i + 1); }
    println("\n" + sk2.toString(true, true));

    sk2.merge(sk);
    final String s2 = sk2.toString(true, true);
    println(LS + s2);
  }

  @Test
  public void viewDirectCompactions() {
    int k = 20;
    int u = 108;
    KllDoublesSketch sk = getDirectDoublesSketch(k, 0);
    for (int i = 1; i <= u; i++) {
      sk.update(i);
      if (sk.levelsArr[0] == 0) {
        println(sk.toString(true, true));
        sk.update(++i);
        println(sk.toString(true, true));
        assertEquals(sk.getDoubleItemsArray()[sk.levelsArr[0]], i);
      }
    }
  }

  @Test
  public void viewCompactionAndSortedView() {
    int k = 20;
    KllDoublesSketch sk = getDirectDoublesSketch(k, 0);
    show(sk, 20);
    DoublesSortedView sv = sk.getSortedView();
    DoublesSortedViewIterator itr = sv.iterator();
    printf("%12s%12s\n", "Value", "CumWeight");
    while (itr.next()) {
      double v = itr.getQuantile();
      long wt = itr.getWeight();
      printf("%12.1f%12d\n", v, wt);
    }
  }

  private static void show(final KllDoublesSketch sk, int limit) {
    int i = (int) sk.getN();
    for ( ; i < limit; i++) { sk.update(i + 1); }
    println(sk.toString(true, true));
  }

  @Test
  public void checkSketchInitializeDoubleHeap() {
    int k = 20; //don't change this
    KllDoublesSketch sk;

    //println("#### CASE: DOUBLE FULL HEAP");
    sk = getDirectDoublesSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    //println(sk.toString(true, true));
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

    //println("#### CASE: DOUBLE HEAP EMPTY");
    sk = getDirectDoublesSketch(k, 0);
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE HEAP SINGLE");
    sk = getDirectDoublesSketch(k, 0);
    sk.update(1);
    //println(sk.toString(true, true));
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

  @Test
  public void checkSketchInitializeDoubleHeapifyCompactMem() {
    int k = 20; //don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    //println("#### CASE: DOUBLE FULL HEAPIFIED FROM COMPACT");
    sk2 = getDirectDoublesSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllDoublesSketch.heapify(wmem);
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

    //println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM COMPACT");
    sk2 = getDirectDoublesSketch(k, 0);
    //println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllDoublesSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM COMPACT");
    sk2 = getDirectDoublesSketch(k, 0);
    sk2.update(1);
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllDoublesSketch.heapify(wmem);
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

  @Test
  public void checkSketchInitializeDoubleHeapifyUpdatableMem() {
    int k = 20; //don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    //println("#### CASE: DOUBLE FULL HEAPIFIED FROM UPDATABLE");
    sk2 = getDirectDoublesSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    sk = KllHeapDoublesSketch.heapifyImpl(wmem);
    //println(sk.toString(true, true));
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

   // println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = getDirectDoublesSketch(k, 0);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllHeapDoublesSketch.heapifyImpl(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray(sk.sketchStructure).length, 2);
    try { sk.getMaxItem(); fail(); } catch (SketchesArgumentException e) { }
    try { sk.getMinItem(); fail(); } catch (SketchesArgumentException e) { }
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = getDirectDoublesSketch(k, 0);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.memoryToString(wmem, true));
    sk = KllHeapDoublesSketch.heapifyImpl(wmem);
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

  @Test
  public void checkMemoryToStringDoubleUpdatable() {
    int k = 20; //don't change this
    KllDoublesSketch sk;
    KllDoublesSketch sk2;
    byte[] upBytes;
    byte[] upBytes2;
    WritableMemory wmem;
    String s;

    println("#### CASE: DOUBLE FULL UPDATABLE");
    sk = getDirectDoublesSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    upBytes = KllHelper.toByteArray(sk, true);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, DOUBLES_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: DOUBLE EMPTY UPDATABLE");
    sk = getDirectDoublesSketch(k, 0);
    upBytes = KllHelper.toByteArray(sk, true);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, DOUBLES_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: DOUBLE SINGLE UPDATABL");
    sk = getDirectDoublesSketch(k, 0);
    sk.update(1);
    upBytes = KllHelper.toByteArray(sk, true);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, DOUBLES_SKETCH, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    upBytes2 = KllHelper.toByteArray(sk2, true);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, DOUBLES_SKETCH, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);
  }

  @Test
  public void checkSimpleMerge() {
    int k = 20;
    int n1 = 21;
    int n2 = 21;
    KllDoublesSketch sk1 = getDirectDoublesSketch(k, 0);
    KllDoublesSketch sk2 = getDirectDoublesSketch(k, 0);
    for (int i = 1; i <= n1; i++) {
      sk1.update(i);
    }
    for (int i = 1; i <= n2; i++) {
      sk2.update(i + 100);
    }
    println(sk1.toString(true, true));
    println(sk2.toString(true, true));
    sk1.merge(sk2);
    println(sk1.toString(true, true));
    assertEquals(sk1.getMaxItem(), 121.0);
    assertEquals(sk1.getMinItem(), 1.0);
  }

  @Test
  public void checkSizes() {
    KllDoublesSketch sk = getDirectDoublesSketch(20, 0);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    //println(sk.toString(true, true));
    byte[] byteArr1 = KllHelper.toByteArray(sk, true);
    int size1 = sk.currentSerializedSizeBytes(true);
    assertEquals(size1, byteArr1.length);
    byte[] byteArr2 = sk.toByteArray();
    int size2 = sk.currentSerializedSizeBytes(false);
    assertEquals(size2, byteArr2.length);
  }

  @Test
  public void checkNewInstance() {
    int k = 200;
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(k, dstMem, memReqSvr);
    for (int i = 1; i <= 10_000; i++) {sk.update(i); }
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getMaxItem(), 10000.0);
    //println(sk.toString(true, true));
  }

  @Test
  public void checkDifferentM() {
    int k = 20;
    int m = 4;
    WritableMemory dstMem = WritableMemory.allocate(1000);
    KllDoublesSketch sk = KllDirectDoublesSketch.newDirectUpdatableInstance(k, m, dstMem, memReqSvr);
    for (int i = 1; i <= 200; i++) {sk.update(i); }
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getMaxItem(), 200.0);
  }

  private static KllDoublesSketch getDirectDoublesSketch(final int k, final int n) {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    byte[] byteArr = KllHelper.toByteArray(sk, true);
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    KllDoublesSketch ddsk = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    return ddsk;
  }

  @Test
  public void printlnTest() {
    String s = "PRINTING:  printf in " + this.getClass().getName();
    println(s);
    printf("%s\n", s);
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
