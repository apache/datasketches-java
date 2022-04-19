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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Objects;

import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class MiscDirectDoublesTest {
  static final String LS = System.getProperty("line.separator");
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void checkBounds() {
    final KllDoublesSketch sk = getDDSketch(200, 0);
    for (int i = 0; i < 1000; i++) {
      sk.update(i);
    }
    final double eps = sk.getNormalizedRankError(false);
    final double est = sk.getQuantile(0.5);
    final double ub = sk.getQuantileUpperBound(0.5);
    final double lb = sk.getQuantileLowerBound(0.5);
    assertEquals(ub, sk.getQuantile(.5 + eps));
    assertEquals(lb, sk.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
  }

  @Test
  public void checkMisc() {
    final KllDoublesSketch sk = getDDSketch(8, 0);
    assertTrue(Objects.isNull(sk.getQuantiles(10)));
    //sk.toString(true, true);
    for (int i = 0; i < 20; i++) { sk.update(i); }
    //sk.toString(true, true);
    //sk.toByteArray();
    final double[] items = sk.getDoubleItemsArray();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray();
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  //@Test //enable static println(..) for visual checking
  public void visualCheckToString() {
    final KllDoublesSketch sk = getDDSketch(20, 0);
    for (int i = 0; i < 10; i++) { sk.update(i + 1); }
    println(sk.toString(true, true));

    final KllDoublesSketch sk2 = getDDSketch(20, 0);
    for (int i = 0; i < 400; i++) { sk2.update(i + 1); }
    println("\n" + sk2.toString(true, true));

    sk2.merge(sk);
    final String s2 = sk2.toString(true, true);
    println(LS + s2);
  }

  //@Test
  public void viewCompactions() {
    final KllDoublesSketch sk = getDDSketch(20, 0);
    show(sk, 20);
    show(sk, 21); //compaction 1
    show(sk, 43);
    show(sk, 44); //compaction 2
    show(sk, 54);
    show(sk, 55); //compaction 3
    show(sk, 73);
    show(sk, 74); //compaction 4
    show(sk, 88);
    show(sk, 89); //compaction 5
    show(sk, 96);
    show(sk, 97); //compaction 6
    show(sk, 108);
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
    sk = getDDSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 33);
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxDoubleValue(), 21.0);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE HEAP EMPTY");
    sk = getDDSketch(k, 0);
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), Double.NaN);
    assertEquals(sk.getMinDoubleValue(), Double.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE HEAP SINGLE");
    sk = getDDSketch(k, 0);
    sk.update(1);
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), 1.0);
    assertEquals(sk.getMinDoubleValue(), 1.0);
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
    sk2 = getDDSketch(k, 0);
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
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxDoubleValue(), 21.0);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM COMPACT");
    sk2 = getDDSketch(k, 0);
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
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), Double.NaN);
    assertEquals(sk.getMinDoubleValue(), Double.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM COMPACT");
    sk2 = getDDSketch(k, 0);
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
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), 1.0);
    assertEquals(sk.getMinDoubleValue(), 1.0);
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
    sk2 = getDDSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = KllHelper.toUpdatableByteArrayImpl(sk2);
    wmem = WritableMemory.writableWrap(compBytes);
    sk = KllHeapDoublesSketch.heapifyImpl(wmem);
    println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getMinK(), k);
    assertEquals(sk.getDoubleItemsArray().length, 33);
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxDoubleValue(), 21.0);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

   // println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = getDDSketch(k, 0);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toUpdatableByteArrayImpl(sk2);
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
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), Double.NaN);
    assertEquals(sk.getMinDoubleValue(), Double.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = getDDSketch(k, 0);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = KllHelper.toUpdatableByteArrayImpl(sk2);
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
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), 1.0);
    assertEquals(sk.getMinDoubleValue(), 1.0);
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
    sk = getDDSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    upBytes = KllHelper.toUpdatableByteArrayImpl(sk);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    upBytes2 = KllHelper.toUpdatableByteArrayImpl(sk2);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: DOUBLE EMPTY UPDATABLE");
    sk = getDDSketch(k, 0);
    upBytes = KllHelper.toUpdatableByteArrayImpl(sk);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    upBytes2 = KllHelper.toUpdatableByteArrayImpl(sk2);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: DOUBLE SINGLE UPDATABL");
    sk = getDDSketch(k, 0);
    sk.update(1);
    upBytes = KllHelper.toUpdatableByteArrayImpl(sk);
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.toString(wmem, true);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    upBytes2 = KllHelper.toUpdatableByteArrayImpl(sk2);
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.toString(wmem, true);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);
  }

  @Test
  public void checkSimpleMerge() {
    int k = 20;
    int n1 = 21;
    int n2 = 21;
    KllDoublesSketch sk1 = getDDSketch(k, 0);
    KllDoublesSketch sk2 = getDDSketch(k, 0);
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
    assertEquals(sk1.getMaxValue(), 121.0);
    assertEquals(sk1.getMinValue(), 1.0);
  }

  @Test
  public void checkSizes() {
    KllDoublesSketch sk = getDDSketch(20, 0);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    //println(sk.toString(true, true));
    byte[] byteArr1 = KllHelper.toUpdatableByteArrayImpl(sk);
    int size1 = sk.getCurrentUpdatableSerializedSizeBytes();
    assertEquals(size1, byteArr1.length);
    byte[] byteArr2 = sk.toByteArray();
    int size2 = sk.getCurrentCompactSerializedSizeBytes();
    assertEquals(size2, byteArr2.length);
  }

  @Test
  public void checkNewInstance() {
    int k = 200;
    WritableMemory dstMem = WritableMemory.allocate(6000);
    KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(k, dstMem, memReqSvr);
    for (int i = 1; i <= 10_000; i++) {sk.update(i); }
    assertEquals(sk.getMinValue(), 1.0);
    assertEquals(sk.getMaxValue(), 10000.0);
    //println(sk.toString(true, true));
  }

  @Test
  public void checkDifferentM() {
    int k = 20;
    int m = 4;
    WritableMemory dstMem = WritableMemory.allocate(1000);
    KllDoublesSketch sk = KllDirectDoublesSketch.newDirectInstance(k, m, dstMem, memReqSvr);
    for (int i = 1; i <= 200; i++) {sk.update(i); }
    assertEquals(sk.getMinValue(), 1.0);
    assertEquals(sk.getMaxValue(), 200.0);
  }

  private static KllDoublesSketch getDDSketch(final int k, final int n) {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    byte[] byteArr = KllHelper.toUpdatableByteArrayImpl(sk);
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    KllDoublesSketch ddsk = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    return ddsk;
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
