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

public class MiscDirectFloatsTest {
  static final String LS = System.getProperty("line.separator");
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void checkBounds() {
    final KllDirectFloatsSketch sk = getDFSketch(200, 0);
    for (int i = 0; i < 1000; i++) {
      sk.update(i);
    }
    final double eps = sk.getNormalizedRankError(false);
    final float est = sk.getQuantile(0.5);
    final float ub = sk.getQuantileUpperBound(0.5);
    final float lb = sk.getQuantileLowerBound(0.5);
    assertEquals(ub, sk.getQuantile(.5 + eps));
    assertEquals(lb, sk.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
  }

  @Test
  public void checkMisc() {
    final KllDirectFloatsSketch sk = getDFSketch(8, 0);
    assertTrue(Objects.isNull(sk.getQuantiles(10)));
    //sk.toString(true, true);
    for (int i = 0; i < 20; i++) { sk.update(i); }
    //sk.toString(true, true);
    //sk.toByteArray();
    final float[] items = sk.getFloatItemsArray();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray();
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  //@Test //enable static println(..) for visual checking
  public void visualCheckToString() {
    final KllDirectFloatsSketch sk = getDFSketch(20, 0);
    for (int i = 0; i < 10; i++) { sk.update(i + 1); }
    println(sk.toString(true, true));

    final KllDirectFloatsSketch sk2 = getDFSketch(20, 0);
    for (int i = 0; i < 400; i++) { sk2.update(i + 1); }
    println("\n" + sk2.toString(true, true));

    sk2.merge(sk);
    final String s2 = sk2.toString(true, true);
    println(LS + s2);
  }

  //@Test
  public void viewCompactions() {
    final KllDirectFloatsSketch sk = getDFSketch(20, 0);
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

  private static void show(final KllDirectFloatsSketch sk, int limit) {
    int i = (int) sk.getN();
    for ( ; i < limit; i++) { sk.update(i + 1); }
    println(sk.toString(true, true));
  }

  @Test
  public void checkSketchInitializeFloatHeap() {
    int k = 20; //don't change this
    KllDirectFloatsSketch sk;

    //println("#### CASE: DOUBLE FULL HEAP");
    sk = getDFSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 33);
    assertEquals(sk.getLayout(), "FLOAT_UPDATABLE");
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxFloatValue(), 21.0F);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE HEAP EMPTY");
    sk = getDFSketch(k, 0);
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLayout(), "FLOAT_UPDATABLE");
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), Float.NaN);
    assertEquals(sk.getMinFloatValue(), Float.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE HEAP SINGLE");
    sk = getDFSketch(k, 0);
    sk.update(1);
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLayout(), "FLOAT_UPDATABLE");
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), 1.0F);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkSketchInitializeFloatHeapifyCompactMem() {
    int k = 20; //don't change this
    KllFloatsSketch sk;
    KllDirectFloatsSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    //println("#### CASE: DOUBLE FULL HEAPIFIED FROM COMPACT");
    sk2 = getDFSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 33);
    assertEquals(sk.getLayout(), "HEAP");
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxFloatValue(), 21.0F);
    assertEquals(sk.getMinFloatValue(), 1.0f);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM COMPACT");
    sk2 = getDFSketch(k, 0);
    //println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLayout(), "HEAP");
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), Float.NaN);
    assertEquals(sk.getMinFloatValue(), Float.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM COMPACT");
    sk2 = getDFSketch(k, 0);
    sk2.update(1);
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLayout(), "HEAP");
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), 1.0F);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkSketchInitializeFloatHeapifyUpdatableMem() {
    int k = 20; //don't change this
    KllFloatsSketch sk;
    KllDirectFloatsSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    //println("#### CASE: DOUBLE FULL HEAPIFIED FROM UPDATABLE");
    sk2 = getDFSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 33);
    assertEquals(sk.getLayout(), "HEAP");
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxFloatValue(), 21.0F);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

   // println("#### CASE: DOUBLE EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = getDFSketch(k, 0);
    //println(sk.toString(true, true));
    compBytes = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLayout(), "HEAP");
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), Float.NaN);
    assertEquals(sk.getMinFloatValue(), Float.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    //println("#### CASE: DOUBLE SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = getDFSketch(k, 0);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    //println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLayout(), "HEAP");
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxFloatValue(), 1.0F);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkMemoryToStringFloatUpdatable() {
    int k = 20; //don't change this
    KllDirectFloatsSketch sk;
    KllDirectFloatsSketch sk2;
    byte[] upBytes;
    byte[] upBytes2;
    WritableMemory wmem;
    String s;

    println("#### CASE: DOUBLE FULL UPDATABLE");
    sk = getDFSketch(k, 0);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = new KllDirectFloatsSketch(wmem, memReqSvr);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: DOUBLE EMPTY UPDATABLE");
    sk = getDFSketch(k, 0);
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = new KllDirectFloatsSketch(wmem, memReqSvr);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: DOUBLE SINGLE UPDATABL");
    sk = getDFSketch(k, 0);
    sk.update(1);
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = new KllDirectFloatsSketch(wmem, memReqSvr);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);
  }

  @Test
  public void checkSimpleMerge() {
    int k = 20;
    int n1 = 21;
    int n2 = 21;
    KllDirectFloatsSketch sk1 = getDFSketch(k, 0);
    KllDirectFloatsSketch sk2 = getDFSketch(k, 0);
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
    assertEquals(sk1.getMaxValue(), 121.0F);
    assertEquals(sk1.getMinValue(), 1.0F);
  }

  @Test
  public void checkSizes() {
    KllDirectFloatsSketch sk = getDFSketch(20, 0);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    //println(sk.toString(true, true));
    byte[] byteArr1 = sk.toUpdatableByteArray();
    int size1 = sk.getCurrentUpdatableSerializedSizeBytes();
    assertEquals(size1, byteArr1.length);
    byte[] byteArr2 = sk.toByteArray();
    int size2 = sk.getCurrentCompactSerializedSizeBytes();
    assertEquals(size2, byteArr2.length);
  }

  private static KllDirectFloatsSketch getDFSketch(final int k, final int n) {
    KllFloatsSketch sk = new KllFloatsSketch(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    byte[] byteArr = sk.toUpdatableByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    KllDirectFloatsSketch dfsk = new KllDirectFloatsSketch(wmem, memReqSvr);
    return dfsk;
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
