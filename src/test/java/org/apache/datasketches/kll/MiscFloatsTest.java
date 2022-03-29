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

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class MiscFloatsTest {
  static final String LS = System.getProperty("line.separator");

  @Test
  public void checkBounds() {
    final KllFloatsSketch kll = new KllFloatsSketch(); //default k = 200
    for (int i = 0; i < 1000; i++) {
      kll.update(i);
    }
    final double eps = kll.getNormalizedRankError(false);
    final float est = kll.getQuantile(0.5);
    final float ub = kll.getQuantileUpperBound(0.5);
    final float lb = kll.getQuantileLowerBound(0.5);
    assertEquals(ub, kll.getQuantile(.5 + eps));
    assertEquals(lb, kll.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions1() {
    KllFloatsSketch sk = new KllFloatsSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(6, (byte)3); //corrupt with odd M
    KllFloatsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions2() {
    KllFloatsSketch sk = new KllFloatsSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte)1); //corrupt preamble ints, should be 2
    KllFloatsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions3() {
    KllFloatsSketch sk = new KllFloatsSketch();
    sk.update(1.0f);
    sk.update(2.0f);
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte)1); //corrupt preamble ints, should be 5
    KllFloatsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions4() {
    KllFloatsSketch sk = new KllFloatsSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(1, (byte)0); //corrupt SerVer, should be 1 or 2
    KllFloatsSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions5() {
    KllFloatsSketch sk = new KllFloatsSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(2, (byte)0); //corrupt FamilyID, should be 15
    KllFloatsSketch.heapify(wmem);
  }

  @Test
  public void checkMisc() {
    KllFloatsSketch sk = new KllFloatsSketch(8);
    assertTrue(Objects.isNull(sk.getQuantiles(10)));
    sk.toString(true, true);
    for (int i = 0; i < 20; i++) { sk.update(i); }
    sk.toString(true, true);
    sk.toByteArray();
    final float[] items = sk.getFloatItemsArray();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray();
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  //@Test //enable static println(..) for visual checking
  public void visualCheckToString() {
    final KllFloatsSketch sketch = new KllFloatsSketch(20);
    for (int i = 0; i < 10; i++) { sketch.update(i + 1); }
    final String s1 = sketch.toString(true, true);
    println(s1);

    final KllFloatsSketch sketch2 = new KllFloatsSketch(20);
    for (int i = 0; i < 400; i++) { sketch2.update(i + 1); }
    println("\n" + sketch2.toString(true, true));

    sketch2.merge(sketch);
    final String s2 = sketch2.toString(true, true);
    println(LS + s2);
  }

  @Test
  public void viewCompactions() {
    KllFloatsSketch sk = new KllFloatsSketch(20);
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

  private static void show(final KllFloatsSketch sk, int limit) {
    int i = (int) sk.getN();
    for ( ; i < limit; i++) { sk.update(i + 1); }
    println(sk.toString(true, true));
  }

  @Test
  public void checkGrowLevels() {
    KllFloatsSketch sk = new KllFloatsSketch(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    assertEquals(sk.getNumLevels(), 2);
    assertEquals(sk.getFloatItemsArray().length, 33);
    assertEquals(sk.getLevelsArray()[2], 33);
  }

  @Test
  public void checkSketchInitializeFloatHeap() {
    int k = 20; //don't change this
    KllFloatsSketch sk;

    println("#### CASE: FLOAT FULL HEAP");
    sk = new KllFloatsSketch(k);
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
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxDoubleValue(), 21.0);
    assertEquals(sk.getMaxFloatValue(), 21.0F);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: FLOAT HEAP EMPTY");
    sk = new KllFloatsSketch(k);
    //println(sk.toString(true, true));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), Double.NaN);
    assertEquals(sk.getMaxFloatValue(), Float.NaN);
    assertEquals(sk.getMinDoubleValue(), Double.NaN);
    assertEquals(sk.getMinFloatValue(), Float.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: FLOAT HEAP SINGLE");
    sk = new KllFloatsSketch(k);
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
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), 1.0);
    assertEquals(sk.getMaxFloatValue(), 1.0F);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkSketchInitializeFloatHeapifyCompactMem() {
    int k = 20; //don't change this
    KllFloatsSketch sk;
    KllFloatsSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    println("#### CASE: FLOAT FULL HEAPIFIED FROM COMPACT");
    sk2 = new KllFloatsSketch(k);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 33);
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxDoubleValue(), 21.0);
    assertEquals(sk.getMaxFloatValue(), 21.0F);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: FLOAT EMPTY HEAPIFIED FROM COMPACT");
    sk2 = new KllFloatsSketch(k);
    //println(sk.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), Double.NaN);
    assertEquals(sk.getMaxFloatValue(), Float.NaN);
    assertEquals(sk.getMinDoubleValue(), Double.NaN);
    assertEquals(sk.getMinFloatValue(), Float.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: FLOAT SINGLE HEAPIFIED FROM COMPACT");
    sk2 = new KllFloatsSketch(k);
    sk2.update(1);
    //println(sk2.toString(true, true));
    compBytes = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), 1.0);
    assertEquals(sk.getMaxFloatValue(), 1.0F);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkSketchInitializeFloatHeapifyUpdatableMem() {
    int k = 20; //don't change this
    KllFloatsSketch sk;
    KllFloatsSketch sk2;
    byte[] compBytes;
    WritableMemory wmem;

    println("#### CASE: FLOAT FULL HEAPIFIED FROM UPDATABLE");
    sk2 = new KllFloatsSketch(k);
    for (int i = 1; i <= k + 1; i++) { sk2.update(i); }
    //println(sk2.toString(true, true));
    compBytes = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), k + 1);
    assertEquals(sk.getNumRetained(), 11);
    assertFalse(sk.isEmpty());
    assertTrue(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 33);
    assertEquals(sk.getLevelsArray().length, 3);
    assertEquals(sk.getMaxDoubleValue(), 21.0);
    assertEquals(sk.getMaxFloatValue(), 21.0F);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 2);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: FLOAT EMPTY HEAPIFIED FROM UPDATABLE");
    sk2 = new KllFloatsSketch(k);
    //println(sk.toString(true, true));
    compBytes = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertTrue(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), Double.NaN);
    assertEquals(sk.getMaxFloatValue(), Float.NaN);
    assertEquals(sk.getMinDoubleValue(), Double.NaN);
    assertEquals(sk.getMinFloatValue(), Float.NaN);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());

    println("#### CASE: FLOAT SINGLE HEAPIFIED FROM UPDATABLE");
    sk2 = new KllFloatsSketch(k);
    sk2.update(1);
    //println(sk.toString(true, true));
    compBytes = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    println(KllPreambleUtil.toString(wmem));
    sk = KllFloatsSketch.heapify(wmem);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getDyMinK(), k);
    assertTrue(Objects.isNull(sk.getDoubleItemsArray()));
    assertEquals(sk.getFloatItemsArray().length, 20);
    assertEquals(sk.getLevelsArray().length, 2);
    assertEquals(sk.getMaxDoubleValue(), 1.0);
    assertEquals(sk.getMaxFloatValue(), 1.0F);
    assertEquals(sk.getMinDoubleValue(), 1.0);
    assertEquals(sk.getMinFloatValue(), 1.0F);
    assertEquals(sk.getNumLevels(), 1);
    assertFalse(sk.isLevelZeroSorted());
  }

  @Test
  public void checkMemoryToStringFloatCompact() {
    int k = 20; //don't change this
    KllFloatsSketch sk;
    KllFloatsSketch sk2;
    byte[] compBytes;
    byte[] compBytes2;
    WritableMemory wmem;
    String s;

    println("#### CASE: FLOAT FULL COMPACT");
    sk = new KllFloatsSketch(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: FLOAT EMPTY COMPACT");
    sk = new KllFloatsSketch(k);
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);

    println("#### CASE: FLOAT SINGLE COMPACT");
    sk = new KllFloatsSketch(k);
    sk.update(1);
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(compBytes, compBytes2);
  }

  @Test
  public void checkMemoryToStringFloatUpdatable() {
    int k = 20; //don't change this
    KllFloatsSketch sk;
    KllFloatsSketch sk2;
    byte[] upBytes;
    byte[] upBytes2;
    WritableMemory wmem;
    String s;

    println("#### CASE: FLOAT FULL UPDATABLE");
    sk = new KllFloatsSketch(20);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: FLOAT EMPTY UPDATABLE");
    sk = new KllFloatsSketch(k);
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 2: memory to heap sketch, to byte[]/memory & analyze memory. Should match above");
    println(s);
    assertEquals(upBytes, upBytes2);

    println("#### CASE: FLOAT SINGLE UPDATABLE");
    sk = new KllFloatsSketch(k);
    sk.update(1);
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println("step 1: sketch to byte[]/memory & analyze memory");
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
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
    int m = 4;
    int n1 = 21;
    int n2 = 43;
    KllFloatsSketch sk1 = new KllFloatsSketch(k, m);
    KllFloatsSketch sk2 = new KllFloatsSketch(k, m);
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
  }

  @Test
  public void checkOtherM() {

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
