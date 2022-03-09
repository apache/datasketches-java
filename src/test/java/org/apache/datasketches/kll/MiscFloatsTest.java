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

import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_K;
import static org.testng.Assert.assertEquals;
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
  public void checkGetKFromEps() {
    final int k = DEFAULT_K;
    final double eps = KllHelper.getNormalizedRankError(k, false);
    final double epsPmf = KllHelper.getNormalizedRankError(k, true);
    final int kEps = KllSketch.getKFromEpsilon(eps, false);
    final int kEpsPmf = KllSketch.getKFromEpsilon(epsPmf, true);
    assertEquals(kEps, k);
    assertEquals(kEpsPmf, k);
  }

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
    wmem.putByte(6, (byte)4); //corrupt with different M
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
    final float[] items = sk.getItems();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray();
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  @Test //enable static println(..) for visual checking
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
  public void checkMemoryToStringFloatCompact() {
    KllFloatsSketch sk = new KllFloatsSketch(20);
    KllFloatsSketch sk2;
    byte[] compBytes;
    byte[] compBytes2;
    WritableMemory wmem;
    String s;

    for (int i = 1; i <= 21; i++) { sk.update(i); }
    println(sk.toString(true, true));

    println("CASE 0: FLOAT_FULL_COMPACT");
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    assertEquals(compBytes, compBytes2);

    println("CASE 1: FLOAT_EMPTY_COMPACT");
    sk = new KllFloatsSketch(20);
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    assertEquals(compBytes, compBytes2);

    println("CASE 4: FLOAT_SINGLE_COMPACT");
    sk = new KllFloatsSketch(20);
    sk.update(1);
    compBytes = sk.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    compBytes2 = sk2.toByteArray();
    wmem = WritableMemory.writableWrap(compBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    assertEquals(compBytes, compBytes2);
  }

  @Test
  public void checkMemoryToStringFloatUpdatable() {
    KllFloatsSketch sk = new KllFloatsSketch(20);
    KllFloatsSketch sk2;
    byte[] upBytes;
    byte[] upBytes2;
    WritableMemory wmem;
    String s;

    for (int i = 1; i <= 21; i++) { sk.update(i); }
    println(sk.toString(true, true));

    println("CASE 0: FLOAT_UPDATABLE");
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    assertEquals(upBytes, upBytes2);

    println("CASE 1: FLOAT_UPDATABLE (empty)");
    sk = new KllFloatsSketch(20);
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    assertEquals(upBytes, upBytes2);

    println("CASE 4: FLOAT_UPDATABLE (single)");
    sk = new KllFloatsSketch(20);
    sk.update(1);
    upBytes = sk.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    sk2 = KllFloatsSketch.heapify(wmem);
    upBytes2 = sk2.toUpdatableByteArray();
    wmem = WritableMemory.writableWrap(upBytes2);
    s = KllPreambleUtil.memoryToString(wmem);
    println(s);
    assertEquals(upBytes, upBytes2);
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
