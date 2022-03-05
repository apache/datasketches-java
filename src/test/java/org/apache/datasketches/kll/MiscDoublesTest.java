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

import static org.apache.datasketches.kll.PreambleUtil.DEFAULT_K;
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
public class MiscDoublesTest {
  static final String LS = System.getProperty("line.separator");

  @Test
  public void checkGetKFromEps() {
    final int k = DEFAULT_K;
    final double eps = KllHelper.getNormalizedRankError(k, false);
    final double epsPmf = KllHelper.getNormalizedRankError(k, true);
    final int kEps = BaseKllSketch.getKFromEpsilon(eps, false);
    final int kEpsPmf = BaseKllSketch.getKFromEpsilon(epsPmf, true);
    assertEquals(kEps, k);
    assertEquals(kEpsPmf, k);
  }

  @Test
  public void checkBounds() {
    final KllDoublesSketch kll = new KllDoublesSketch(); //default k = 200
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
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions1() {
    KllDoublesSketch sk = new KllDoublesSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(6, (byte)4); //corrupt with different M
    KllDoublesSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions2() {
    KllDoublesSketch sk = new KllDoublesSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte)1); //corrupt preamble ints, should be 2
    KllDoublesSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions3() {
    KllDoublesSketch sk = new KllDoublesSketch();
    sk.update(1.0f);
    sk.update(2.0f);
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(0, (byte)1); //corrupt preamble ints, should be 5
    KllDoublesSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions4() {
    KllDoublesSketch sk = new KllDoublesSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(1, (byte)0); //corrupt SerVer, should be 1 or 2
    KllDoublesSketch.heapify(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyExceptions5() {
    KllDoublesSketch sk = new KllDoublesSketch();
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    wmem.putByte(2, (byte)0); //corrupt FamilyID, should be 15
    KllDoublesSketch.heapify(wmem);
  }

  @Test
  public void checkMisc() {
    KllDoublesSketch sk = new KllDoublesSketch(8, true);
    assertTrue(Objects.isNull(sk.getQuantiles(10)));
    sk.toString(true, true);
    for (int i = 0; i < 20; i++) { sk.update(i); }
    sk.toString(true, true);
    sk.toByteArray();
    final double[] items = sk.getItems();
    assertEquals(items.length, 16);
    final int[] levels = sk.getLevelsArray();
    assertEquals(levels.length, 3);
    assertEquals(sk.getNumLevels(), 2);
  }

  @Test //enable static println(..) for visual checking
  public void visualCheckToString() {
    final KllDoublesSketch sketch = new KllDoublesSketch(20);
    for (int i = 0; i < 10; i++) { sketch.update(i + 1); }
    println(sketch.toString(true, true));

    final KllDoublesSketch sketch2 = new KllDoublesSketch(20);
    for (int i = 0; i < 400; i++) { sketch2.update(i + 1); }
    println("\n" + sketch2.toString(true, true));

    sketch2.merge(sketch);
    final String s2 = sketch2.toString(true, true);
    println(LS + s2);
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

