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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class KllDirectCompactDoublesSketchTest {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void checkRODirectUpdatable() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    byte[] byteArr = KllHelper.toUpdatableByteArrayImpl(sk);
    Memory srcMem = Memory.wrap(byteArr);
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(srcMem);
    assertEquals(sk2.getMinValue(), 1.0);
    assertEquals(sk2.getMaxValue(), 21.0);
  }

  @Test
  public void checkRODirectCompact() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    Memory srcMem = Memory.wrap(sk.toByteArray());
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(srcMem);
    println(sk2.toString(true, true));
    assertEquals(sk2.getMinValue(), 1.0);
    assertEquals(sk2.getMaxValue(), 21.0);
    Memory srcMem2 = Memory.wrap(sk2.toByteArray());
    KllDoublesSketch sk3 = KllDoublesSketch.writableWrap((WritableMemory)srcMem2, null);
    assertEquals(sk3.getMinValue(), 1.0F);
    assertEquals(sk3.getMaxValue(), 21.0F);
  }

  @Test
  public void checkDirectCompactSingleItem() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    sk.update(1);
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getDoubleSingleItem(), 1.0);
    sk.update(2);
    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getN(), 2);
    try {
      sk2.getDoubleSingleItem();
      fail();
    } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkDirectCompactGetFloatItemsArray() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);

    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    double[] itemsArr = sk2.getDoubleItemsArray();
    for (int i = 0; i < 20; i++) { assertEquals(itemsArr[i], 0F); }

    sk.update(1);
    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    itemsArr = sk2.getDoubleItemsArray();
    for (int i = 0; i < 19; i++) { assertEquals(itemsArr[i], 0F); }
    assertEquals(itemsArr[19], 1F);

    for (int i = 2; i <= 21; i++) { sk.update(i); }
    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    itemsArr = sk2.getDoubleItemsArray();
    assertEquals(itemsArr.length, 33);
    assertEquals(itemsArr[22], 21);
    //for (int i = 0; i < itemsArr.length; i++) {
    //  println(i + ": " + itemsArr[i]);
    //}
  }

  @Test
  public void checkMinAndMax() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertTrue(Double.isNaN(sk2.getMaxValue()));
    assertTrue(Double.isNaN(sk2.getMinValue()));
    sk.update(1);
    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getMaxValue(),1.0F);
    assertEquals(sk2.getMinValue(),1.0F);
    for (int i = 2; i <= 21; i++) { sk.update(i); }
    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getMaxValue(),21.0F);
    assertEquals(sk2.getMinValue(),1.0F);
  }

  @Test
  public void checkQuantile() {
    KllDoublesSketch sk1 = KllDoublesSketch.newHeapInstance();
    for (int i = 1; i <= 1000; i++) { sk1.update(i); }
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk1.toByteArray()));
    double med2 = sk2.getQuantile(0.5);
    double med1 = sk1.getQuantile(0.5);
    assertEquals(med1, med2);
    println("Med1: " + med1);
    println("Med2: " + med2);
  }

  @Test
  public void checkCompactSingleItemMerge() {
    int k = 20;
    KllDoublesSketch skH1 = KllDoublesSketch.newHeapInstance(k); //Heap with 1 (single)
    skH1.update(21);
    KllDoublesSketch skDC1 = KllDoublesSketch.wrap(Memory.wrap(skH1.toByteArray())); //Direct Compact with 1 (single)
    KllDoublesSketch skH20 =  KllDoublesSketch.newHeapInstance(k); //Heap with 20
    for (int i = 1; i <= 20; i++) { skH20.update(i); }
    skH20.merge(skDC1);
    assertEquals(skH20.getN(), 21);

    WritableMemory wmem = WritableMemory.allocate(1000);
    KllDoublesSketch skDU20 = KllDoublesSketch.newDirectInstance(k, wmem, memReqSvr);//Direct Updatable with 21
    for (int i = 1; i <= 20; i++) { skDU20.update(i); }
    skDU20.merge(skDC1);
    assertEquals(skDU20.getN(), 21);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param o value to print
   */
  static void println(final Object o) {
    //System.out.println(o.toString()); //disable here
  }

}
