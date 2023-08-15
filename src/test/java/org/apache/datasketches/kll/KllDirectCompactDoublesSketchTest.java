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
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDirectDoublesSketch.KllDirectCompactDoublesSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class KllDirectCompactDoublesSketchTest {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void checkRODirectUpdatable_ROandWritable() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    byte[] byteArr = KllHelper.toByteArray(sk, true); //request  updatable
    Memory srcMem = Memory.wrap(byteArr); //cast to Memory -> read only
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(srcMem);
    assertTrue(sk2 instanceof KllDirectDoublesSketch);

    assertTrue(sk2.isMemoryUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), 1.0);
    assertEquals(sk2.getMaxItem(), 21.0);

    WritableMemory srcWmem = WritableMemory.writableWrap(byteArr);
    KllDoublesSketch sk3 = KllDoublesSketch.writableWrap(srcWmem, memReqSvr);
    assertTrue(sk3 instanceof KllDirectDoublesSketch);
    println(sk3.toString(true, false));
    assertFalse(sk3.isReadOnly());
    sk3.update(22.0);
    assertEquals(sk3.getMinItem(), 1.0);
    assertEquals(sk3.getMaxItem(), 22.0);
  }

  @Test
  public void checkRODirectCompact() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    Memory srcMem = Memory.wrap(sk.toByteArray()); //compact RO fmt
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(srcMem);
    assertTrue(sk2 instanceof KllDirectCompactDoublesSketch);
    //println(sk2.toString(true, false));
    assertFalse(sk2.isMemoryUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), 1.0);
    assertEquals(sk2.getMaxItem(), 21.0);
    Memory srcMem2 = Memory.wrap(sk2.toByteArray());
    KllDoublesSketch sk3 = KllDoublesSketch.writableWrap((WritableMemory)srcMem2, memReqSvr);
    assertTrue(sk3 instanceof KllDirectCompactDoublesSketch);
    assertFalse(sk2.isMemoryUpdatableFormat());
    //println(sk3.toString(true, false));
    assertTrue(sk3.isReadOnly());
    assertEquals(sk3.getMinItem(), 1.0);
    assertEquals(sk3.getMaxItem(), 21.0);
  }

  @Test
  public void checkDirectCompactSingleItem() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);

    sk.update(1);
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertTrue(sk2 instanceof KllDirectCompactDoublesSketch);
    //println(sk2.toString(true, false));
    assertTrue(sk2.isReadOnly());
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
  public void checkDirectCompactGetDoubleItemsArray() {
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
  }

  @Test
  public void checkHeapAndDirectCompactGetRetainedItemsArray() {
    int k = 20;

    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    double[] retArr = sk.getDoubleRetainedItemsArray();
    assertEquals(retArr.length, 0);

    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    retArr = sk2.getDoubleRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 0);

    sk.update(1.0);
    retArr = sk.getDoubleRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], 1.0);

    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    retArr = sk2.getDoubleRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], 1.0);

    for (int i = 2; i <= 21; i++) { sk.update(i); }
    retArr = sk.getDoubleRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 11);

    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(retArr.length, sk2.getNumRetained());
    assertEquals(retArr.length, 11);
  }

  @Test
  public void checkMinAndMax() {
    int k = 20;
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    try { sk2.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sk2.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    sk.update(1);
    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getMaxItem(),1.0F);
    assertEquals(sk2.getMinItem(),1.0F);
    for (int i = 2; i <= 21; i++) { sk.update(i); }
    sk2 = KllDoublesSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getMaxItem(),21.0F);
    assertEquals(sk2.getMinItem(),1.0F);
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

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
