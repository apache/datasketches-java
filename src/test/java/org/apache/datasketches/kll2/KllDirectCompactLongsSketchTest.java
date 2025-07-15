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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDirectLongsSketch.KllDirectCompactLongsSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class KllDirectCompactLongsSketchTest {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void checkRODirectUpdatable_ROandWritable() {
    int k = 20;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    byte[] byteArr = KllHelper.toByteArray(sk, true); //request  updatable
    Memory srcMem = Memory.wrap(byteArr); //cast to Memory -> read only
    KllLongsSketch sk2 = KllLongsSketch.wrap(srcMem);
    assertTrue(sk2 instanceof KllDirectLongsSketch);

    assertTrue(sk2.isMemoryUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 21L);

    WritableMemory srcWmem = WritableMemory.writableWrap(byteArr);
    KllLongsSketch sk3 = KllLongsSketch.writableWrap(srcWmem, memReqSvr);
    assertTrue(sk3 instanceof KllDirectLongsSketch);
    println(sk3.toString(true, false));
    assertFalse(sk3.isReadOnly());
    sk3.update(22);
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 22L);
  }

  @Test
  public void checkRODirectCompact() {
    int k = 20;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= k + 1; i++) { sk.update(i); }
    Memory srcMem = Memory.wrap(sk.toByteArray()); //compact RO fmt
    KllLongsSketch sk2 = KllLongsSketch.wrap(srcMem);
    assertTrue(sk2 instanceof KllDirectCompactLongsSketch);
    //println(sk2.toString(true, false));
    assertFalse(sk2.isMemoryUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 21L);
    Memory srcMem2 = Memory.wrap(sk2.toByteArray());
    KllLongsSketch sk3 = KllLongsSketch.writableWrap((WritableMemory)srcMem2, memReqSvr);
    assertTrue(sk3 instanceof KllDirectCompactLongsSketch);
    assertFalse(sk2.isMemoryUpdatableFormat());
    //println(sk3.toString(true, false));
    assertTrue(sk3.isReadOnly());
    assertEquals(sk3.getMinItem(), 1L);
    assertEquals(sk3.getMaxItem(), 21L);
  }

  @Test
  public void checkDirectCompactSingleItem() {
    int k = 20;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);

    sk.update(1);
    KllLongsSketch sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertTrue(sk2 instanceof KllDirectCompactLongsSketch);
    //println(sk2.toString(true, false));
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getLongSingleItem(), 1L);

    sk.update(2);
    sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getN(), 2);
    try {
      sk2.getLongSingleItem();
      fail();
    } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkDirectCompactGetLongItemsArray() {
    int k = 20;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);

    KllLongsSketch sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    long[] itemsArr = sk2.getLongItemsArray();
    for (int i = 0; i < 20; i++) { assertEquals(itemsArr[i], 0); }

    sk.update(1);
    sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    itemsArr = sk2.getLongItemsArray();
    for (int i = 0; i < 19; i++) { assertEquals(itemsArr[i], 0); }
    assertEquals(itemsArr[19], 1L);

    for (int i = 2; i <= 21; i++) { sk.update(i); }
    sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    itemsArr = sk2.getLongItemsArray();
    assertEquals(itemsArr.length, 33);
    assertEquals(itemsArr[22], 21);
  }

  @Test
  public void checkHeapAndDirectCompactGetRetainedItemsArray() {
    int k = 20;

    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    long[] retArr = sk.getLongRetainedItemsArray();
    assertEquals(retArr.length, 0);

    KllLongsSketch sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    retArr = sk2.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 0);

    sk.update(1);
    retArr = sk.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], 1L);

    sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    retArr = sk2.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], 1L);

    for (int i = 2; i <= 21; i++) { sk.update(i); }
    retArr = sk.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 11);

    sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(retArr.length, sk2.getNumRetained());
    assertEquals(retArr.length, 11);
  }

  @Test
  public void checkMinAndMax() {
    int k = 20;
    KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    KllLongsSketch sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    try { sk2.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sk2.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    sk.update(1);
    sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getMaxItem(),1L);
    assertEquals(sk2.getMinItem(),1L);
    for (int i = 2; i <= 21; i++) { sk.update(i); }
    sk2 = KllLongsSketch.wrap(Memory.wrap(sk.toByteArray()));
    assertEquals(sk2.getMaxItem(),21L);
    assertEquals(sk2.getMinItem(),1L);
  }

  @Test
  public void checkQuantile() {
    KllLongsSketch sk1 = KllLongsSketch.newHeapInstance();
    for (int i = 1; i <= 1000; i++) { sk1.update(i); }
    KllLongsSketch sk2 = KllLongsSketch.wrap(Memory.wrap(sk1.toByteArray()));
    long med2 = sk2.getQuantile(0.5);
    long med1 = sk1.getQuantile(0.5);
    assertEquals(med1, med2);
    println("Med1: " + med1);
    println("Med2: " + med2);
  }

  @Test
  public void checkCompactSingleItemMerge() {
    int k = 20;
    KllLongsSketch skH1 = KllLongsSketch.newHeapInstance(k); //Heap with 1 (single)
    skH1.update(21);
    KllLongsSketch skDC1 = KllLongsSketch.wrap(Memory.wrap(skH1.toByteArray())); //Direct Compact with 1 (single)
    KllLongsSketch skH20 =  KllLongsSketch.newHeapInstance(k); //Heap with 20
    for (int i = 1; i <= 20; i++) { skH20.update(i); }
    skH20.merge(skDC1);
    assertEquals(skH20.getN(), 21);

    WritableMemory wmem = WritableMemory.allocate(1000);
    KllLongsSketch skDU20 = KllLongsSketch.newDirectInstance(k, wmem, memReqSvr);//Direct Updatable with 21
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
