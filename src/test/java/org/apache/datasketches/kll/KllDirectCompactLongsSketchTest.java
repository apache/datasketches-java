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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDirectLongsSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllLongsSketch;
import org.apache.datasketches.kll.KllSketch.SketchStructure;
import org.testng.annotations.Test;

public class KllDirectCompactLongsSketchTest {

  @Test
  public void checkRODirectUpdatable_ROandWritable() {
    final int k = 20;
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= (k + 1); i++) { sk.update(i); }
    final byte[] byteArr = KllHelper.toByteArray(sk, true); //request  updatable
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArr).asReadOnly(); //cast to MemorySegment -> read only
    final KllLongsSketch sk2 = KllLongsSketch.wrap(srcSeg);
    assertTrue(sk2 instanceof KllDirectLongsSketch);

    assertTrue(sk2.isMemorySegmentUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 21L);

    final MemorySegment srcWseg = MemorySegment.ofArray(byteArr);
    final KllLongsSketch sk3 = KllLongsSketch.wrap(srcWseg);
    assertTrue(sk3 instanceof KllDirectLongsSketch);
    println(sk3.toString(true, false));
    assertFalse(sk3.isReadOnly());
    sk3.update(22);
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 22L);
  }

  @Test
  public void checkRODirectCompact() {
    final int k = 20;
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= (k + 1); i++) { sk.update(i); }
    // go from heap to MemorySegment
    final MemorySegment srcSeg = MemorySegment.ofArray(sk.toByteArray()); //compact RO fmt
    final KllLongsSketch sk2 = KllLongsSketch.wrap(srcSeg);
    assertTrue(sk2.hasMemorySegment());
    assertTrue(sk2.sketchStructure == SketchStructure.COMPACT_FULL);
    assertTrue(sk2.isCompactMemorySegmentFormat());
    assertFalse(sk2.isMemorySegmentUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 21L);
    // go from MemorySegment to byte array back to MemorySegment
    final MemorySegment srcSeg2 = MemorySegment.ofArray(sk2.toByteArray());
    // wrap it back to a sketch
    final KllLongsSketch sk3 = KllLongsSketch.wrap(srcSeg2);
    assertTrue(sk2.hasMemorySegment());
    assertTrue(sk2.sketchStructure == SketchStructure.COMPACT_FULL);
    assertTrue(sk2.isCompactMemorySegmentFormat());
    assertFalse(sk2.isMemorySegmentUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), 1L);
    assertEquals(sk2.getMaxItem(), 21L);
  }

  @Test
  public void checkDirectCompactSingleItem() {
    final int k = 20;
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);

    sk.update(1);
    KllLongsSketch sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    assertTrue(sk2.sketchStructure == SketchStructure.COMPACT_SINGLE);
    //println(sk2.toString(true, false));
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getLongSingleItem(), 1L);

    sk.update(2);
    sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    assertEquals(sk2.getN(), 2);
    try {
      sk2.getLongSingleItem();
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkDirectCompactGetLongItemsArray() {
    final int k = 20;
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);

    KllLongsSketch sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    long[] itemsArr = sk2.getLongItemsArray();
    for (int i = 0; i < 20; i++) { assertEquals(itemsArr[i], 0); }

    sk.update(1);
    sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    itemsArr = sk2.getLongItemsArray();
    for (int i = 0; i < 19; i++) { assertEquals(itemsArr[i], 0); }
    assertEquals(itemsArr[19], 1L);

    for (int i = 2; i <= 21; i++) { sk.update(i); }
    sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    itemsArr = sk2.getLongItemsArray();
    assertEquals(itemsArr.length, 33);
    assertEquals(itemsArr[22], 21);
  }

  @Test
  public void checkHeapAndDirectCompactGetRetainedItemsArray() {
    final int k = 20;

    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    long[] retArr = sk.getLongRetainedItemsArray();
    assertEquals(retArr.length, 0);

    KllLongsSketch sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    retArr = sk2.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 0);

    sk.update(1);
    retArr = sk.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], 1L);

    sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    retArr = sk2.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], 1L);

    for (int i = 2; i <= 21; i++) { sk.update(i); }
    retArr = sk.getLongRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 11);

    sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    assertEquals(retArr.length, sk2.getNumRetained());
    assertEquals(retArr.length, 11);
  }

  @Test
  public void checkMinAndMax() {
    final int k = 20;
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    KllLongsSketch sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    try { sk2.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sk2.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    sk.update(1);
    sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    assertEquals(sk2.getMaxItem(),1L);
    assertEquals(sk2.getMinItem(),1L);
    for (int i = 2; i <= 21; i++) { sk.update(i); }
    sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()));
    assertEquals(sk2.getMaxItem(),21L);
    assertEquals(sk2.getMinItem(),1L);
  }

  @Test
  public void checkQuantile() {
    final KllLongsSketch sk1 = KllLongsSketch.newHeapInstance();
    for (int i = 1; i <= 1000; i++) { sk1.update(i); }
    final KllLongsSketch sk2 = KllLongsSketch.wrap(MemorySegment.ofArray(sk1.toByteArray()));
    final long med2 = sk2.getQuantile(0.5);
    final long med1 = sk1.getQuantile(0.5);
    assertEquals(med1, med2);
    println("Med1: " + med1);
    println("Med2: " + med2);
  }

  @Test
  public void checkCompactSingleItemMerge() {
    final int k = 20;
    final KllLongsSketch skH1 = KllLongsSketch.newHeapInstance(k); //Heap with 1 (single)
    skH1.update(21);
    final KllLongsSketch skDC1 = KllLongsSketch.wrap(MemorySegment.ofArray(skH1.toByteArray())); //Direct Compact with 1 (single)
    final KllLongsSketch skH20 =  KllLongsSketch.newHeapInstance(k); //Heap with 20
    for (int i = 1; i <= 20; i++) { skH20.update(i); }
    skH20.merge(skDC1);
    assertEquals(skH20.getN(), 21);

    final MemorySegment wseg = MemorySegment.ofArray(new byte[1000]);
    final KllLongsSketch skDU20 = KllLongsSketch.newDirectInstance(k, wseg, null);//Direct Updatable with 21
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
