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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.kll.KllDirectCompactItemsSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllItemsSketch;
import org.testng.annotations.Test;

public class KllDirectCompactItemsSketchTest {
  public ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test
  public void checkRODirectCompact() {
    final int k = 20;
    final int n = 21;
    final int digits = Util.numDigits(n);
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= n; i++) { sk.update(Util.longToFixedLengthString(i, digits)); }
    final byte[] byteArr = KllHelper.toByteArray(sk, true); //request for updatable is denied -> COMPACT, RO
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArr);  //compact RO fmt
    final KllItemsSketch<String> sk2 = KllItemsSketch.wrap(srcSeg, Comparator.naturalOrder(), serDe);
    assertTrue(sk2 instanceof KllDirectCompactItemsSketch);
    println(sk2.toString(true, false));
    assertFalse(sk2.isMemorySegmentUpdatableFormat());
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getMinItem(), " 1");
    assertEquals(sk2.getMaxItem(), "21");
  }

  @Test
  public void checkDirectCompactSingleItem() {
    final int k = 20;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);

    sk.update("1");
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    assertTrue(sk2 instanceof KllDirectCompactItemsSketch);
    //println(sk2.toString(true, false));
    assertTrue(sk2.isReadOnly());
    assertEquals(sk2.getSingleItem(), "1");

    sk.update("2");
    sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    assertEquals(sk2.getN(), 2);
    try {
      sk2.getSingleItem(); //not a single item
      fail();
    } catch (final SketchesArgumentException e) {  }
  }

  @Test
  public void checkHeapGetFullItemsArray() {
    final int k = 20;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);

    String[] itemsArr = sk.getTotalItemsArray();
    for (int j = 0; j < k; j++) { assertNull(itemsArr[j]); }

    sk.update(" 1"); //single
    itemsArr = sk.getTotalItemsArray();
    for (int j = 0; j < (k - 1); j++) { assertNull(itemsArr[j]); }
    assertEquals(itemsArr[k - 1], " 1");

    sk.update(" 2"); //multiple
    itemsArr = sk.getTotalItemsArray();
    for (int j = 0; j < (k - 2); j++) { assertNull(itemsArr[j]); }
    assertEquals(itemsArr[k - 1], " 1");
    assertEquals(itemsArr[k - 2], " 2");
  }

  @Test
  public void checkDirectCompactGetFullItemsArray() {
    final int k = 20;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);

    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    String[] itemsArr = sk2.getTotalItemsArray(); //empty
    for (int j = 0; j < k; j++) { assertNull(itemsArr[j]); }

    sk.update(" 1"); //single
    sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    itemsArr = sk2.getTotalItemsArray();
    for (int j = 0; j < (k - 1); j++) { assertNull(itemsArr[j]); }
    assertEquals(itemsArr[k - 1], " 1");

    sk.update(" 2"); //multi
    sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    itemsArr = sk2.getTotalItemsArray();
    for (int j = 0; j < (k - 2); j++) { assertNull(itemsArr[j]); }
    assertEquals(itemsArr[k - 1], " 1");
    assertEquals(itemsArr[k - 2], " 2");
  }

  @Test
  public void checkHeapAndDirectCompactGetRetainedItemsArray() {
    final int k = 20;

    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    String[] retArr = sk.getRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 0);

    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    retArr = sk2.getRetainedItemsArray();
    assertEquals(retArr.length, sk2.getNumRetained());
    assertEquals(retArr.length, 0);

    sk.update(" 1");
    retArr = sk.getRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], " 1");

    sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    retArr = sk2.getRetainedItemsArray();
    assertEquals(retArr.length, sk2.getNumRetained());
    assertEquals(retArr.length, 1);
    assertEquals(retArr[0], " 1");

    for (int i = 2; i <= 21; i++) { sk.update(Util.longToFixedLengthString(i, 2)); }
    retArr = sk.getRetainedItemsArray();
    assertEquals(retArr.length, sk.getNumRetained());
    assertEquals(retArr.length, 11);

    sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    assertEquals(retArr.length, sk2.getNumRetained());
    assertEquals(retArr.length, 11);
  }

  @Test
  public void checkMinAndMax() {
    final int k = 20;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    try { sk2.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sk2.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    sk.update(" 1");
    sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    assertEquals(sk2.getMaxItem()," 1");
    assertEquals(sk2.getMinItem()," 1");
    for (int i = 2; i <= 21; i++) { sk.update(Util.longToFixedLengthString(i, 2)); }
    sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    assertEquals(sk2.getMaxItem(),"21");
    assertEquals(sk2.getMinItem()," 1");
  }

  @Test
  public void checkQuantile() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= 1000; i++) { sk1.update(Util.longToFixedLengthString(i, 4)); }
    final KllItemsSketch<String> sk2 = KllItemsSketch.wrap(MemorySegment.ofArray(sk1.toByteArray()), Comparator.naturalOrder(), serDe);
    final String med2 = sk2.getQuantile(0.5);
    final String med1 = sk1.getQuantile(0.5);
    assertEquals(med1, med2);
    println("Med1: " + med1);
    println("Med2: " + med2);
  }

  @Test
  public void checkCompactSingleItemMerge() {
    final int k = 20;
    final KllItemsSketch<String> skH1 =
        KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe); //Heap with 1 (single)
    skH1.update("21");
    final KllItemsSketch<String> skDC1 = //Direct Compact with 1 (single)
        KllItemsSketch.wrap(MemorySegment.ofArray(skH1.toByteArray()), Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> skH20 =  KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe); //Heap with 20
    for (int i = 1; i <= 20; i++) { skH20.update(Util.longToFixedLengthString(i, 2)); }
    skH20.merge(skDC1);
    assertEquals(skH20.getN(), 21);
  }

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
