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

package org.apache.datasketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.hll.AbstractCoupons;
import org.apache.datasketches.hll.CurMode;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.PairIterator;
import org.apache.datasketches.hll.TgtHllType;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class CouponListTest {

  @Test //visual check
  public void checkIterator() {
    checkIterator(false, 4, 7);
    checkIterator(true, 4, 7);
  }

  private static void checkIterator(final boolean direct, final int lgK, final int n) {
    final TgtHllType tgtHllType = TgtHllType.HLL_4;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = (direct) ? new HllSketch(lgK, tgtHllType, wseg) : new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) { sk.update(i); }
    final String store = direct ? "MemorySegment" : "Heap";
    println("CurMode: " + sk.getCurMode().toString() + "; Store: " + store);
    final PairIterator itr = sk.iterator();
    println(itr.getHeader());
    while (itr.nextAll()) {
      assertTrue(itr.getSlot() < (1 << lgK));
      println(itr.getString());
    }
  }

  @Test
  public void checkDuplicatesAndMisc() {
    checkDuplicatesAndMisc(false);
    checkDuplicatesAndMisc(true);
  }

  private static void checkDuplicatesAndMisc(final boolean direct) {
    final int lgConfigK = 8;
    final TgtHllType tgtHllType = TgtHllType.HLL_4;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = (direct) ? new HllSketch(lgConfigK, tgtHllType, wseg) : new HllSketch(8);

    for (int i = 1; i <= 7; i++) {
      sk.update(i);
      sk.update(i);
    }
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertEquals(sk.getCompositeEstimate(), 7.0, 7 * .01);
    assertEquals(sk.getHipEstimate(), 7.0, 7 * .01);
    sk.hllSketchImpl.putEmptyFlag(false); //dummy
    sk.hllSketchImpl.putRebuildCurMinNumKxQFlag(false); //dummy
    if (direct) {
      assertNotNull(sk.getMemorySegment());
    } else {
      assertNull(sk.getMemorySegment());
    }

    sk.update(8);
    sk.update(8);
    assertEquals(sk.getCurMode(), CurMode.SET);
    assertEquals(sk.getCompositeEstimate(), 8.0, 8 * .01);
    assertEquals(sk.getHipEstimate(), 8.0, 8 * .01);
    if (direct) {
      assertNotNull(sk.getMemorySegment());
    } else {
      assertNull(sk.getMemorySegment());
    }

    for (int i = 9; i <= 25; i++) {
      sk.update(i);
      sk.update(i);
    }
    assertEquals(sk.getCurMode(), CurMode.HLL);
    assertEquals(sk.getCompositeEstimate(), 25.0, 25 * .1);
    if (direct) {
      assertNotNull(sk.getMemorySegment());
    } else {
      assertNull(sk.getMemorySegment());

    }
  }

  @Test
  public void toByteArray_Heapify() {
    toByteArrayHeapify(7);
    toByteArrayHeapify(21);
  }

  private static void toByteArrayHeapify(final int lgK) {
    final HllSketch sk1 = new HllSketch(lgK);

    final int u = (lgK < 8) ? 7 : ((1 << (lgK - 3))/4) * 3;
    for (int i = 0; i < u; i++) {
      sk1.update(i);
    }
    final double est1 = sk1.getEstimate();
    assertEquals(est1, u, u * 100.0E-6);
    //println("Original\n" + sk1.toString());

    byte[] byteArray = sk1.toCompactByteArray();
    //println("Preamble: " + PreambleUtil.toString(byteArray));
    HllSketch sk2 = HllSketch.heapify(byteArray);
    double est2 = sk2.getEstimate();
    //println("Heapify Compact\n" + sk2.toString(true, true, true, true));
    assertEquals(est2, est1, 0.0);

    byteArray = sk1.toUpdatableByteArray();
    sk2 = HllSketch.heapify(byteArray);
    est2 = sk2.getEstimate();
    //println("Heapify Updatable\n" + sk2.toString());
    assertEquals(est2, est1, 0.0);
  }

  @Test
  public void checkGetMemory() {
    final HllSketch sk1 = new HllSketch(4);
    final AbstractCoupons absCoup = (AbstractCoupons) sk1.hllSketchImpl;
    assertNull(absCoup.getMemorySegment());
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }
}
