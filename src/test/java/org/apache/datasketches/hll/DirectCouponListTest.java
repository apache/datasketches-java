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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.hll.PreambleUtil.LG_ARR_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.hll.AbstractCoupons;
import org.apache.datasketches.hll.CurMode;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DirectCouponListTest {

  @Test
  public void promotionTests() {
    //true means convert to compact array
    promotions(8, 7, TgtHllType.HLL_8, true, CurMode.LIST);
    promotions(8, 7, TgtHllType.HLL_8, false, CurMode.LIST);
    promotions(8, 24, TgtHllType.HLL_8, true, CurMode.SET);
    promotions(8, 24, TgtHllType.HLL_8, false, CurMode.SET);
    promotions(8, 25, TgtHllType.HLL_8, true, CurMode.HLL);
    promotions(8, 25, TgtHllType.HLL_8, false, CurMode.HLL);
    promotions(8, 25, TgtHllType.HLL_6, true, CurMode.HLL);
    promotions(8, 25, TgtHllType.HLL_6, false, CurMode.HLL);
    promotions(8, 25, TgtHllType.HLL_4, true, CurMode.HLL);
    promotions(8, 25, TgtHllType.HLL_4, false, CurMode.HLL);

    promotions(4, 7, TgtHllType.HLL_8, true, CurMode.LIST);
    promotions(4, 7, TgtHllType.HLL_8, false, CurMode.LIST);
    promotions(4, 8, TgtHllType.HLL_8, true, CurMode.HLL);
    promotions(4, 8, TgtHllType.HLL_8, false, CurMode.HLL);
    promotions(4, 8, TgtHllType.HLL_6, true, CurMode.HLL);
    promotions(4, 8, TgtHllType.HLL_6, false, CurMode.HLL);
    promotions(4, 8, TgtHllType.HLL_4, true, CurMode.HLL);
    promotions(4, 8, TgtHllType.HLL_4, false, CurMode.HLL);
    promotions(4, 25, TgtHllType.HLL_4, true, CurMode.HLL);
    promotions(4, 25, TgtHllType.HLL_4, false, CurMode.HLL);
  }

  private static void promotions(final int lgConfigK, final int n, final TgtHllType tgtHllType, final boolean compact,
      final CurMode tgtMode) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    HllSketch hllSketch;

    //println("DIRECT");
    byte[] barr1;
    MemorySegment wseg;
    try (Arena arena = Arena.ofConfined()) {
      wseg = arena.allocate(bytes);
      hllSketch = new HllSketch(lgConfigK, tgtHllType, wseg);
      assertTrue(hllSketch.isEmpty());

      for (int i = 0; i < n; i++) {
        hllSketch.update(i);
      }
      //println(hllSketch.toString(true, true, false, false));
      assertFalse(hllSketch.isEmpty());
      assertEquals(hllSketch.getCurMode(), tgtMode);
      assertTrue(hllSketch.hasMemorySegment());
      assertTrue(hllSketch.isOffHeap());
      assertTrue(hllSketch.isSameResource(wseg));

      //convert direct sketch to byte[]
      barr1 = (compact) ? hllSketch.toCompactByteArray() : hllSketch.toUpdatableByteArray();
      //println(PreambleUtil.toString(barr1));
      hllSketch.reset();
      assertTrue(hllSketch.isEmpty());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    //println("HEAP");
    final HllSketch hllSketch2 = new HllSketch(lgConfigK, tgtHllType);
    for (int i = 0; i < n; i++) {
      hllSketch2.update(i);
    }
    //println(hllSketch2.toString(true, true, false, false));
    //println(PreambleUtil.toString(barr2));
    assertEquals(hllSketch2.getCurMode(), tgtMode);
    assertFalse(hllSketch2.hasMemorySegment());
    assertFalse(hllSketch2.isOffHeap());
    assertFalse(hllSketch2.isSameResource(wseg));
    final byte[] barr2 = (compact) ? hllSketch2.toCompactByteArray() : hllSketch2.toUpdatableByteArray();
    assertEquals(barr1.length, barr2.length, barr1.length + ", " + barr2.length);
    //printDiffs(barr1, barr2);
    assertEquals(barr1, barr2);
  }

  @SuppressWarnings("unused") //only used when above printlns are enabled.
  private static void printDiffs(final byte[] arr1, final byte[] arr2) {
    final int len1 = arr1.length;
    final int len2 = arr2.length;
    final int minLen = Math.min(len1,  len2);
    for (int i = 0; i < minLen; i++) {
      final int v1 = arr1[i] & 0XFF;
      final int v2 = arr2[i] & 0XFF;
      if (v1 == v2) { continue; }
      println(i + ", " + v1 + ", " + v2);
    }
  }

  @Test
  public void checkCouponToByteArray() {
    final int lgK = 8;
    final TgtHllType type = TgtHllType.HLL_8;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, type, wseg);
    int i;
    for (i = 0; i < 7; i++) { sk.update(i); } //LIST

    //toCompactSeg Arr from compact seg
    byte[] compactByteArr = sk.toCompactByteArray();
    MemorySegment compactSeg = MemorySegment.ofArray(compactByteArr);
    HllSketch skCompact = HllSketch.wrap(compactSeg);
    byte[] compactByteArr2 = skCompact.toCompactByteArray();
    assertEquals(compactByteArr2, compactByteArr);

    //now check to UpdatableByteArr from compact seg
    byte[] updatableByteArr = sk.toUpdatableByteArray();
    byte[] updatableByteArr2 = skCompact.toUpdatableByteArray();
    assertEquals(updatableByteArr2.length, updatableByteArr.length);
    assertEquals(skCompact.getEstimate(), sk.getEstimate());


    sk.update(i); //SET
    //toCompactSeg Arr from compact seg
    compactByteArr = sk.toCompactByteArray();
    compactSeg = MemorySegment.ofArray(compactByteArr);
    skCompact = HllSketch.wrap(compactSeg);
    compactByteArr2 = skCompact.toCompactByteArray();
    assertEquals(compactByteArr2, compactByteArr);

    //now check to UpdatableByteArr from compact seg
    updatableByteArr = sk.toUpdatableByteArray();
    updatableByteArr2 = skCompact.toUpdatableByteArray();
    assertEquals(updatableByteArr2.length, updatableByteArr.length);
    assertEquals(skCompact.getEstimate(), sk.getEstimate());
  }

  @Test
  public void checkDirectGetCouponIntArr() {
    final int lgK = 8;
    final TgtHllType type = TgtHllType.HLL_8;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, type, wseg);
    final AbstractCoupons absCoup = (AbstractCoupons)sk.hllSketchImpl;
    assertNull(absCoup.getCouponIntArr());
  }

  @Test
  public void checkBasicGetLgCouponArrInts() {
    final int lgK = 8;
    final TgtHllType type = TgtHllType.HLL_8;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, type, wseg);
    for (int i = 0; i < 7; i++) { sk.update(i); }
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 3);
    sk.update(7);
    assertEquals(sk.getCurMode(), CurMode.SET);
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 5);

    sk.reset();
    for (int i = 0; i < 7; i++) { sk.update(i); }
    final byte lgArr = wseg.get(JAVA_BYTE, LG_ARR_BYTE);
    wseg.set(JAVA_BYTE, LG_ARR_BYTE, (byte) 0); //corrupt to 0
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 3);
    wseg.set(JAVA_BYTE, LG_ARR_BYTE, lgArr); //put correct value back
    sk.update(7);
    assertEquals(sk.getCurMode(), CurMode.SET);
    assertEquals(sk.hllSketchImpl.curMode, CurMode.SET);
    wseg.set(JAVA_BYTE, LG_ARR_BYTE, (byte) 0); //corrupt to 0 again
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 5);
  }

  @Test
  public void checkHeapifyGetLgCouponArrInts() {
    final int lgK = 8;
    final TgtHllType type = TgtHllType.HLL_8;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, type, wseg);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    assertEquals(sk.getCurMode(), CurMode.SET);
    final double est1 = sk.getEstimate();

    wseg.set(JAVA_BYTE, LG_ARR_BYTE, (byte) 0); //corrupt to 0
    final HllSketch sk2 = HllSketch.heapify(wseg);
    final double est2 = sk2.getEstimate();
    assertEquals(est2, est1, 0.0);
    //println(sk2.toString(true, true, true, true));
    //println(PreambleUtil.toString(wseg));
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
