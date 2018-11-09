/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.LG_ARR_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;

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

  private static void promotions(int lgConfigK, int n, TgtHllType tgtHllType, boolean compact,
      CurMode tgtMode) {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    HllSketch hllSketch;

    //println("DIRECT");
    byte[] barr1;
    WritableMemory wmem = null;
    try (WritableDirectHandle hand = WritableMemory.allocateDirect(bytes)) {
      wmem = hand.get();
      //byte[] byteArr = new byte[bytes];
      //WritableMemory wmem = WritableMemory.wrap(byteArr);
      hllSketch = new HllSketch(lgConfigK, tgtHllType, wmem);
      assertTrue(hllSketch.isEmpty());

      for (int i = 0; i < n; i++) {
        hllSketch.update(i);
      }
      //println(hllSketch.toString(true, true, false, false));
      assertFalse(hllSketch.isEmpty());
      assertEquals(hllSketch.getCurMode(), tgtMode);
      assertTrue(hllSketch.isMemory());
      assertTrue(hllSketch.isOffHeap());
      assertTrue(hllSketch.isSameResource(wmem));

      //convert direct sketch to byte[]
      barr1 = (compact) ? hllSketch.toCompactByteArray() : hllSketch.toUpdatableByteArray();
      //println(PreambleUtil.toString(barr1));
      hllSketch.reset();
      assertTrue(hllSketch.isEmpty());
    }

    //println("HEAP");
    HllSketch hllSketch2 = new HllSketch(lgConfigK, tgtHllType);
    for (int i = 0; i < n; i++) {
      hllSketch2.update(i);
    }
    //println(hllSketch2.toString(true, true, false, false));
    //println(PreambleUtil.toString(barr2));
    assertEquals(hllSketch2.getCurMode(), tgtMode);
    assertFalse(hllSketch2.isMemory());
    assertFalse(hllSketch2.isOffHeap());
    assertFalse(hllSketch2.isSameResource(wmem));
    byte[] barr2 = (compact) ? hllSketch2.toCompactByteArray() : hllSketch2.toUpdatableByteArray();
    assertEquals(barr1.length, barr2.length, barr1.length + ", " + barr2.length);
    //printDiffs(barr1, barr2);
    assertEquals(barr1, barr2);
  }

  @SuppressWarnings("unused") //only used when above printlns are enabled.
  private static void printDiffs(byte[] arr1, byte[] arr2) {
    int len1 = arr1.length;
    int len2 = arr2.length;
    int minLen = Math.min(len1,  len2);
    for (int i = 0; i < minLen; i++) {
      int v1 = arr1[i] & 0XFF;
      int v2 = arr2[i] & 0XFF;
      if (v1 == v2) { continue; }
      println(i + ", " + v1 + ", " + v2);
    }
  }

  @Test
  public void checkCouponToByteArray() {
    int lgK = 8;
    TgtHllType type = TgtHllType.HLL_8;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, type, wmem);
    int i;
    for (i = 0; i < 7; i++) { sk.update(i); } //LIST

    //toCompactMemArr from compact mem
    byte[] compactByteArr = sk.toCompactByteArray();
    Memory compactMem = Memory.wrap(compactByteArr);
    HllSketch skCompact = HllSketch.wrap(compactMem);
    byte[] compactByteArr2 = skCompact.toCompactByteArray();
    assertEquals(compactByteArr2, compactByteArr);

    //now check to UpdatableByteArr from compact mem
    byte[] updatableByteArr = sk.toUpdatableByteArray();
    byte[] updatableByteArr2 = skCompact.toUpdatableByteArray();
    assertEquals(updatableByteArr2.length, updatableByteArr.length);
    assertEquals(skCompact.getEstimate(), sk.getEstimate());


    sk.update(i); //SET
    //toCompactMemArr from compact mem
    compactByteArr = sk.toCompactByteArray();
    compactMem = Memory.wrap(compactByteArr);
    skCompact = HllSketch.wrap(compactMem);
    compactByteArr2 = skCompact.toCompactByteArray();
    assertEquals(compactByteArr2, compactByteArr);

    //now check to UpdatableByteArr from compact mem
    updatableByteArr = sk.toUpdatableByteArray();
    updatableByteArr2 = skCompact.toUpdatableByteArray();
    assertEquals(updatableByteArr2.length, updatableByteArr.length);
    assertEquals(skCompact.getEstimate(), sk.getEstimate());
  }

  @Test
  public void checkDirectGetCouponIntArr() {
    int lgK = 8;
    TgtHllType type = TgtHllType.HLL_8;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, type, wmem);
    AbstractCoupons absCoup = (AbstractCoupons)sk.hllSketchImpl;
    assertNull(absCoup.getCouponIntArr());
  }

  @Test
  public void checkBasicGetLgCouponArrInts() {
    int lgK = 8;
    TgtHllType type = TgtHllType.HLL_8;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, type, wmem);
    for (int i = 0; i < 7; i++) { sk.update(i); }
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 3);
    sk.update(7);
    assertEquals(sk.getCurMode(), CurMode.SET);
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 5);

    sk.reset();
    for (int i = 0; i < 7; i++) { sk.update(i); }
    byte lgArr = wmem.getByte(LG_ARR_BYTE);
    wmem.putByte(LG_ARR_BYTE, (byte) 0); //corrupt to 0
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 3);
    wmem.putByte(LG_ARR_BYTE, lgArr); //put correct value back
    sk.update(7);
    assertEquals(sk.getCurMode(), CurMode.SET);
    assertEquals(sk.hllSketchImpl.curMode, CurMode.SET);
    wmem.putByte(LG_ARR_BYTE, (byte) 0); //corrupt to 0 again
    assertEquals(((AbstractCoupons) sk.hllSketchImpl).getLgCouponArrInts(), 5);
  }

  @Test
  public void checkHeapifyGetLgCouponArrInts() {
    int lgK = 8;
    TgtHllType type = TgtHllType.HLL_8;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, type, wmem);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    assertEquals(sk.getCurMode(), CurMode.SET);
    double est1 = sk.getEstimate();

    wmem.putByte(LG_ARR_BYTE, (byte) 0); //corrupt to 0
    HllSketch sk2 = HllSketch.heapify(wmem);
    double est2 = sk2.getEstimate();
    assertEquals(est2, est1, 0.0);
    //println(sk2.toString(true, true, true, true));
    //println(PreambleUtil.toString(wmem));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
