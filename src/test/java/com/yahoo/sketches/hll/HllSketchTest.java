/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllSketch.getMaxUpdatableSerializationBytes;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_LIST_SIZE;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static com.yahoo.sketches.hll.PreambleUtil.HASH_SET_INT_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.LIST_INT_ARR_START;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
//@SuppressWarnings("unused")
public class HllSketchTest {

  @Test
  public void checkCopies() {
    runCheckCopy(14, HLL_4, null);
    runCheckCopy(8, HLL_6, null);
    runCheckCopy(8, HLL_8, null);

    int bytes = getMaxUpdatableSerializationBytes(14, TgtHllType.HLL_8);
    WritableMemory wmem = WritableMemory.allocate(bytes);

    runCheckCopy(14, HLL_4, wmem);
    runCheckCopy(8, HLL_6, wmem);
    runCheckCopy(8, HLL_8, wmem);
  }

  private static void runCheckCopy(int lgConfigK, TgtHllType tgtHllType, WritableMemory wmem) {
    HllSketch sk;
    if (wmem == null) { //heap
      sk = new HllSketch(lgConfigK, tgtHllType);
    } else { //direct
      sk = new HllSketch(lgConfigK, tgtHllType, wmem);
    }

    for (int i = 0; i < 7; i++) {
      sk.update(i);
    }
    assertEquals(sk.getCurMode(), CurMode.LIST);

    HllSketch skCopy = sk.copy();
    assertEquals(skCopy.getCurMode(), CurMode.LIST);
    HllSketchImpl impl1 = sk.hllSketchImpl;

    HllSketchImpl impl2 = skCopy.hllSketchImpl;
    AbstractCoupons absCoupons1 = (AbstractCoupons) sk.hllSketchImpl;
    AbstractCoupons absCoupons2 = (AbstractCoupons) skCopy.hllSketchImpl;
    assertEquals(absCoupons1.getCouponCount(), absCoupons2.getCouponCount());
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);

    for (int i = 7; i < 24; i++) {
      sk.update(i);
    }
    assertEquals(sk.getCurMode(), CurMode.SET);

    skCopy = sk.copy();
    assertEquals(skCopy.getCurMode(), CurMode.SET);
    impl1 = sk.hllSketchImpl;

    impl2 = skCopy.hllSketchImpl;
    absCoupons1 = (AbstractCoupons) sk.hllSketchImpl;
    absCoupons2 = (AbstractCoupons) skCopy.hllSketchImpl;
    assertEquals(absCoupons1.getCouponCount(), absCoupons2.getCouponCount());
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);
    final int u = (sk.getTgtHllType() == TgtHllType.HLL_4) ? 100000 : 25;
    for (int i = 24; i < u; i++) {
      sk.update(i);
    }
    sk.getCompactSerializationBytes();
    assertEquals(sk.getCurMode(), CurMode.HLL);

    skCopy = sk.copy();
    assertEquals(skCopy.getCurMode(), CurMode.HLL);
    impl1 = sk.hllSketchImpl;

    impl2 = skCopy.hllSketchImpl;
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);
  }

  @Test
  public void checkCopyAs() {
    copyAs(HLL_4, HLL_4, false);
    copyAs(HLL_4, HLL_6, false);
    copyAs(HLL_4, HLL_8, false);
    copyAs(HLL_6, HLL_4, false);
    copyAs(HLL_6, HLL_6, false);
    copyAs(HLL_6, HLL_8, false);
    copyAs(HLL_8, HLL_4, false);
    copyAs(HLL_8, HLL_6, false);
    copyAs(HLL_8, HLL_8, false);
    copyAs(HLL_4, HLL_4, true);
    copyAs(HLL_4, HLL_6, true);
    copyAs(HLL_4, HLL_8, true);
    copyAs(HLL_6, HLL_4, true);
    copyAs(HLL_6, HLL_6, true);
    copyAs(HLL_6, HLL_8, true);
    copyAs(HLL_8, HLL_4, true);
    copyAs(HLL_8, HLL_6, true);
    copyAs(HLL_8, HLL_8, true);
  }

  private static void copyAs(TgtHllType srcType, TgtHllType dstType, boolean direct) {
    int lgK = 8;
    int n1 = 7;
    int n2 = 24;
    int n3 = 1000;
    int base = 0;
    int bytes = getMaxUpdatableSerializationBytes(lgK, srcType);
    WritableMemory wmem = WritableMemory.allocate(bytes);

    HllSketch src = (direct) ? new HllSketch(lgK, srcType, wmem) : new HllSketch(lgK, srcType);
    for (int i = 0; i < n1; i++) { src.update(i + base); }
    HllSketch dst = src.copyAs(dstType);
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);

    for (int i = n1; i < n2; i++) { src.update(i); }
    dst = src.copyAs(dstType);
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);

    for (int i = n2; i < n3; i++) { src.update(i); }
    dst = src.copyAs(dstType);
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);
  }

  @Test
  public void checkMisc1() {
    misc(false);
    misc(true);
  }

  private static void misc(boolean direct) {
    int lgConfigK = 8;
    TgtHllType srcType = TgtHllType.HLL_8;
    int bytes = getMaxUpdatableSerializationBytes(lgConfigK, srcType);
    WritableMemory wmem = WritableMemory.allocate(bytes);

    HllSketch sk = (direct)
        ? new HllSketch(lgConfigK, srcType, wmem) : new HllSketch(lgConfigK, srcType);

    for (int i = 0; i < 7; i++) { sk.update(i); } //LIST
    AbstractCoupons absCoupons = (AbstractCoupons) sk.hllSketchImpl;
    assertEquals(absCoupons.getCouponCount(), 7);
    assertEquals(sk.getCompactSerializationBytes(), 36);
    assertEquals(sk.getUpdatableSerializationBytes(), 40);

    for (int i = 7; i < 24; i++) { sk.update(i); } //SET
    absCoupons = (AbstractCoupons) sk.hllSketchImpl;
    assertEquals(absCoupons.getCouponCount(), 24);
    assertEquals(sk.getCompactSerializationBytes(), 108);
    assertEquals(sk.getUpdatableSerializationBytes(), 140);

    sk.update(24); //HLL
    AbstractHllArray absHll = (AbstractHllArray) sk.hllSketchImpl;
    assertNull(absHll.getAuxIterator());
    assertEquals(absHll.getCurMin(), 0);
    assertEquals(absHll.getHipAccum(), 25.0, 25 * .02);
    assertTrue(absHll.getNumAtCurMin() >= 0);
    assertEquals(sk.getUpdatableSerializationBytes(), 40 + 256);
    assertEquals(absHll.getMemDataStart(), 40);
    assertEquals(absHll.getPreInts(), 10);


    final int hllBytes = PreambleUtil.HLL_BYTE_ARR_START + (1 << lgConfigK);
    assertEquals(sk.getCompactSerializationBytes(), hllBytes);
    assertEquals(getMaxUpdatableSerializationBytes(lgConfigK, TgtHllType.HLL_8), hllBytes);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNumStdDev() {
    HllUtil.checkNumStdDev(0);
  }

  @Test
  public void checkSerSizes() {
    checkSerSizes(8, TgtHllType.HLL_8, false);
    checkSerSizes(8, TgtHllType.HLL_8, true);
    checkSerSizes(8, TgtHllType.HLL_6, false);
    checkSerSizes(8, TgtHllType.HLL_6, true);
    checkSerSizes(8, TgtHllType.HLL_4, false);
    checkSerSizes(8, TgtHllType.HLL_4, true);
  }

  private static void checkSerSizes(int lgConfigK, TgtHllType tgtHllType, boolean direct) {
    int bytes = getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = (direct)
        ? new HllSketch(lgConfigK, tgtHllType, wmem) : new HllSketch(lgConfigK, tgtHllType);
    int i;

    //LIST
    for (i = 0; i < 7; i++) { sk.update(i); }
    int expected = LIST_INT_ARR_START + (i << 2);
    assertEquals(sk.getCompactSerializationBytes(), expected);
    expected = LIST_INT_ARR_START + (4 << LG_INIT_LIST_SIZE);
    assertEquals(sk.getUpdatableSerializationBytes(), expected);

    //SET
    for (i = 7; i < 24; i++) { sk.update(i); }
    expected = HASH_SET_INT_ARR_START + (i << 2);
    assertEquals(sk.getCompactSerializationBytes(), expected);
    expected = HASH_SET_INT_ARR_START + (4 << LG_INIT_SET_SIZE);
    assertEquals(sk.getUpdatableSerializationBytes(), expected);

    //HLL
    sk.update(i);
    assertEquals(sk.getCurMode(), CurMode.HLL);
    AbstractHllArray absHll = (AbstractHllArray) sk.hllSketchImpl;

    int auxCountBytes = 0;
    int auxArrBytes = 0;
    if (absHll.tgtHllType == HLL_4) {
      AuxHashMap auxMap =  absHll.getAuxHashMap();
      if (auxMap != null) {
        auxCountBytes = auxMap.getAuxCount() << 2;
        auxArrBytes = 4 << auxMap.getLgAuxArrInts();
      } else {
        auxArrBytes = 4 << LG_AUX_ARR_INTS[lgConfigK];
      }
    }
    int hllArrBytes = absHll.getHllByteArrBytes();
    expected = HLL_BYTE_ARR_START + hllArrBytes + auxCountBytes;
    assertEquals(sk.getCompactSerializationBytes(), expected);
    expected = HLL_BYTE_ARR_START + hllArrBytes + auxArrBytes;
    assertEquals(sk.getUpdatableSerializationBytes(), expected);
    int fullAuxBytes = (tgtHllType == TgtHllType.HLL_4) ? (4 << LG_AUX_ARR_INTS[lgConfigK]) : 0;
    expected = HLL_BYTE_ARR_START + hllArrBytes + fullAuxBytes;
    assertEquals(getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType), expected);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkConfigKLimits() {
    try {
      HllSketch sk = new HllSketch(HllUtil.MIN_LOG_K - 1);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
    try {
      HllSketch sk = new HllSketch(HllUtil.MAX_LOG_K + 1);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void exerciseToString() {
    HllSketch sk = new HllSketch(15, TgtHllType.HLL_4);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    println(sk.toString(false, true, true, true));
    for (int i = 25; i < (1 << 20); i++) { sk.update(i); }
    println(sk.toString(false, true, true, true));
    println(sk.toString(false, true, true, false));
    println(sk.toString(false, true, true));
    sk = new HllSketch(8, TgtHllType.HLL_6);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    println(sk.toString(false, true, true, true));
  }

  @SuppressWarnings("unused")
  @Test
  public void checkMemoryNotLargeEnough() {
    int bytes = getMaxUpdatableSerializationBytes(8, TgtHllType.HLL_8);
    WritableMemory wmem = WritableMemory.allocate(bytes -1);
    try {
      HllSketch sk = new HllSketch(8, TgtHllType.HLL_8, wmem);
      fail();
    } catch (SketchesArgumentException e) {
      //OK
    }
  }

  @Test
  public void checkEmptyCoupon() {
    int lgK = 8;
    TgtHllType type = TgtHllType.HLL_8;
    HllSketch sk = new HllSketch(lgK, type);
    for (int i = 0; i < 20; i++) { sk.update(i); } //SET mode
    sk.couponUpdate(0);
    assertEquals(sk.getEstimate(), 20.0, 0.001);
  }

  @Test
  public void checkCompactFlag() {
    int lgK = 8;
    //LIST: follows the toByteArray request
    assertEquals(checkCompact(lgK, 7, HLL_8, false, false), false);
    assertEquals(checkCompact(lgK, 7, HLL_8, false, true), true);
    assertEquals(checkCompact(lgK, 7, HLL_8, false, false), false);
    assertEquals(checkCompact(lgK, 7, HLL_8, false, true), true);
    assertEquals(checkCompact(lgK, 7, HLL_8, true, false), false);
    assertEquals(checkCompact(lgK, 7, HLL_8, true, true), true);
    assertEquals(checkCompact(lgK, 7, HLL_8, true, false), false);
    assertEquals(checkCompact(lgK, 7, HLL_8, true, true), true);

    //SET: follows the toByteArray request
    assertEquals(checkCompact(lgK, 24, HLL_8, false, false), false);
    assertEquals(checkCompact(lgK, 24, HLL_8, false, true), true);
    assertEquals(checkCompact(lgK, 24, HLL_8, false, false), false);
    assertEquals(checkCompact(lgK, 24, HLL_8, false, true), true);
    assertEquals(checkCompact(lgK, 24, HLL_8, true, false), false);
    assertEquals(checkCompact(lgK, 24, HLL_8, true, true), true);
    assertEquals(checkCompact(lgK, 24, HLL_8, true, false), false);
    assertEquals(checkCompact(lgK, 24, HLL_8, true, true), true);

    //HLL8: always updatable
    assertEquals(checkCompact(lgK, 25, HLL_8, false, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_8, false, true), false);
    assertEquals(checkCompact(lgK, 25, HLL_8, false, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_8, false, true), false);
    assertEquals(checkCompact(lgK, 25, HLL_8, true, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_8, true, true), false);
    assertEquals(checkCompact(lgK, 25, HLL_8, true, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_8, true, true), false);

    //HLL6: always updatable
    assertEquals(checkCompact(lgK, 25, HLL_6, false, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_6, false, true), false);
    assertEquals(checkCompact(lgK, 25, HLL_6, false, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_6, false, true), false);
    assertEquals(checkCompact(lgK, 25, HLL_6, true, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_6, true, true), false);
    assertEquals(checkCompact(lgK, 25, HLL_6, true, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_6, true, true), false);

    //HLL:4 follows the toByteArray request
    assertEquals(checkCompact(lgK, 25, HLL_4, false, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_4, false, true), true);
    assertEquals(checkCompact(lgK, 25, HLL_4, false, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_4, false, true), true);
    assertEquals(checkCompact(lgK, 25, HLL_4, true, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_4, true, true), true);
    assertEquals(checkCompact(lgK, 25, HLL_4, true, false), false);
    assertEquals(checkCompact(lgK, 25, HLL_4, true, true), true);
  }

  //Creates either a direct or heap sketch,
  // Serializes to either compact or updatable form.
  // Confirms the isMemory() for direct, isOffHeap(), and the
  // get compact or updatable serialization bytes.
  // Returns true if the compact flag is set.
  private static boolean checkCompact(int lgK, int n, TgtHllType type, boolean direct,
      boolean compact) {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = (direct) ? new HllSketch(lgK, type, wmem) : new HllSketch(lgK, type);
    assertEquals(sk.isMemory(), direct);
    assertFalse(sk.isOffHeap());
    for (int i = 0; i < n; i++) { sk.update(i); } //LOAD
    byte[] byteArr = (compact) ? sk.toCompactByteArray() : sk.toUpdatableByteArray();
    int len = byteArr.length;
    if (compact) {
      assertEquals(len, sk.getCompactSerializationBytes());
    } else {
      assertEquals(len, sk.getUpdatableSerializationBytes());
    }
    HllSketch sk2 = HllSketch.wrap(Memory.wrap(byteArr));
    assertEquals(sk2.getEstimate(), n, .01);
    boolean resourceCompact = sk2.isCompact();
    if (resourceCompact) {
      try {
        HllSketch.writableWrap(WritableMemory.wrap(byteArr));
        fail();
      } catch (SketchesArgumentException e) {
        //OK
      }
    }
    return resourceCompact;
    //return (byteArr[5] & COMPACT_FLAG_MASK) > 0;
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWritableWrapOfCompact() {
    HllSketch sk = new HllSketch();
    byte[] byteArr = sk.toCompactByteArray();
    WritableMemory wmem = WritableMemory.wrap(byteArr);
    HllSketch sk2 = HllSketch.writableWrap(wmem);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    //System.out.print(s); //disable here
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
}
