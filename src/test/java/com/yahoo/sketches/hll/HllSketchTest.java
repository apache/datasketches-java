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
    assertEquals(sk.getCurrentMode(), CurMode.LIST);

    HllSketch skCopy = sk.copy();
    assertEquals(skCopy.getCurrentMode(), CurMode.LIST);
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
    assertEquals(sk.getCurrentMode(), CurMode.SET);

    skCopy = sk.copy();
    assertEquals(skCopy.getCurrentMode(), CurMode.SET);
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
    assertEquals(sk.getCurrentMode(), CurMode.HLL);

    skCopy = sk.copy();
    assertEquals(skCopy.getCurrentMode(), CurMode.HLL);
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

  private static void checkSerSizes(int lgConfigK, TgtHllType tgtHllType, boolean direct) { //START HERE
    int bytes = getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = (direct)
        ? new HllSketch(lgConfigK, tgtHllType, wmem) : new HllSketch(lgConfigK, tgtHllType);
    int i;
    for (i = 0; i < 7; i++) { sk.update(i); }

    int expected = LIST_INT_ARR_START + (i << 2);
    assertEquals(sk.getCompactSerializationBytes(), expected);
    expected = LIST_INT_ARR_START + (4 << LG_INIT_LIST_SIZE);
    assertEquals(sk.getUpdatableSerializationBytes(), expected);

    for (i = 7; i < 24; i++) { sk.update(i); }
    expected = HASH_SET_INT_ARR_START + (i << 2);
    assertEquals(sk.getCompactSerializationBytes(), expected);
    expected = HASH_SET_INT_ARR_START + (4 << LG_INIT_SET_SIZE);
    assertEquals(sk.getUpdatableSerializationBytes(), expected);

    sk.update(i);
    AbstractHllArray absHll = (AbstractHllArray) sk.hllSketchImpl;
    AuxHashMap map =  absHll.auxHashMap;
    int auxCountBytes = 0;
    int auxArrBytes = 0;
    if (map != null) {
      auxCountBytes = map.getAuxCount() << 2;
      auxArrBytes = 4 << map.getLgAuxArrInts();
    }
    int hllArrBytes = hllArrBytes(lgConfigK, tgtHllType);
    expected = HLL_BYTE_ARR_START + hllArrBytes + auxCountBytes;
    assertEquals(sk.getCompactSerializationBytes(), expected);
    expected = HLL_BYTE_ARR_START + hllArrBytes + auxArrBytes;
    assertEquals(sk.getUpdatableSerializationBytes(), expected);
    int fullAuxBytes = (tgtHllType == TgtHllType.HLL_4) ? (4 << LG_AUX_ARR_INTS[lgConfigK]) : 0;
    expected = HLL_BYTE_ARR_START + hllArrBytes + fullAuxBytes;
    assertEquals(getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType), expected);
  }

  private static int hllArrBytes(int lgConfigK, TgtHllType tgtHllType) {
    if (tgtHllType == TgtHllType.HLL_4) {
      return AbstractHllArray.hll4ArrBytes(lgConfigK);
    }
    if (tgtHllType == TgtHllType.HLL_6) {
      return AbstractHllArray.hll6ArrBytes(lgConfigK);
    }
    return AbstractHllArray.hll8ArrBytes(lgConfigK);
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
  public void checkToString() {
    HllSketch sk = new HllSketch(15, TgtHllType.HLL_4);
    for (int i = 0; i < (1 << 20); i++) { sk.update(i); }
    sk.toString(false, true, true, true);
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
