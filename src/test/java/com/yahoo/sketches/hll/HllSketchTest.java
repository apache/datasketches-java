/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
//@SuppressWarnings("unused")
public class HllSketchTest {

  @Test
  public void checkCopies() {
    checkCopy(HLL_4);
    checkCopy(HLL_6);
    checkCopy(HLL_8);
  }

  public void checkCopy(TgtHllType tgtHllType) {
    HllSketch sk1;
    if (tgtHllType == HLL_4) {
      sk1 = new HllSketch(14, HLL_4);
    } else if (tgtHllType == HLL_6) {
      sk1 = new HllSketch(8, HLL_6);
    } else {
      sk1 = new HllSketch(8, HLL_8);
    }

    for (int i = 0; i < 7; i++) {
      sk1.update(i);
    }
    assertEquals(sk1.getCurrentMode(), CurMode.LIST);

    HllSketch sk2 = sk1.copy();
    assertEquals(sk2.getCurrentMode(), CurMode.LIST);
    HllSketchImpl impl1 = sk1.hllSketchImpl;

    HllSketchImpl impl2 = sk2.hllSketchImpl;
    AbstractCoupons absCoupons1 = (AbstractCoupons) sk1.hllSketchImpl;
    AbstractCoupons absCoupons2 = (AbstractCoupons) sk2.hllSketchImpl;
    assertEquals(absCoupons1.getCouponCount(), absCoupons2.getCouponCount());
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);

    for (int i = 7; i < 24; i++) {
      sk1.update(i);
    }
    assertEquals(sk1.getCurrentMode(), CurMode.SET);

    sk2 = sk1.copy();
    assertEquals(sk2.getCurrentMode(), CurMode.SET);
    impl1 = sk1.hllSketchImpl;

    impl2 = sk2.hllSketchImpl;
    absCoupons1 = (AbstractCoupons) sk1.hllSketchImpl;
    absCoupons2 = (AbstractCoupons) sk2.hllSketchImpl;
    assertEquals(absCoupons1.getCouponCount(), absCoupons2.getCouponCount());
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);
    final int u = (sk1.getTgtHllType() == TgtHllType.HLL_4) ? 100000 : 25;
    for (int i = 24; i < u; i++) {
      sk1.update(i);
    }
    sk1.getCompactSerializationBytes();
    assertEquals(sk1.getCurrentMode(), CurMode.HLL);

    sk2 = sk1.copy();
    assertEquals(sk2.getCurrentMode(), CurMode.HLL);
    impl1 = sk1.hllSketchImpl;

    impl2 = sk2.hllSketchImpl;
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);
  }

  @Test
  public void checkCopyAs() {
    copyAs(HLL_4, HLL_4);
    copyAs(HLL_4, HLL_6);
    copyAs(HLL_4, HLL_8);
    copyAs(HLL_6, HLL_4);
    copyAs(HLL_6, HLL_6);
    copyAs(HLL_6, HLL_8);
    copyAs(HLL_8, HLL_4);
    copyAs(HLL_8, HLL_6);
    copyAs(HLL_8, HLL_8);
  }

  public void copyAs(TgtHllType srcType, TgtHllType dstType) {
    int lgK = 8;
    int n1 = 15;
    int n2 = 24;
    int n3 = 1000;
    int base = 0;

    HllSketch src = new HllSketch(lgK, srcType);
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
  public void misc() {
    final int lgConfigK = 8;
    HllSketch sk = new HllSketch(lgConfigK, TgtHllType.HLL_8);
    for (int i = 0; i < 7; i++) { sk.update(i); } //LIST
    AbstractCoupons absCoupons = (AbstractCoupons) sk.hllSketchImpl;
    assertEquals(absCoupons.getCouponCount(), 7);
    assertEquals(sk.getCompactSerializationBytes(), 36);

    for (int i = 7; i < 24; i++) { sk.update(i); } //SET
    absCoupons = (AbstractCoupons) sk.hllSketchImpl;
    assertEquals(absCoupons.getCouponCount(), 24);
    assertEquals(sk.getCompactSerializationBytes(), 108);

    sk.update(24); //HLL
    AbstractHllArray absHll = (AbstractHllArray) sk.hllSketchImpl;
    assertNull(absHll.getAuxIterator());
    assertEquals(absHll.getCurMin(), 0);
    assertEquals(absHll.getHipAccum(), 25.0, 25 * .02);
    assertTrue(absHll.getNumAtCurMin() >= 0);

    final int hllBytes = PreambleUtil.HLL_BYTE_ARR_START + (1 << lgConfigK);
    assertEquals(sk.getCompactSerializationBytes(), hllBytes);
    assertEquals(BaseHllSketch.getMaxUpdatableSerializationBytes(lgConfigK, TgtHllType.HLL_8), hllBytes);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNumStdDev() {
    HllUtil.checkNumStdDev(0);
  }

  @Test
  public void checkSerSizes() {
    final int lgConfigK = 8;
    HllSketch sk = new HllSketch(lgConfigK, HLL_8);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    int hllBytes = PreambleUtil.HLL_BYTE_ARR_START + (1 << lgConfigK);
    assertEquals(sk.getCompactSerializationBytes(), hllBytes);
    assertEquals(BaseHllSketch.getMaxUpdatableSerializationBytes(lgConfigK, HLL_8), hllBytes);

    sk = new HllSketch(lgConfigK, HLL_6);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    hllBytes = PreambleUtil.HLL_BYTE_ARR_START + AbstractHllArray.hll6ArrBytes(lgConfigK);
    assertEquals(sk.getCompactSerializationBytes(), hllBytes);
    assertEquals(BaseHllSketch.getMaxUpdatableSerializationBytes(lgConfigK, HLL_6), hllBytes);

    sk = new HllSketch(lgConfigK, HLL_4);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    hllBytes = PreambleUtil.HLL_BYTE_ARR_START + (1 << (lgConfigK - 1));
    assertEquals(sk.getCompactSerializationBytes(), hllBytes);
    hllBytes += (4 << LG_AUX_ARR_INTS[lgConfigK]);
    assertEquals(BaseHllSketch.getMaxUpdatableSerializationBytes(lgConfigK, HLL_4), hllBytes);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkMisc() {
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
    HllSketch sk = new HllSketch(HllUtil.MAX_LOG_K);
    println(sk.toString(false, false, true, false));
  }

  @Test
  public void checkSizes() {
    int lgK = 6;
    HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_4);
    for (int i = 0; i < (1 << lgK); i++ ) {
      sk.update(i);
    }
    println(sk.toString(true, true, true, false));
    int bytes = sk.getCompactSerializationBytes();
    println("Size: " + bytes);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s); //disable here
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
}
