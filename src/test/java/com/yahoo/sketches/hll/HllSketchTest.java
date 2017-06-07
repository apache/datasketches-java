/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

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
      sk1 = new HllSketch(8, HLL_4);
    } else if (tgtHllType == HLL_6) {
      sk1 = new HllSketch(8, HLL_6);
    } else {
      sk1 = new HllSketch(8, HLL_8);
    }

    for (int i = 0; i < 15; i++) {
      sk1.update(i);
    }
    assertEquals(sk1.getCurrentMode(), CurMode.LIST);

    HllSketch sk2 = sk1.copy();
    assertEquals(sk2.getCurrentMode(), CurMode.LIST);
    HllSketchImpl impl1 = sk1.hllSketchImpl;

    HllSketchImpl impl2 = sk2.hllSketchImpl;
    assertEquals(impl1.getCount(), impl2.getCount());
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);

    for (int i = 15; i < 24; i++) {
      sk1.update(i);
    }
    assertEquals(sk1.getCurrentMode(), CurMode.SET);

    sk2 = sk1.copy();
    assertEquals(sk2.getCurrentMode(), CurMode.SET);
    impl1 = sk1.hllSketchImpl;

    impl2 = sk2.hllSketchImpl;
    assertEquals(impl1.getCount(), impl2.getCount());
    assertEquals(impl1.getEstimate(), impl2.getEstimate(), 0.0);
    assertFalse(impl1 == impl2);

    for (int i = 24; i < 30; i++) {
      sk1.update(i);
    }
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
//    println(src.getTgtHllType() + " " + src.getEstimate()
//      + ", " + dst.getTgtHllType() + " " + dst.getEstimate());

    for (int i = n1; i < n2; i++) { src.update(i); }
    dst = src.copyAs(dstType);
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);
//    println(src.getTgtHllType() + " " + src.getEstimate()
//    + ", " + dst.getTgtHllType() + " " + dst.getEstimate());

    for (int i = n2; i < n3; i++) { src.update(i); }
    dst = src.copyAs(dstType);
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);
//    println(src.getTgtHllType() + " " + src.getEstimate()
//    + ", " + dst.getTgtHllType() + " " + dst.getEstimate());
//    println("");
  }

  @Test
  public void misc() {
    final int lgConfigK = 8;
    HllSketch sk = new HllSketch(lgConfigK, TgtHllType.HLL_8);
    for (int i = 0; i < 7; i++) { sk.update(i); } //LIST
    assertNull(sk.getAuxIterator());
    assertEquals(sk.getCurMin(), -1);
    assertEquals(sk.hllSketchImpl.getHipAccum(), 7.0, .001);
    assertEquals(sk.hllSketchImpl.getMaxCouponArrInts(), 16);
    assertEquals(sk.hllSketchImpl.getNumAtCurMin(), -1);
    assertEquals(sk.hllSketchImpl.getCount(), 7);
    assertEquals(sk.getCurrentSerializationBytes(), 36);
    assertNull(sk.getAuxIterator());

    for (int i = 7; i < 24; i++) { sk.update(i); } //SET
    assertNull(sk.getAuxIterator());
    assertEquals(sk.getCurMin(), -1);
    assertEquals(sk.hllSketchImpl.getHipAccum(), 24.0, .001);
    assertEquals(sk.hllSketchImpl.getMaxCouponArrInts(), 32);
    assertEquals(sk.hllSketchImpl.getNumAtCurMin(), -1);
    assertEquals(sk.hllSketchImpl.getCount(), 24);
    assertEquals(sk.getCurrentSerializationBytes(), 108);
    assertNull(sk.getAuxIterator());

    sk.update(24); //HLL
    assertNull(sk.getAuxIterator());
    assertEquals(sk.getCurMin(), 0);
    assertEquals(sk.hllSketchImpl.getHipAccum(), 25.0, 25 * .02);
    assertEquals(sk.hllSketchImpl.getMaxCouponArrInts(), -1);
    assertTrue(sk.hllSketchImpl.getNumAtCurMin() >= 0);
    assertEquals(sk.hllSketchImpl.getCount(), -1);
    final int hllBytes = PreambleUtil.HLL_BYTE_ARRAY_START + (1 << lgConfigK);
    assertEquals(sk.getCurrentSerializationBytes(), hllBytes);
    assertEquals(HllSketch.getMaxSerializationBytes(lgConfigK, TgtHllType.HLL_8), hllBytes);
    assertNull(sk.getAuxIterator());
  }

  @Test
  public void checkSerSizes() {
    final int lgConfigK = 8;
    HllSketch sk = new HllSketch(lgConfigK, HLL_8);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    int hllBytes = PreambleUtil.HLL_BYTE_ARRAY_START + (1 << lgConfigK);
    assertEquals(sk.getCurrentSerializationBytes(), hllBytes);
    assertEquals(HllSketch.getMaxSerializationBytes(lgConfigK, HLL_8), hllBytes);

    sk = new HllSketch(lgConfigK, HLL_6);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    hllBytes = PreambleUtil.HLL_BYTE_ARRAY_START + Hll6Array.byteArrBytes(lgConfigK);
    assertEquals(sk.getCurrentSerializationBytes(), hllBytes);
    assertEquals(HllSketch.getMaxSerializationBytes(lgConfigK, HLL_6), hllBytes);

    sk = new HllSketch(lgConfigK, HLL_4);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    hllBytes = PreambleUtil.HLL_BYTE_ARRAY_START + (1 << (lgConfigK - 1));
    assertEquals(sk.getCurrentSerializationBytes(), hllBytes);
    hllBytes += (4 << Hll4Array.getExpectedLgAuxInts(lgConfigK));
    assertEquals(HllSketch.getMaxSerializationBytes(lgConfigK, HLL_4), hllBytes);
  }

  static void print(String s) {
    System.out.print(s);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.println(s); //disable here
  }
}
