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

package org.apache.datasketches.hll2;

import static org.apache.datasketches.hll2.HllSketch.getMaxUpdatableSerializationBytes;
import static org.apache.datasketches.hll2.HllUtil.LG_AUX_ARR_INTS;
import static org.apache.datasketches.hll2.HllUtil.LG_INIT_LIST_SIZE;
import static org.apache.datasketches.hll2.HllUtil.LG_INIT_SET_SIZE;
import static org.apache.datasketches.hll2.PreambleUtil.HASH_SET_INT_ARR_START;
import static org.apache.datasketches.hll2.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll2.PreambleUtil.LIST_INT_ARR_START;
import static org.apache.datasketches.hll2.TgtHllType.HLL_4;
import static org.apache.datasketches.hll2.TgtHllType.HLL_6;
import static org.apache.datasketches.hll2.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class HllSketchTest {

  @Test
  public void checkCopies() {
    runCheckCopy(14, HLL_4, null);
    runCheckCopy(8, HLL_6, null);
    runCheckCopy(8, HLL_8, null);

    final int bytes = getMaxUpdatableSerializationBytes(14, TgtHllType.HLL_8);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);

    runCheckCopy(14, HLL_4, wseg);
    runCheckCopy(8, HLL_6, wseg);
    runCheckCopy(8, HLL_8, wseg);
  }

  private static void runCheckCopy(final int lgConfigK, final TgtHllType tgtHllType, final MemorySegment wseg) {
    HllSketch sk;
    if (wseg == null) { //heap
      sk = new HllSketch(lgConfigK, tgtHllType);
    } else { //direct
      sk = new HllSketch(lgConfigK, tgtHllType, wseg);
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

  private static void copyAs(final TgtHllType srcType, final TgtHllType dstType, final boolean direct) {
    final int lgK = 8;
    final int n1 = 7;
    final int n2 = 24;
    final int n3 = 1000;
    final int base = 0;
    final int bytes = getMaxUpdatableSerializationBytes(lgK, srcType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);

    final HllSketch src = (direct) ? new HllSketch(lgK, srcType, wseg) : new HllSketch(lgK, srcType);
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

  private static void misc(final boolean direct) {
    final int lgConfigK = 8;
    final TgtHllType srcType = TgtHllType.HLL_8;
    final int bytes = getMaxUpdatableSerializationBytes(lgConfigK, srcType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);

    final HllSketch sk = (direct)
        ? new HllSketch(lgConfigK, srcType, wseg) : new HllSketch(lgConfigK, srcType);

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
    final AbstractHllArray absHll = (AbstractHllArray) sk.hllSketchImpl;
    assertNull(absHll.getAuxIterator());
    assertEquals(absHll.getCurMin(), 0);
    assertEquals(absHll.getHipAccum(), 25.0, 25 * .02);
    assertTrue(absHll.getNumAtCurMin() >= 0);
    assertEquals(sk.getUpdatableSerializationBytes(), 40 + 256);
    assertEquals(absHll.getSegDataStart(), 40);
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

  private static void checkSerSizes(final int lgConfigK, final TgtHllType tgtHllType, final boolean direct) {
    final int bytes = getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = (direct)
        ? new HllSketch(lgConfigK, tgtHllType, wseg) : new HllSketch(lgConfigK, tgtHllType);
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
    final AbstractHllArray absHll = (AbstractHllArray) sk.hllSketchImpl;

    int auxCountBytes = 0;
    int auxArrBytes = 0;
    if (absHll.tgtHllType == HLL_4) {
      final AuxHashMap auxMap =  absHll.getAuxHashMap();
      if (auxMap != null) {
        auxCountBytes = auxMap.getAuxCount() << 2;
        auxArrBytes = 4 << auxMap.getLgAuxArrInts();
      } else {
        auxArrBytes = 4 << LG_AUX_ARR_INTS[lgConfigK];
      }
    }
    final int hllArrBytes = absHll.getHllByteArrBytes();
    expected = HLL_BYTE_ARR_START + hllArrBytes + auxCountBytes;
    assertEquals(sk.getCompactSerializationBytes(), expected);
    expected = HLL_BYTE_ARR_START + hllArrBytes + auxArrBytes;
    assertEquals(sk.getUpdatableSerializationBytes(), expected);
    final int fullAuxBytes = (tgtHllType == TgtHllType.HLL_4) ? (4 << LG_AUX_ARR_INTS[lgConfigK]) : 0;
    expected = HLL_BYTE_ARR_START + hllArrBytes + fullAuxBytes;
    assertEquals(getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType), expected);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkConfigKLimits() {
    try {
      final HllSketch sk = new HllSketch(HllUtil.MIN_LOG_K - 1);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
    try {
      final HllSketch sk = new HllSketch(HllUtil.MAX_LOG_K + 1);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void exerciseToStringDetails() {
    HllSketch sk = new HllSketch(15, TgtHllType.HLL_4);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    println(sk.toString(false, true, true, true)); //SET mode
    for (int i = 25; i < (1 << 12); i++) { sk.update(i); }
    println(sk.toString(false, true, true, true)); //HLL mode no Aux
    for (int i = (1 << 12); i < (1 << 15); i++) { sk.update(i); } //Aux with exceptions
    println(sk.toString(false, true, true, true));
    println(sk.toString(false, true, true, false));
    println(sk.toString(false, true, true));
    sk = new HllSketch(8, TgtHllType.HLL_6);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    println(sk.toString(false, true, true, true));
  }

  @SuppressWarnings("unused")
  @Test
  public void checkMemorySegmentNotLargeEnough() {
    final int bytes = getMaxUpdatableSerializationBytes(8, TgtHllType.HLL_8);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes -1]);
    try {
      final HllSketch sk = new HllSketch(8, TgtHllType.HLL_8, wseg);
      fail();
    } catch (final SketchesArgumentException e) {
      //OK
    }
  }

  @Test
  public void checkEmptyCoupon() {
    final int lgK = 8;
    final TgtHllType type = TgtHllType.HLL_8;
    final HllSketch sk = new HllSketch(lgK, type);
    for (int i = 0; i < 20; i++) { sk.update(i); } //SET mode
    sk.couponUpdate(0);
    assertEquals(sk.getEstimate(), 20.0, 0.001);
  }

  @Test
  public void checkCompactFlag() {
    final int lgK = 8;
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
  // Confirms the hasMemorySegment() for direct, isOffHeap(), and the
  // get compact or updatable serialization bytes.
  // Returns true if the compact flag is set.
  private static boolean checkCompact(final int lgK, final int n, final TgtHllType type, final boolean direct,
      final boolean compact) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = (direct) ? new HllSketch(lgK, type, wseg) : new HllSketch(lgK, type);
    assertEquals(sk.hasMemorySegment(), direct);
    assertFalse(sk.isOffHeap());
    for (int i = 0; i < n; i++) { sk.update(i); } //LOAD
    final byte[] byteArr = (compact) ? sk.toCompactByteArray() : sk.toUpdatableByteArray();
    final int len = byteArr.length;
    if (compact) {
      assertEquals(len, sk.getCompactSerializationBytes());
    } else {
      assertEquals(len, sk.getUpdatableSerializationBytes());
    }
    final HllSketch sk2 = HllSketch.wrap(MemorySegment.ofArray(byteArr));
    assertEquals(sk2.getEstimate(), n, .01);
    final boolean resourceCompact = sk2.isCompact();
    if (resourceCompact) {
      try {
        HllSketch.writableWrap(MemorySegment.ofArray(byteArr));
        fail();
      } catch (final SketchesArgumentException e) {
        //OK
      }
    }
    return resourceCompact;
    //return (byteArr[5] & COMPACT_FLAG_MASK) > 0;
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWritableWrapOfCompact() {
    final HllSketch sk = new HllSketch();
    final byte[] byteArr = sk.toCompactByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    final HllSketch sk2 = HllSketch.writableWrap(wseg);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkJavadocExample() {
    Union union; HllSketch sk, sk2;
    final int lgK = 12;
    sk = new HllSketch(lgK, TgtHllType.HLL_4); //can be 4, 6, or 8
    for (int i = 0; i < (2 << lgK); i++) { sk.update(i); }
    final byte[] arr = sk.toCompactByteArray();
    //  ...
    union = Union.heapify(arr); //initializes the union using data from the array.
    //OR, if used in an off-heap environment:
    union = Union.heapify(MemorySegment.ofArray(arr));

    //To recover an updatable Heap sketch:
    sk2 = HllSketch.heapify(arr);
    //OR, if used in an off-heap environment:
    sk2 = HllSketch.heapify(MemorySegment.ofArray(arr));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void print(final String s) {
    //System.out.print(s); //disable here
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }
}
