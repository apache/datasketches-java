/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.FAMILY_ID;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.SER_VER;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 *
 */
public class HllArrayTest {

  @Test
  public void checkCheckPreamble() {
    HllSketch sk = new HllSketch(7, TgtHllType.HLL_6);
    for (int i = 0; i < 100; i++) { sk.update(i); }
    byte[] byteArr = sk.toCompactByteArray();
    WritableMemory wmem = WritableMemory.wrap(byteArr);
    final long memAdd = wmem.getCumulativeOffset(0);
    HllArray.checkPreamble(wmem, byteArr, memAdd);
    try {
      wmem.putByte(PreambleUtil.PREAMBLE_INTS_BYTE, (byte) 0);
      HllArray.checkPreamble(wmem, byteArr, memAdd);
      fail();
    } catch (SketchesArgumentException e) {
      wmem.putByte(PreambleUtil.PREAMBLE_INTS_BYTE, (byte) HLL_PREINTS);
    }
    try {
      wmem.putByte(PreambleUtil.SER_VER_BYTE, (byte) 0);
      HllArray.checkPreamble(wmem, byteArr, memAdd);
      fail();
    } catch (SketchesArgumentException e) {
      wmem.putByte(PreambleUtil.SER_VER_BYTE, (byte) SER_VER);
    }
    try {
      wmem.putByte(PreambleUtil.FAMILY_BYTE, (byte) 0);
      HllArray.checkPreamble(wmem, byteArr, memAdd);
      fail();
    } catch (SketchesArgumentException e) {
      wmem.putByte(PreambleUtil.FAMILY_BYTE, (byte) FAMILY_ID);
    }
  }

  @Test
  public void checkCompositeEst() {
    testComposite(4, TgtHllType.HLL_8, 1000);
    testComposite(5, TgtHllType.HLL_8, 1000);
    testComposite(6, TgtHllType.HLL_8, 1000);
    testComposite(13, TgtHllType.HLL_8, 10000);
  }

  @Test
  public void checkBigHipGetRse() {
    HllSketch sk = new HllSketch(13, TgtHllType.HLL_8);
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    sk.getRse(1);
    sk.getRseFactor(1);
  }

  private static void testComposite(int lgK, TgtHllType tgtHllType, int n) {
    Union u = new Union(lgK);
    HllSketch sk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      u.update(i);
      sk.update(i);
    }
    u.update(sk); //merge
    HllSketch res = u.getResult(TgtHllType.HLL_8);
    res.getCompositeEstimate();
    res.getRse(1);
    res.getRseFactor(1);
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
