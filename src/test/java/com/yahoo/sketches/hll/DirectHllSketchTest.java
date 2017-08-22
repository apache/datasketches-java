/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class DirectHllSketchTest {

  @Test
  public void checkNoWriteAccess() {
    noWriteAccess(TgtHllType.HLL_4, 7);
    noWriteAccess(TgtHllType.HLL_4, 24);
    noWriteAccess(TgtHllType.HLL_4, 25);
    noWriteAccess(TgtHllType.HLL_6, 25);
    noWriteAccess(TgtHllType.HLL_8, 25);
  }

  private static void noWriteAccess(TgtHllType tgtHllType, int n) {
    int lgConfigK = 8;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgConfigK, tgtHllType, wmem);

    for (int i = 0; i < n; i++) { sk.update(i); }

    HllSketch sk2 = HllSketch.wrap(wmem);
    try {
      sk2.update(1);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkPutSlotGetSlotDummies() {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_8);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(4, TgtHllType.HLL_8, wmem);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    AbstractHllArray absArr = (AbstractHllArray) sk.hllSketchImpl;
    assertEquals(absArr.getSlot(0), -1);
    absArr.putSlot(0, -1); //a no-op
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
