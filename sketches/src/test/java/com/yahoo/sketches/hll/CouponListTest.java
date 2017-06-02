/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 *
 */
public class CouponListTest {

  @Test
  public void checkIterator() {
    HllSketch sk = new HllSketch(8);
    for (int i = 0; i < 15; i++) { sk.update(i); }
    PairIterator itr = sk.getIterator();
    println(itr.getHeader());
    while (itr.nextAll()) {
      int key = itr.getKey();
      int val = itr.getValue();
      int idx = itr.getIndex();
      println("Idx: " + idx + ", Key: " + key + ", Val: " + val);
    }
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkPutHipAccum() {
    HllSketch sk = new HllSketch(8);
    for (int i = 0; i < 15; i++) { sk.update(i); }
    sk.hllSketchImpl.putHipAccum(0);
  }


  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
}
