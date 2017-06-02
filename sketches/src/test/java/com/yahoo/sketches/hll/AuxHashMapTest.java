/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 */
public class AuxHashMapTest {

  @Test
  public void exerciseAux() {
    HllSketch sk = new HllSketch(14, TgtHllType.HLL_4);
    for (int i = 0; i < (1 << 20); i++) { sk.update(i); }
    PairIterator itr = sk.getAuxIterator();
    while (itr.nextValid()) {
      println(itr.getString());
    }
    byte[] byteArr = sk.toByteArray();
    HllSketch sk2 = HllSketch.heapify(Memory.wrap(byteArr));
    assertEquals(sk.getEstimate(), sk2.getEstimate());

    PairIterator h4itr = sk.getIterator();
    println(h4itr.getHeader());
    while (h4itr.nextValid()) {
      if (h4itr.getValue() > 14) {
        println(h4itr.getString());
      }
    }
    h4itr = sk.getAuxIterator();
    println(h4itr.getHeader());
    while (h4itr.nextAll()) {
      if (h4itr.getValue() > 14) {
        println(h4itr.getString());
      }
    }
    println("CurMin: " + sk.hllSketchImpl.getCurMin());
    sk.toString(true);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
