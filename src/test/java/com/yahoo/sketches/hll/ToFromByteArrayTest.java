/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 *
 */
public class ToFromByteArrayTest {

  static final int[] nArr = new int[] {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};


  @Test
  public void checkToFromSketch1() {
    for (int i = 0; i < 10; i++) {
      int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom1(lgK, HLL_4, n);
        toFrom1(lgK, HLL_6, n);
        toFrom1(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  private static void toFrom1(int lgK, TgtHllType tgtHllType, int n) {
    HllSketch src = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      src.update(i);
    }
    //println("n: " + n + ", lgK: " + lgK + ", type: " + tgtHllType);
    //printSketch(src, "SRC");

    byte[] byteArr = src.toCompactByteArray();
    Memory mem = Memory.wrap(byteArr);
    HllSketch dst = HllSketch.heapify(mem);
    //printSketch(dst, "DST");

    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);
  }

  @Test
  public void checkToFromSketch2() {
    for (int i = 0; i < 10; i++) {
      int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom2(lgK, HLL_4, n);
        toFrom2(lgK, HLL_6, n);
        toFrom2(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  private static void toFrom2(int lgK, TgtHllType tgtHllType, int n) {
    HllSketch src = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      src.update(i);
    }
    //println("n: " + n + ", lgK: " + lgK + ", type: " + tgtHllType);
    //printSketch(src, "SRC");

    byte[] byteArr = src.toCompactByteArray();
    HllSketch dst = HllSketch.heapify(byteArr);
    //printSketch(dst, "DST");

    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);
  }

  static void printSketch(HllSketch sketch, String name) {
    println(name +":\n" + sketch.toString(true, false, true)); //summary + aux only
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
