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

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class CrossCountingTest {
  static final String LS = System.getProperty("line.separator");

  @Test
  public void crossCountingChecks() {
    crossCountingCheck(4, 100);
    crossCountingCheck(4, 10000);
    crossCountingCheck(12, 7);
    crossCountingCheck(12, 384);
    crossCountingCheck(12, 10000);
  }

  void crossCountingCheck(int lgK, int n) {
    HllSketch sk4 = buildSketch(n, lgK, HLL_4);
    int s4csum = computeChecksum(sk4);
    //println(sk4.toString(true, true, true, true));
    int csum;

    HllSketch sk6 = buildSketch(n, lgK, HLL_6);
    csum = computeChecksum(sk6);
    assertEquals(csum, s4csum);
    //println(sk6.toString(true, true, true, true));

    HllSketch sk8 = buildSketch(n, lgK, HLL_8);
    csum = computeChecksum(sk8);
    assertEquals(csum, s4csum);
    //println(sk8.toString(true, true, true, true));

    //Conversions
//    println("\nConverted HLL_6 to HLL_4:");
    HllSketch sk6to4 = sk6.copyAs(HLL_4);
    csum = computeChecksum(sk6to4);
    assertEquals(csum, s4csum);
//    println(sk6to4.toString(true, true, true, true));

//    println("\nConverted HLL_8 to HLL_4:");
    HllSketch sk8to4 = sk8.copyAs(HLL_4);
    csum = computeChecksum(sk8to4);
    assertEquals(csum, s4csum);
//    println(sk8to4.toString(true, true, true, true));

//    println("\nConverted HLL_4 to HLL_6:");
    HllSketch sk4to6 = sk4.copyAs(HLL_6);
    csum = computeChecksum(sk4to6);
    //println(sk4to6.toString(true, true, true, true));
    assertEquals(csum, s4csum);

//    println("\nConverted HLL_8 to HLL_6:");
    HllSketch sk8to6 = sk8.copyAs(HLL_6);
    csum = computeChecksum(sk8to6);
    assertEquals(csum, s4csum);
//    println(sk8to6.toString(true, true, true, true));

//    println("\nConverted HLL_4 to HLL_8:");
    HllSketch sk4to8 = sk4.copyAs(HLL_8);
    csum = computeChecksum(sk4to8);
    assertEquals(csum, s4csum);
//    println(sk4to8.toString(true, true, true, true));

//    println("\nConverted HLL_6 to HLL_8:");
    HllSketch sk6to8 = sk6.copyAs(HLL_8);
    csum = computeChecksum(sk6to8);
    assertEquals(csum, s4csum);
//    println(sk6to8.toString(true, true, true, true));
  }

  private static HllSketch buildSketch(int n, int lgK, TgtHllType tgtHllType) {
    HllSketch sketch = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      sketch.update(i);
    }
    return sketch;
  }

  private static int computeChecksum(HllSketch sketch) {
    PairIterator itr = sketch.iterator();
    int checksum = 0;
    int key  = 0;
    while (itr.nextAll()) {
      checksum += itr.getPair();
      key = itr.getKey(); //dummy
    }
    return checksum;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    //System.out.print(s); //disable here
  }
}
