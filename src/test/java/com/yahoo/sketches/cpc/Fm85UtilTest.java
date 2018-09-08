/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.Fm85Util.countTrailingZerosByByte;
import static com.yahoo.sketches.cpc.Fm85Util.floorLog2ofX;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class Fm85UtilTest {

  @Test
  public void checkTrailingZeros() {
    for (int i = 0; i < 64; i++) {
      long in = 1L << i;
      assertEquals(countTrailingZerosByByte(in), Long.numberOfTrailingZeros(in));
    }
  }



  @Test
  public void checkFloorLg2OfLong() {
    assertEquals(floorLog2ofX((1L << 10) + 1), 10);
  }


  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }


}
