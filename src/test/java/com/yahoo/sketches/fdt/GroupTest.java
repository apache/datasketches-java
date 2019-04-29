/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class GroupTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  public void checkToString() { //check visually
    Group gp = new Group();
    //gp.init("AAAAAAAA,BBBBBBBBBB", 1_000_000, 1E10, 1.2E10, 8E9, 0.1, 0.01);
    gp.init("AAAAAAAA,BBBBBBBBBB", 100_000_000, 1E8, 1.2E8, 8E7, 0.1, 0.01);
    assertEquals(gp.getPrimaryKey(), "AAAAAAAA,BBBBBBBBBB");
    assertEquals(gp.getCount(), 100_000_000);
    assertEquals(gp.getEstimate(), 1E8);
    assertEquals(gp.getUpperBound(), 1.2E8);
    assertEquals(gp.getLowerBound(), 8E7);
    assertEquals(gp.getFraction(), 0.1);
    assertEquals(gp.getRse(), 0.01);

    println(gp.getHeader());
    println(gp.toString());
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
    //System.out.print(s);  //disable here
  }

}
