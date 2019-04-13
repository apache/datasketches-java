/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class RowTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  public void checkToString() { //check visually
    Row<String> ros = new Row<>("A", 1_000_000, 10_000_000_000L, 1.2E10, 8E9);
    println(Row.getRowHeader());
    println(ros.toString());
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
