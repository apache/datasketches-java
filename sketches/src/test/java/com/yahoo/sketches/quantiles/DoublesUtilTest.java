/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapUpdateDoublesSketchTest.buildAndLoadQS;
import static com.yahoo.sketches.quantiles.Util.LS;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

public class DoublesUtilTest {

  @Test
  public void checkPrintMemData() {
    final int k = 16;
    final int n = 1000;
    final DoublesSketch qs = buildAndLoadQS(k,n);

    byte[] byteArr = qs.toByteArray(false);
    Memory mem = new NativeMemory(byteArr);
    println(DoublesUtil.memToString(true, true, mem));

    byteArr = qs.toByteArray(true);
    mem = new NativeMemory(byteArr);
    println(DoublesUtil.memToString(true, true, mem));
  }

  @Test
  public void checkPrintMemData2() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 0;
    final DoublesSketch qs = buildAndLoadQS(k,n);

    final byte[] byteArr = qs.toByteArray();
    final Memory mem = new NativeMemory(byteArr);
    println(DoublesUtil.memToString(true, true, mem));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(final String s) {
    //System.out.print(s); //disable here
  }

}
