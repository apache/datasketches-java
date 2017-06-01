/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapUpdateDoublesSketchTest.buildAndLoadQS;
import static com.yahoo.sketches.quantiles.Util.LS;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

public class DoublesUtilTest {

  @Test
  public void checkPrintMemData() {
    final int k = 16;
    final int n = 1000;
    final DoublesSketch qs = buildAndLoadQS(k,n);

    byte[] byteArr = qs.toByteArray(false);
    Memory mem = Memory.wrap(byteArr);
    println(DoublesUtil.memToString(true, true, mem));

    byteArr = qs.toByteArray(true);
    mem = Memory.wrap(byteArr);
    println(DoublesUtil.memToString(true, true, mem));
  }

  @Test
  public void checkPrintMemData2() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 0;
    final DoublesSketch qs = buildAndLoadQS(k,n);

    final byte[] byteArr = qs.toByteArray();
    final Memory mem = Memory.wrap(byteArr);
    println(DoublesUtil.memToString(true, true, mem));
  }

  @Test
  public void checkCopyToHeap() {
    final int k = 128;
    final int n = 400;

    // HeapUpdateDoublesSketch
    final HeapUpdateDoublesSketch huds = (HeapUpdateDoublesSketch) buildAndLoadQS(k, n);
    final HeapUpdateDoublesSketch target1 = DoublesUtil.copyToHeap(huds);
    DoublesSketchTest.testSketchEquality(huds, target1);

    // DirectUpdateDoublesSketch
    final WritableMemory mem1 = WritableMemory.wrap(huds.toByteArray());
    final DirectUpdateDoublesSketch duds = (DirectUpdateDoublesSketch) UpdateDoublesSketch.wrap(mem1);
    final HeapUpdateDoublesSketch target2 = DoublesUtil.copyToHeap(duds);
    DoublesSketchTest.testSketchEquality(huds, duds);
    DoublesSketchTest.testSketchEquality(duds, target2);

    // HeapCompactDoublesSketch
    final CompactDoublesSketch hcds = huds.compact();
    final HeapUpdateDoublesSketch target3  = DoublesUtil.copyToHeap(hcds);
    DoublesSketchTest.testSketchEquality(huds, hcds);
    DoublesSketchTest.testSketchEquality(hcds, target3);

    // DirectCompactDoublesSketch
    final Memory mem2 = Memory.wrap(hcds.toByteArray());
    final DirectCompactDoublesSketch dcds = (DirectCompactDoublesSketch) DoublesSketch.wrap(mem2);
    final HeapUpdateDoublesSketch target4 = DoublesUtil.copyToHeap(dcds);
    DoublesSketchTest.testSketchEquality(huds, dcds);
    DoublesSketchTest.testSketchEquality(dcds, target4);
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
