/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

public class ReadOnlyMemoryTest {

  @Test
  public void checkHeapQuickSelect() {
    int k = 16;
    int u = k;

    //Heap Writable Memory
    UpdateSketch srcSk = UpdateSketch.builder().build(k);
    for (int i = 0; i < u; i++) { srcSk.update(i); }
    byte[] arr = srcSk.toByteArray();

    Memory mem = new NativeMemory(arr);
    Sketch tgtSk = Sketches.heapifySketch(mem);
    assertEquals(tgtSk.getEstimate(), (double)u);

    //Heap Read-Only Memory
    Memory memRO = mem.asReadOnlyMemory();
    tgtSk = Sketches.heapifySketch(memRO);
    assertEquals(tgtSk.getEstimate(), (double)u);

    //Direct Writable Memory
    int bytes = Sketch.getMaxUpdateSketchBytes(k);
    Memory memD = new AllocMemory(bytes);
    UpdateSketch srcSkD = UpdateSketch.builder().initMemory(memD).build(k);
    for (int i = 0; i < u; i++) { srcSkD.update(i); }

    tgtSk = Sketches.heapifySketch(memD);
    assertEquals(tgtSk.getEstimate(), (double)u);

    //Direct Read-Only Memory
    Memory memDRO = mem.asReadOnlyMemory();
    tgtSk = Sketches.heapifySketch(memDRO);
    assertEquals(tgtSk.getEstimate(), (double)u);
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
