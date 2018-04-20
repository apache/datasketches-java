/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

public class DoublesUnionBuilderTest {

  @Test
  public void checkBuilds() {
    UpdateDoublesSketch qs1 = DoublesSketch.builder().build();
    for (int i=0; i<1000; i++) { qs1.update(i); }

    int bytes = qs1.getCompactStorageBytes();
    WritableMemory dstMem = WritableMemory.wrap(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;

    DoublesUnionBuilder bldr = new DoublesUnionBuilder();
    bldr.setMaxK(128);
    DoublesUnion union = bldr.build(); //virgin union

    union = DoublesUnion.heapify(srcMem);
    DoublesSketch qs2 = union.getResult();
    assertEquals(qs1.getCompactStorageBytes(), qs2.getCompactStorageBytes());

    union = DoublesUnion.heapify(qs2);
    DoublesSketch qs3 = union.getResult();
    assertEquals(qs2.getCompactStorageBytes(), qs3.getCompactStorageBytes());
    assertFalse(qs2 == qs3);
  }


@Test
public void checkDeprecated1() {
  UpdateDoublesSketch qs1 = DoublesSketch.builder().build();
  for (int i=0; i<1000; i++) {
    qs1.update(i);
  }

  int bytes = qs1.getCompactStorageBytes();
  WritableMemory dstMem = WritableMemory.wrap(new byte[bytes]);
  qs1.putMemory(dstMem);
  Memory srcMem = dstMem;

  DoublesUnionBuilder bldr = new DoublesUnionBuilder();
  bldr.setMaxK(128);
  DoublesUnion union = bldr.build(); //virgin union

  union = DoublesUnion.heapify(srcMem); //heapify
  DoublesSketch qs2 = union.getResult();
  assertEquals(qs1.getCompactStorageBytes(), qs2.getCompactStorageBytes());
  assertEquals(qs1.getUpdatableStorageBytes(), qs2.getUpdatableStorageBytes());

  union = DoublesUnion.heapify(qs2);  //heapify again
  DoublesSketch qs3 = union.getResult();
  assertEquals(qs2.getCompactStorageBytes(), qs3.getCompactStorageBytes());
  assertEquals(qs2.getUpdatableStorageBytes(), qs3.getUpdatableStorageBytes());
  assertFalse(qs2 == qs3); //different objects
}

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }

}
