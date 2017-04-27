/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

public class DoublesSketchBuilderTest {

  @Test
  public void checkBuilder() {
    int k = 256; //default is 128
    DoublesSketchBuilder bldr = DoublesSketch.builder();
    bldr.setK(k);
    assertEquals(bldr.getK(), k); //confirms new k
    println(bldr.toString());
    int bytes = DoublesSketch.getUpdatableStorageBytes(k, 0);
    byte[] byteArr = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(byteArr);
    bldr.initMemory(mem);
    assertTrue(bldr.getMemory() == mem);
    DoublesSketch ds = bldr.build(k);
    assertTrue(ds.isDirect());
    println(bldr.toString());

    bldr = DoublesSketch.builder();
    assertEquals(bldr.getK(), PreambleUtil.DEFAULT_K);
    assertNull(bldr.getMemory());
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
