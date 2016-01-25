/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

public class QuantilesSketchBuilderTest {

  @Test
  public void checkBuilder() {
    int k = 100;
    byte[] byteArr = new byte[k];
    Memory mem = new NativeMemory(byteArr);
    QuantilesSketchBuilder bldr = QuantilesSketch.builder();
    bldr.setK(k).initMemory(mem);
    assertEquals(bldr.getK(), k);
    assertEquals(bldr.getMemory(), mem);
    println(bldr.toString());
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
