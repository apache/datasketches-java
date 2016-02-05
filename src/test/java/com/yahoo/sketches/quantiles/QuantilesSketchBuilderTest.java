/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;


public class QuantilesSketchBuilderTest {

  @Test
  public void checkBuilder() {
    int k = 256;
    short seed = 32749;
    byte[] byteArr = new byte[k]; //dummy value
    Memory mem = new NativeMemory(byteArr);
    
    QuantilesSketchBuilder bldr = QuantilesSketch.builder();
    
    bldr.setK(k);
    assertEquals(bldr.getK(), k);
    
    bldr.initMemory(mem);
    assertEquals(bldr.getMemory(), mem);
    
    bldr.setSeed(seed);
    assertEquals(bldr.getSeed(), seed);
    
    println(bldr.toString());
    
    bldr = QuantilesSketch.builder();
    assertEquals(bldr.getK(), QuantilesSketch.DEFAULT_K);
    assertEquals(bldr.getSeed(), QuantilesSketch.DEFAULT_SEED);
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
