/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

//import com.yahoo.sketches.memory.Memory;
//import com.yahoo.sketches.memory.NativeMemory;


public class DoublesSketchBuilderTest {

  @Test
  public void checkBuilder() {
    int k = 256;
//    byte[] byteArr = new byte[k]; //dummy value
//    Memory mem = new NativeMemory(byteArr);
    
    DoublesSketchBuilder bldr = DoublesSketch.builder();
    
    bldr.setK(k);
    assertEquals(bldr.getK(), k);
    
//    bldr.initMemory(mem);
//    assertEquals(bldr.getMemory(), mem);
    
    println(bldr.toString());
    
    bldr = DoublesSketch.builder();
    assertEquals(bldr.getK(), DoublesSketch.DEFAULT_K);
//    assertNull(bldr.getMemory());
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
