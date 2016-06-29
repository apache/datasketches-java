/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class HllSketchBuilderTest {
  
  @Test
  public void checkParams() {
    int lgK = 12;
    HllSketchBuilder bldr = HllSketch.builder();
    bldr.setLogBuckets(lgK);
    
    assertEquals(lgK, bldr.getLogBuckets());
    assertFalse(bldr.isDenseMode());
    assertFalse(bldr.isCompressedDense());
    assertFalse(bldr.isHipEstimator());
    assertTrue(bldr.getPreamble() != null);
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
