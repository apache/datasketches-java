/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import org.testng.annotations.Test;

/** 
 * @author Lee Rhodes
 */
public class ExamplesTest {
  
  @Test
  public void simpleCountingSketch() {
    int k = 4096;
    int u = 1000000;
    
    UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < u; i++) {
      sketch.update(i);
    }
    
    println(sketch.toString());
  }
  /*
### HeapQuickSelectSketch SUMMARY: 
   Nominal Entries (k)     : 4096
   Estimate                : 1002714.745231455
   Upper Bound, 95% conf   : 1027777.3354974985
   Lower Bound, 95% conf   : 978261.4472857157
   p                       : 1.0
   Theta (double)          : 0.00654223948655085
   Theta (long)            : 60341508738660257
   Theta (long, hex        : 00d66048519437a1
   EstMode?                : true
   Empty?                  : false
   Resize Factor           : 8
   Array Size Entries      : 8192
   Retained Entries        : 6560
   Update Seed             : 9001
   Seed Hash               : ffff93cc
### END SKETCH SUMMARY
  */
  
  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //enable/disable here
  }
  
}
