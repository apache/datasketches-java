/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HarmonicNumbers.harmonicNumber;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class HarmonicNumbersTest {
  
  @Test
  public void checkExactHarmonicNumbers() {
    assertEquals(harmonicNumber(1L), 1.0, 0.0);
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
