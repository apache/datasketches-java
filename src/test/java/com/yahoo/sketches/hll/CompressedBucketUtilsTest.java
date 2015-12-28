/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static org.testng.Assert.*;
import static com.yahoo.sketches.hll.CompressedBucketUtils.*;

import org.testng.annotations.Test;

public class CompressedBucketUtilsTest {
  
  @Test
  public void checkUpdateNibble() {
    byte[] buckets = new byte[2];
    updateNibble(buckets, 0, (byte) -1, null);
    assertEquals(buckets[0], (byte) 0);
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
