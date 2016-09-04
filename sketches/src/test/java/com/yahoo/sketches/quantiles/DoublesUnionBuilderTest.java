/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

public class DoublesUnionBuilderTest {

  @Test
  public void checkBuilds() {
    DoublesSketch qs1 = DoublesSketch.builder().build();
    for (int i=0; i<1000; i++) qs1.update(i);
    
    int bytes = qs1.getStorageBytes();
    Memory dstMem = new NativeMemory(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;
    
    DoublesUnionBuilder bldr = new DoublesUnionBuilder();
    bldr.setK(128);
    DoublesUnion union = bldr.build(); //virgin union
    
    union = DoublesUnionBuilder.build(srcMem);
    DoublesSketch qs2 = union.getResult();
    assertEquals(qs1.getStorageBytes(), qs2.getStorageBytes());
    
    union = DoublesUnionBuilder.copyBuild(qs2);
    DoublesSketch qs3 = union.getResult();
    assertEquals(qs2.getStorageBytes(), qs3.getStorageBytes());
    assertFalse(qs2 == qs3);
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
