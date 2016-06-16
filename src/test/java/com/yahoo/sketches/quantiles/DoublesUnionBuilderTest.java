/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

public class DoublesUnionBuilderTest {

  @Test
  public void checkBuilds() {
    DoublesUnionBuilder bldr = new DoublesUnionBuilder();
    DoublesUnion union = bldr.build(128); //virgin union
    
    DoublesSketch qs1 = DoublesSketch.builder().build();
    for (int i=0; i<1000; i++) qs1.update(i);
    int bytes = qs1.getStorageBytes();
    Memory dstMem = new NativeMemory(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;
    
    union = bldr.build(srcMem);
    DoublesSketch qs2 = union.getResult();
    assertEquals(qs1.getStorageBytes(), qs2.getStorageBytes());
    
    union = bldr.copyBuild(qs2);
    DoublesSketch qs3 = union.getResult();
    assertEquals(qs2.getStorageBytes(), qs3.getStorageBytes());
    assertFalse(qs2 == qs3);
  }
  
}
