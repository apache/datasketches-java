/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class UnionImplTest {

  @Test
  public void checkUpdateWithSketch() {
    int k = 16;
    Memory mem = new NativeMemory(new byte[k*8 + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().build(k);
    for (int i=0; i<k; i++) sketch.update(i);
    CompactSketch sketchInDirect = sketch.compact(true, mem);
    CompactSketch sketchInHeap = sketch.compact(true, null);
    
    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(sketchInDirect);
    union.update(sketchInHeap);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }
  
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkUpdateMemException() {
    int k = 16;
    Memory mem = new NativeMemory(new byte[k*8 + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().build(k);
    for (int i=0; i<k; i++) sketch.update(i);
    sketch.compact(true, mem);
    
    //corrupt mem
    mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)2);
    
    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(mem);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
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
