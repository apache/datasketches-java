/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static org.testng.Assert.*;

import com.yahoo.memory.NativeMemory;
import com.yahoo.memory.AllocMemory;

import org.testng.annotations.Test;

public class DirectDoublesSketchTest {

  @Test
  public void checkUnsafePutsGets() {
    NativeMemory mem = new AllocMemory(160);
    DirectDoublesSketch dds = DirectDoublesSketch.newInstance(16, mem);
    
    mem.clear();
    dds.putMinValue(1.0);
    assertEquals(dds.getMinValue(), 1.0);
    
    dds.putMaxValue(2.0);
    assertEquals(dds.getMaxValue(), 2.0);
    
    mem.freeMemory();
  }
  
}
