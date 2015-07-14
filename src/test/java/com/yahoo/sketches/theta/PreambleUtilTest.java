/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.PreambleUtil;
import com.yahoo.sketches.theta.UpdateSketch;

/** 
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void checkToString() {
    int k = 4096;
    int u = 2*k;
    int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);

    UpdateSketch quick1 = UpdateSketch.builder().initMemory(mem).build(k);
    println(PreambleUtil.toString(byteArray));
    
    Assert.assertTrue(quick1.isEmpty());

    for (int i = 0; i< u; i++) {
      quick1.update(i);
    }

    assertEquals(quick1.getEstimate(), u, 1000.0);
    assertTrue(quick1.getRetainedEntries(false) > k);
    println(quick1.toString());
    println(PreambleUtil.toString(mem));
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadSeedHashFromSeed() {
    //In the first 64K values 50541 produces a seedHash of 0, 
    for (int i=0; i < (1<<16); i++) { 
      PreambleUtil.computeSeedHash(i);
    }
  }
  
  @Test
  public void checkPreLongs() {
    UpdateSketch sketch = UpdateSketch.builder().build(16);
    CompactSketch comp = sketch.compact(false, null);
    byte[] byteArr = comp.toByteArray();
    println(PreambleUtil.toString(byteArr)); //PreLongs = 1
    
    sketch.update(1);
    comp = sketch.compact(false, null);
    byteArr = comp.toByteArray();
    println(PreambleUtil.toString(byteArr)); //PreLongs = 2
    
    for (int i=2; i<=32; i++) sketch.update(i);
    comp = sketch.compact(false, null);
    byteArr = comp.toByteArray();
    println(PreambleUtil.toString(byteArr)); //PreLongs = 3
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
   
}