/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.UpdateSketch;

/** 
 * @author Lee Rhodes
 */
public class UpdateSketchTest {

  @Test
  public void checkOtherUpdates() {
    int k = 512;
    UpdateSketch sk1 = UpdateSketch.builder().build(k);
    sk1.update(1.5); //#1 double
    sk1.update(0.0);
    sk1.update(-0.0);
    String s = null;
    sk1.update(s); //null string
    s = "";
    sk1.update(s); //empty string
    s = "String";
    sk1.update(s); //#2 actual string
    byte[] byteArr = null;
    sk1.update(byteArr); //null byte[]
    byteArr = new byte[0];
    sk1.update(byteArr); //empty byte[]
    byteArr = "Byte Array".getBytes();
    sk1.update(byteArr); //#3 actual byte[]
    int[] intArr = null;
    sk1.update(intArr); //null int[]
    intArr = new int[0];
    sk1.update(intArr); //empty int[]
    int[] intArr2 = { 1, 2, 3, 4, 5 };
    sk1.update(intArr2); //#4 actual int[]
    long[] longArr = null;
    sk1.update(longArr); //null long[]
    longArr = new long[0];
    sk1.update(longArr); //empty long[]
    long[] longArr2 = { 6, 7, 8, 9 };
    sk1.update(longArr2); //#5 actual long[]

    double est = sk1.getEstimate();
    assertEquals(est, 6.0, 0.0);
  }
  
  @Test
  public void checkStartingSubMultiple() {
    int lgSubMul;
    lgSubMul = UpdateSketch.startingSubMultiple(10, ResizeFactor.X1, 5);
    assertEquals(lgSubMul, 10);
    lgSubMul = UpdateSketch.startingSubMultiple(10, ResizeFactor.X2, 5);
    assertEquals(lgSubMul, 5);
    lgSubMul = UpdateSketch.startingSubMultiple(10, ResizeFactor.X4, 5);
    assertEquals(lgSubMul, 6);
    lgSubMul = UpdateSketch.startingSubMultiple(10, ResizeFactor.X8, 5);
    assertEquals(lgSubMul, 7);
    lgSubMul = UpdateSketch.startingSubMultiple(4, ResizeFactor.X1, 5);
    assertEquals(lgSubMul, 5);
  }
  
  @Test
  public void checkBuilder() {
    UpdateSketchBuilder bldr = UpdateSketch.builder();
    
    long seed = 12345L;
    bldr.setSeed(seed);
    assertEquals(seed, bldr.getSeed());
    
    float p = (float)0.5;
    bldr.setP(p);
    assertEquals(p, bldr.getP());
    
    ResizeFactor rf = ResizeFactor.X4;
    bldr.setResizeFactor(rf);
    assertEquals(rf, bldr.getResizeFactor());
    
    Family fam = Family.ALPHA;
    bldr.setFamily(fam);
    assertEquals(fam, bldr.getFamily());
    
    Memory mem = new NativeMemory(new byte[16]);
    bldr.initMemory(mem);
    assertEquals(mem, bldr.getMemory());
    
    int lgK = 10;
    int k = 1 << lgK;
    bldr.setNominalEntries(k);
    assertEquals(lgK, bldr.getLgNominalEntries());
    
    println(bldr.toString());
  }
  
  @Test
  public void checkCompact() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    CompactSketch csk = sk.compact();
    assertEquals(csk.getCurrentBytes(true), 8);
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
