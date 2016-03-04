/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * @author Lee Rhodes
 */
public class CompactSketchTest {
  
  @Test
  public void checkHeapifyWrap() {
    int k = 16;
    checkHeapifyWrap(k, k); //exact
  }
  
  //test combinations of compact ordered/not ordered and heap/direct
  public void checkHeapifyWrap(int k, int u) {
    UpdateSketch usk = UpdateSketch.builder().build(k);
    for (int i=0; i<u; i++) usk.update(i);
    double uskEst = usk.getEstimate();
    assertEquals(uskEst, k, 0.0);
    int uskCount = usk.getRetainedEntries(true);
    short uskSeedHash = usk.getSeedHash();
    long uskThetaLong = usk.getThetaLong();
    
    CompactSketch csk, csk2, csk3;
    byte[] byteArray;
    boolean ordered = true;
    boolean compact = true;
    
    /****/
    csk = usk.compact( !ordered, null); //NOT ORDERED
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactSketch");
    assertFalse(csk.isDirect());
    assertTrue(csk.isCompact());
    assertFalse(csk.isOrdered());
    //println("Ord: "+(!ordered)+", Mem: "+"Null");
    //println(csk.toString(true, true, 8, true));
    //println(PreambleUtil.toString(byteArray));
    
    //put image in memory, check heapify
    Memory srcMem = new NativeMemory(csk.toByteArray());
    csk2 = (CompactSketch) Sketch.heapify(srcMem);
    //println(csk2.toString(true, true, 8, true));
    double csk2est = csk2.getEstimate();
    assertEquals(csk2est, uskEst, 0.0);
    assertEquals(csk2.getRetainedEntries(true), uskCount);
    assertEquals(csk2.getSeedHash(), uskSeedHash);
    assertEquals(csk2.getThetaLong(), uskThetaLong);
    assertNull(csk2.getMemory());
    assertFalse(csk2.isOrdered()); //CHECK NOT ORDERED
    assertNotNull(csk2.getCache());
    
    /****/
    csk = usk.compact(  ordered, null); //ORDERED
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactOrderedSketch");
    assertFalse(csk.isDirect());
    assertTrue(csk.isCompact());
    assertTrue(csk.isOrdered()); //CHECK ORDERED
    
    Memory srcMem2 = new NativeMemory(csk.toByteArray());
    csk3 = (CompactSketch)Sketch.heapify(srcMem2);
    double csk3est = csk3.getEstimate();
    assertEquals(csk3est, uskEst, 0.0);

    assertEquals(csk3.getRetainedEntries(true), uskCount);
    assertEquals(csk3.getSeedHash(), uskSeedHash);
    assertEquals(csk3.getThetaLong(), uskThetaLong);
    assertNull(csk3.getMemory());
    assertTrue(csk3.isOrdered());
    assertNotNull(csk3.getCache());
    
    /****/
    //Prepare Memory for direct
    int bytes = usk.getCurrentBytes(compact);
    byte[] memArr = new byte[bytes];
    Memory mem = new NativeMemory(memArr);
    Memory mem2;
    
    /**Via CompactSketch.compact**/
    csk = usk.compact( !ordered, mem); //NOT ORDERED, DIRECT
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");
    
    csk2 = (CompactSketch)Sketch.wrap(mem);
    assertEquals(csk2.getEstimate(), uskEst, 0.0);

    assertEquals(csk2.getRetainedEntries(true), uskCount);
    assertEquals(csk2.getSeedHash(), uskSeedHash);
    assertEquals(csk2.getThetaLong(), uskThetaLong);
    assertNotNull(csk2.getMemory());
    assertFalse(csk2.isOrdered());
    assertNotNull(csk2.getCache());
    
    /**Via byte[]**/
    csk = usk.compact( !ordered, mem);
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");
    assertTrue(csk.isDirect());
    assertTrue(csk.isCompact());
    assertFalse(csk.isOrdered());
    
    byteArray = csk.toByteArray();
    mem2 = new NativeMemory(byteArray);
    csk2 = (CompactSketch)Sketch.wrap(mem2);
    assertEquals(csk2.getEstimate(), uskEst, 0.0);

    assertEquals(csk2.getRetainedEntries(true), uskCount);
    assertEquals(csk2.getSeedHash(), uskSeedHash);
    assertEquals(csk2.getThetaLong(), uskThetaLong);
    assertNotNull(csk2.getMemory());
    assertFalse(csk2.isOrdered());
    assertNotNull(csk2.getCache());
    
    /**Via CompactSketch.compact**/
    csk = usk.compact(  ordered, mem);
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactOrderedSketch");
    assertTrue(csk.isDirect());
    assertTrue(csk.isCompact());
    assertTrue(csk.isOrdered());
    
    csk2 = (CompactSketch)Sketch.wrap(mem);
    assertEquals(csk2.getEstimate(), uskEst, 0.0);

    assertEquals(csk2.getRetainedEntries(true), uskCount);
    assertEquals(csk2.getSeedHash(), uskSeedHash);
    assertEquals(csk2.getThetaLong(), uskThetaLong);
    assertNotNull(csk2.getMemory());
    assertTrue(csk2.isOrdered());
    assertNotNull(csk2.getCache());
    
    /**Via byte[]**/
    csk = usk.compact(  ordered, mem);
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactOrderedSketch");
    
    byteArray = csk.toByteArray();
    mem2 = new NativeMemory(byteArray);
    csk2 = (CompactSketch)Sketch.wrap(mem2);
    assertEquals(csk2.getEstimate(), uskEst, 0.0);

    assertEquals(csk2.getRetainedEntries(true), uskCount);
    assertEquals(csk2.getSeedHash(), uskSeedHash);
    assertEquals(csk2.getThetaLong(), uskThetaLong);
    assertNotNull(csk2.getMemory());
    assertTrue(csk2.isOrdered());
    assertNotNull(csk2.getCache());
  }
 
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkMemTooSmall() {
    int k = 512;
    int u = k;
    boolean compact = true;
    boolean ordered = false;
    UpdateSketch usk = UpdateSketch.builder().build(k);
    for (int i=0; i<u; i++) usk.update(i);
    
    int bytes = usk.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes -8]; //too small
    Memory mem = new NativeMemory(byteArray);
    
    @SuppressWarnings("unused")
    CompactSketch csk = usk.compact(ordered, mem);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkMemTooSmallOrdered() {
    int k = 512;
    int u = k;
    boolean compact = true;
    boolean ordered = true;
    UpdateSketch usk = UpdateSketch.builder().build(k);
    for (int i=0; i<u; i++) usk.update(i);
    
    int bytes = usk.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes -8]; //too small
    Memory mem = new NativeMemory(byteArray);
    
    @SuppressWarnings("unused")
    CompactSketch csk = usk.compact(ordered, mem);
  }
  
  @Test
  public void checkCompactCachePart() {
    //phony values except for curCount = 0.
    long[] result = CompactSketch.compactCachePart(null, 4, 0, 0L, false);
    assertEquals(result.length, 0);
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
