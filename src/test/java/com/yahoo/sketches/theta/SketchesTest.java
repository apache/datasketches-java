/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import org.testng.annotations.Test;

import static com.yahoo.sketches.theta.ForwardCompatibilityTest.convertSerV3toSerV1;
import static com.yahoo.sketches.theta.Sketches.getMaxCompactSketchBytes;
import static com.yahoo.sketches.theta.Sketches.getMaxIntersectionBytes;
import static com.yahoo.sketches.theta.Sketches.getMaxUnionBytes;
import static com.yahoo.sketches.theta.Sketches.getMaxUpdateSketchBytes;
import static com.yahoo.sketches.theta.Sketches.getSerializationVersion;
import static com.yahoo.sketches.theta.Sketches.heapifySetOperation;
import static com.yahoo.sketches.theta.Sketches.heapifySketch;
import static com.yahoo.sketches.theta.Sketches.setOperationBuilder;
import static com.yahoo.sketches.theta.Sketches.updateSketchBuilder;
import static com.yahoo.sketches.theta.Sketches.wrapSetOperation;
import static com.yahoo.sketches.theta.Sketches.wrapSketch;
import static org.testng.Assert.*;

/**
 * @author Lee Rhodes
 */
public class SketchesTest {
  
  private static Memory getCompactSketch(int k, int from, int to) {
    UpdateSketch sk1 = updateSketchBuilder().build(k);
    for (int i=from; i<to; i++) sk1.update(i);
    CompactSketch csk = sk1.compact(true, null);
    byte[] sk1bytes = csk.toByteArray();
    NativeMemory mem = new NativeMemory(sk1bytes);
    return mem;
  }
  
  @Test
  public void checkSketchMethods() {
    int k = 1024;
    Memory mem = getCompactSketch(k, 0, k);
    
    CompactSketch csk2 = (CompactSketch)heapifySketch(mem);
    assertEquals((int)csk2.getEstimate(), k);
    
    csk2 = (CompactSketch)heapifySketch(mem, Util.DEFAULT_UPDATE_SEED);
    assertEquals((int)csk2.getEstimate(), k);
    
    csk2 = (CompactSketch)wrapSketch(mem);
    assertEquals((int)csk2.getEstimate(), k); //TODO fails
    
    csk2 = (CompactSketch)wrapSketch(mem, Util.DEFAULT_UPDATE_SEED);
    assertEquals((int)csk2.getEstimate(), k);
  }
  
  @Test
  public void checkSetOpMethods() {
    int k = 1024;
    Memory mem1 = getCompactSketch(k, 0, k);
    Memory mem2 = getCompactSketch(k, k/2, 3*k/2);
    
    SetOperationBuilder bldr = setOperationBuilder();
    Union union = bldr.buildUnion(2*k);
    
    union.update(mem1);
    CompactSketch cSk = union.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), k);
    union.update(mem2);
    cSk = union.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);
    
    byte[] ubytes = union.toByteArray();
    NativeMemory uMem = new NativeMemory(ubytes);
    
    Union union2 = (Union)heapifySetOperation(uMem);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);
    
    union2 = (Union)heapifySetOperation(uMem, Util.DEFAULT_UPDATE_SEED);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);
    
    union2 = (Union)wrapSetOperation(uMem);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);
    
    union2 = (Union)wrapSetOperation(uMem, Util.DEFAULT_UPDATE_SEED);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);
    
    int serVer = getSerializationVersion(uMem);
    assertEquals(serVer, 3);
  }
  
  @Test
  public void checkUtilMethods() {
    int k = 1024;
    
    int maxUnionBytes = getMaxUnionBytes(k);
    assertEquals(2*k*8+32, maxUnionBytes);
    
    int maxInterBytes = getMaxIntersectionBytes(k);
    assertEquals(2*k*8+24, maxInterBytes);
    
    int maxCompSkBytes = getMaxCompactSketchBytes(k+1);
    assertEquals(24+(k+1)*8, maxCompSkBytes);
    
    int maxSkBytes = getMaxUpdateSketchBytes(k);
    assertEquals(24+2*k*8, maxSkBytes);
  }
  
  @Test
  public void checkStaticEstimators() {
    int k = 4096;
    int u = 4*k;
    Memory srcMem = getCompactSketch(k, 0, u);
    double est = Sketches.getEstimate(srcMem);
    assertEquals(est, u, 0.05*u);
    double rse = 1.0/Math.sqrt(k);
    double ub = Sketches.getUpperBound(1, srcMem);
    assertEquals(ub, est+rse, 0.05*u);
    double lb = Sketches.getLowerBound(1, srcMem);
    assertEquals(lb, est-rse, 0.05*u);
    Memory memV1 = convertSerV3toSerV1(srcMem);
    boolean empty = Sketches.getEmpty(memV1);
    assertFalse(empty);
    Memory emptyMemV3 = getCompactSketch(k, 0, 0);
    assertEquals(Sketches.getRetainedEntries(emptyMemV3), 0);
    assertEquals(Sketches.getThetaLong(emptyMemV3), Long.MAX_VALUE);
    Memory emptyMemV1 = convertSerV3toSerV1(emptyMemV3);
    empty = Sketches.getEmpty(emptyMemV1);
    assertTrue(empty);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadSketchFamily() {
    Union union = setOperationBuilder().buildUnion();
    byte[] byteArr = union.toByteArray();
    Memory srcMem = new NativeMemory(byteArr);
    Sketches.getEstimate(srcMem);
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
