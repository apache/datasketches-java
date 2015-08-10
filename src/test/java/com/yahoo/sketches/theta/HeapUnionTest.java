/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.ForwardCompatibilityTest.*;
import static com.yahoo.sketches.theta.HeapUnionTest.testAllCompactForms;
import static com.yahoo.sketches.theta.SetOperation.getMaxUnionBytes;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * @author Lee Rhodes
 */
public class HeapUnionTest {

  @Test
  public void checkExactUnionNoOverlap() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //256
    for (int i=u/2; i<u; i++) usk2.update(i); //256 no overlap
    
    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch
    
    testAllCompactForms(union, u, 0.0);
  }
  
  @Test
  public void checkEstUnionNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2*k
    for (int i=u/2; i<u; i++) usk2.update(i); //2*k no overlap
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch
    
    testAllCompactForms(union, u, 0.05);
  }
  
  @Test
  public void checkExactUnionWithOverlap() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //256
    for (int i=0; i<u  ; i++) usk2.update(i); //512, 256 overlapped
    
    assertEquals(u, usk1.getEstimate() + usk2.getEstimate()/2, 0.0); //exact, overlapped
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch
    
    testAllCompactForms(union, u, 0.0);
  }

  @Test 
  public void checkHeapifyExact() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //256
    for (int i=u/2; i<u; i++) usk2.update(i); //256 no overlap
    
    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch
    
    testAllCompactForms(union, u, 0.0);
    
    Union union2 = (Union)SetOperation.heapify(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.0);
  }
  
  @Test
  public void checkHeapifyEstNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2k
    for (int i=u/2; i<u; i++) usk2.update(i); //2k no overlap, exact
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch, early stop not possible
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.heapify(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
  }
  
  @Test
  public void checkHeapifyEstNoOverlapOrderedIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact for early stop test
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2k
    for (int i=u/2; i<u; i++) usk2.update(i); //2k no overlap, exact, will force early stop
    
    CompactSketch cosk2 = usk2.compact(true, null);
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);  //update with heap UpdateSketch
    union.update(cosk2); //update with heap Compact, Ordered input, early stop
    
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch); //updates with empty
    emptySketch = null;
    union.update(emptySketch); //updates with null
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.heapify(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
    
    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }
  
  @Test
  public void checkWrapEstNoOverlapOrderedDirectIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact for early stop test
    
    for (int i=0; i<u/2; i++) usk1.update(i);  //2k estimating
    for (int i=u/2; i<u; i++) usk2.update(i);  //2k no overlap, exact, will force early stop
    
    NativeMemory cskMem2 = new NativeMemory(new byte[usk2.getCurrentBytes(true)]);
    CompactSketch cosk2 = usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cosk2);       //updates with direct CompactSketch, ordered, use early stop
    
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.heapify(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
    
    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }
  
  
  @Test
  public void checkHeapifyEstNoOverlapOrderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact for early stop test
    
    for (int i=0; i<u/2; i++) usk1.update(i);  //2k estimating
    for (int i=u/2; i<u; i++) usk2.update(i);  //2k no overlap, exact, will force early stop
    
    NativeMemory cskMem2 = new NativeMemory(new byte[usk2.getCurrentBytes(true)]);
    usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cskMem2);     //updates with direct CompactSketch, ordered, use early stop
    
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.heapify(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
    
    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }
  
  @Test
  public void checkMultiUnion() {
    int lgK = 13; //8192
    int k = 1 << lgK;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    UpdateSketch usk3 = UpdateSketch.builder().build(k);
    UpdateSketch usk4 = UpdateSketch.builder().build(k);
    
    int v=0;
    int u = 1000000;
    for (int i=0; i<u; i++) usk1.update(i+v);
    v += u;
    u = 26797;
    for (int i=0; i<u; i++) usk2.update(i+v);
    v += u;
    for (int i=0; i<u; i++) usk3.update(i+v);
    v += u;
    for (int i=0; i<u; i++) usk4.update(i+v);
    v += u;
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1); //updates with heap UpdateSketch
    union.update(usk2); //updates with heap UpdateSketch
    union.update(usk3); //updates with heap UpdateSketch
    union.update(usk4); //updates with heap UpdateSketch
    
    CompactSketch csk = union.getResult(true, null);
    double est = csk.getEstimate();
    assertEquals(est, v, .01*v);
  }
  
  @Test
  public void checkDirectMemoryIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u1 = 2*k;
    int u2 = 1024; //smaller exact sketch forces early stop 
    int totU = u1+u2;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u1; i++) usk1.update(i); //2*k
    for (int i=u1; i<totU; i++) usk2.update(i); //2*k + 1024 no overlap
    
    NativeMemory skMem1 = new NativeMemory(usk1.compact(false, null).toByteArray());
    NativeMemory skMem2 = new NativeMemory(usk2.compact(true, null).toByteArray());
    
    CompactSketch csk1 = (CompactSketch)Sketch.wrap(skMem1);
    CompactSketch csk2 = (CompactSketch)Sketch.wrap(skMem2);
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(csk1);
    union.update(csk2);
    
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
  }
  
  @Test
  public void checkSerVer1Handling() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u1 = 2*k;
    int u2 = 1024; //smaller exact sketch forces early stop 
    int totU = u1+u2;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u1; i++) usk1.update(i); //2*k
    for (int i=u1; i<totU; i++) usk2.update(i); //2*k + 1024 no overlap
    
    NativeMemory skMem1 = new NativeMemory(usk1.compact(true, null).toByteArray());
    NativeMemory skMem2 = new NativeMemory(usk2.compact(true, null).toByteArray());
    
    Memory v1mem1 = convertSerV3toSerV1(skMem1);
    Memory v1mem2 = convertSerV3toSerV1(skMem2);
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(v1mem1);
    union.update(v1mem2);
    
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
  }
  
  @Test
  public void checkSerVer2Handling() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u1 = 2*k;
    int u2 = 1024; //smaller exact sketch forces early stop 
    int totU = u1+u2;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u1; i++) usk1.update(i); //2*k
    for (int i=u1; i<totU; i++) usk2.update(i); //2*k + 1024 no overlap
    
    NativeMemory skMem1 = new NativeMemory(usk1.compact(true, null).toByteArray());
    NativeMemory skMem2 = new NativeMemory(usk2.compact(true, null).toByteArray());
    
    Memory v2mem1 = convertSerV3toSerV2(skMem1);
    Memory v2mem2 = convertSerV3toSerV2(skMem2);
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(v2mem1);
    union.update(v2mem2);
    
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
  }
  
  @Test
  public void checkUpdateMemorySpecialCases() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    CompactSketch usk1c = usk1.compact(true, null);
    NativeMemory v3mem1 = new NativeMemory(usk1c.toByteArray());
    
    Memory v1mem1 = convertSerV3toSerV1(v3mem1);
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    union.update(v1mem1);
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
    
    Memory v2mem1 = convertSerV3toSerV2(v3mem1);
    
    union = (Union)SetOperation.builder().build(k, Family.UNION);
    union.update(v2mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
    
    union = (Union)SetOperation.builder().build(k, Family.UNION);
    union.update(v3mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
    
    union = (Union)SetOperation.builder().build(k, Family.UNION);
    v3mem1 = null;
    union.update(v3mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
  }
  
  //used by DirectUnionTest as well
  public static void testAllCompactForms(Union union, double expected, double toll) {
    double compEst1, compEst2;
    compEst1 = union.getResult(false, null).getEstimate(); //not ordered, no mem
    assertEquals(compEst1, expected, toll*expected);
    
    CompactSketch comp2 = union.getResult(true, null); //ordered, no mem
    compEst2 = comp2.getEstimate();
    assertEquals(compEst2, compEst1, 0.0);
    
    Memory mem = new NativeMemory(new byte[comp2.getCurrentBytes(false)]);
    
    compEst2 = union.getResult(false, mem).getEstimate(); //not ordered, mem
    assertEquals(compEst2, compEst1, 0.0);
    
    compEst2 = union.getResult(true, mem).getEstimate(); //ordered, mem
    assertEquals(compEst2, compEst1, 0.0);
  }
  
  @Test
  public void printlnTest() {
    println("Test");
  }
  
  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.println(s); //Disable here
  }
}