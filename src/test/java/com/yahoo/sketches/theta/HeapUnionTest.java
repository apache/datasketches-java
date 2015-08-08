/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.ForwardCompatibilityTest.*;
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

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i); //256
    for (int i=k/2; i<k; i++) usk2.update(i); //256 no overlap
    
    double usk1est = usk1.getEstimate();
    double usk2est = usk2.getEstimate();
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = k;
    
    CompactSketch comp1, comp2, comp3, comp4;
    double compEst;
    
    //test all the compacts
    comp1 = union.getResult(false, null); //ordered: false
    compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    assertEquals(compEst, usk1est + usk2est, 0.0);

    comp2 = union.getResult(true, null); //ordered: true
    compEst = comp2.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    
    int bytes = comp2.getCurrentBytes(false);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    
    comp3 = union.getResult(false, mem);
    compEst = comp3.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    
    comp4 = union.getResult(true, mem);
    compEst = comp4.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
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
    
//    double usk1est = usk1.getEstimate();
//    double usk2est = usk2.getEstimate();
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = u;
    
    CompactSketch comp1, comp2, comp3, comp4;
    double compEst;
    
  //test all the compacts
    comp1 = union.getResult(false, null); //ordered: false
    compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);

    comp2 = union.getResult(true, null); //ordered: true
    compEst = comp2.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    int bytes = comp2.getCurrentBytes(false);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    
    comp3 = union.getResult(false, mem);
    compEst = comp3.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    comp4 = union.getResult(true, mem);
    compEst = comp4.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
  }
  
  @Test
  public void checkExactUnionWithOverlap() {
    int lgK = 9; //512
    int k = 1 << lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i); //256
    for (int i=0; i<k  ; i++) usk2.update(i); //512, 256 overlapped
    
    double usk1est = usk1.getEstimate();
    double usk2est = usk2.getEstimate();
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = k;
    
    CompactSketch comp1, comp2, comp3, comp4;
    double compEst;
    
    //test all the compacts
    comp1 = union.getResult(false, null); //ordered: false
    compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    assertEquals(compEst, usk1est + usk2est/2, 0.0);
  
    comp2 = union.getResult(true, null);
    compEst = comp2.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    
    int bytes = comp2.getCurrentBytes(false);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    
    comp3 = union.getResult(false, mem);
    compEst = comp3.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    
    comp4 = union.getResult(true, mem);
    compEst = comp4.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
  }

  @Test 
  public void checkHeapifyExact() {
    int lgK = 9; //512
    int k = 1 << lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i); //256
    for (int i=k/2; i<k; i++) usk2.update(i); //256 no overlap
    
    double usk1est = usk1.getEstimate();
    double usk2est = usk2.getEstimate();
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = k;
    byte[] byteArr1 = union.toByteArray();
    Memory srcMem = new NativeMemory(byteArr1);
    Union union2 = (Union)SetOperation.heapify(srcMem);
    
    CompactSketch comp1, comp2, comp3, comp4;
    double compEst;
    
    //test all the compacts
    comp1 = union2.getResult(false, null); //ordered: false
    compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    assertEquals(compEst, usk1est + usk2est, 0.0);

    comp2 = union2.getResult(true, null); //ordered: true
    compEst = comp2.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    
    int bytes = comp2.getCurrentBytes(false);
    byte[] byteArr2 = new byte[bytes];
    Memory mem = new NativeMemory(byteArr2);
    
    comp3 = union2.getResult(false, mem);
    compEst = comp3.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
    
    comp4 = union2.getResult(true, mem);
    compEst = comp4.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
  }
  
  @Test
  public void checkHeapifyEstNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2*k
    for (int i=u/2; i<u; i++) usk2.update(i); //2*k no overlap
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = u;
    
    byte[] byteArr1 = union.toByteArray();
    Memory srcMem = new NativeMemory(byteArr1);
    Union union2 = (Union)SetOperation.heapify(srcMem);
    
    CompactSketch comp1, comp2, comp3, comp4;
    double compEst;
    
  //test all the compacts
    comp1 = union2.getResult(false, null); //ordered: false
    compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    comp2 = union2.getResult(true, null); //ordered: true
    compEst = comp2.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    int bytes = comp2.getCurrentBytes(false);
    byte[] byteArr2 = new byte[bytes];
    Memory mem = new NativeMemory(byteArr2);
    
    comp3 = union2.getResult(false, mem);
    compEst = comp3.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    comp4 = union2.getResult(true, mem);
    compEst = comp4.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
  }
  
  @Test
  public void checkHeapifyEstNoOverlapOrderedIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2*k
    for (int i=u/2; i<u; i++) usk2.update(i); //2*k no overlap
    
    CompactSketch compOrdered = usk2.compact(true, null);
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(usk1);
    union.update(compOrdered);
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch);
    emptySketch = null;
    union.update(emptySketch);
    
    double exactUnionAnswer = u;
    
    byte[] byteArr1 = union.toByteArray();
    Memory uMem = new NativeMemory(byteArr1);
    Union union2 = (Union)SetOperation.heapify(uMem);
    
    CompactSketch comp1, comp2, comp3, comp4;
    double compEst;
    
    //test all the compacts
    comp1 = union2.getResult(false, null); //ordered: false
    compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    comp2 = union2.getResult(true, null); //ordered: true
    compEst = comp2.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    int bytes = comp2.getCurrentBytes(false);
    byte[] byteArr2 = new byte[bytes];
    Memory mem = new NativeMemory(byteArr2);
    
    comp3 = union2.getResult(false, mem);
    compEst = comp3.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    comp4 = union2.getResult(true, mem);
    compEst = comp4.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    
    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }
  
  @Test
  public void checkHeapifyEstNoOverlapOrderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2*k
    for (int i=u/2; i<u; i++) usk2.update(i); //2*k no overlap
    
    CompactSketch usk1c = usk1.compact(true, null);
    CompactSketch usk2c = usk2.compact(true, null);
    NativeMemory skMem1 = new NativeMemory(usk1c.toByteArray());
    NativeMemory skMem2 = new NativeMemory(usk2c.toByteArray());
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(skMem1);
    union.update(skMem2);
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch);
    emptySketch = null;
    union.update(emptySketch);
    
    double exactUnionAnswer = u;
    
    byte[] byteArr1 = union.toByteArray();
    Memory uMem = new NativeMemory(byteArr1);
    Union union2 = (Union)SetOperation.heapify(uMem);
    
    CompactSketch comp1, comp2, comp3, comp4;
    double compEst;
    
    //test all the compacts
    comp1 = union2.getResult(false, null); //ordered: false
    compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    println(""+(compEst/exactUnionAnswer -1));
    
    comp2 = union2.getResult(true, null); //ordered: true
    compEst = comp2.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    println(""+(compEst/exactUnionAnswer -1));
    
    int bytes = comp2.getCurrentBytes(false);
    byte[] byteArr2 = new byte[bytes];
    Memory mem = new NativeMemory(byteArr2);
    
    comp3 = union2.getResult(false, mem);
    compEst = comp3.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    println(""+(compEst/exactUnionAnswer -1));
    
    comp4 = union2.getResult(true, mem);
    compEst = comp4.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.05*u);
    println(""+(compEst/exactUnionAnswer -1));
    
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
    println(""+v);
    u = 26797;
    for (int i=0; i<u; i++) usk2.update(i+v);
    v += u;
    println(""+v);
    for (int i=0; i<u; i++) usk3.update(i+v);
    v += u;
    println(""+v);
    for (int i=0; i<u; i++) usk4.update(i+v);
    v += u;
    println(""+v);
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    union.update(usk1);
    union.update(usk2);
    union.update(usk3);
    union.update(usk4);
    CompactSketch csk = union.getResult(true, null);
    double est = csk.getEstimate();
    assertEquals(est, v, .01*v);
    println("CskEst: "+est);
    
  }
  
  @Test
  public void checkDirectMemory() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u1 = 2*k;
    int u2 = 1024; //smaller exact sketch forces early stop 
    int totU = u1+u2;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u1; i++) usk1.update(i); //2*k
    for (int i=u1; i<totU; i++) usk2.update(i); //2*k + 1024 no overlap
    
    CompactSketch usk1c = usk1.compact(false, null);
    CompactSketch usk2c = usk2.compact(true, null);
    NativeMemory skMem1 = new NativeMemory(usk1c.toByteArray());
    NativeMemory skMem2 = new NativeMemory(usk2c.toByteArray());
    
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
    
    CompactSketch usk1c = usk1.compact(true, null);
    CompactSketch usk2c = usk2.compact(true, null);
    NativeMemory skMem1 = new NativeMemory(usk1c.toByteArray());
    NativeMemory skMem2 = new NativeMemory(usk2c.toByteArray());
    
    Memory v1mem1 = convertSerV3toSerV1(skMem1);
    Memory v1mem2 = convertSerV3toSerV1(skMem2);
    
    Union union = (Union)SetOperation.builder().build(k, Family.UNION);
    
    union.update(v1mem1);
    union.update(v1mem2);
    
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
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