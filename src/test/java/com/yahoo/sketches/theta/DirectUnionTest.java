/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.PreambleUtil;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * @author Lee Rhodes
 */
public class DirectUnionTest {

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
    
    int memBytes = (k << 4) + (Family.UNION.getMinPreLongs() << 3);
    byte[] memByteArr = new byte[memBytes];
    Memory uMem = new NativeMemory(memByteArr);
    
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    int memBytes = (k << 4) + (Family.UNION.getMinPreLongs() << 3);
    byte[] memByteArr = new byte[memBytes];
    Memory uMem = new NativeMemory(memByteArr);
    
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    int memBytes = (k << 4) + (Family.UNION.getMinPreLongs() << 3);
    byte[] memByteArr = new byte[memBytes];
    Memory uMem = new NativeMemory(memByteArr);
    
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    int memBytes = (k << 4) + (Family.UNION.getMinPreLongs() << 3);
    byte[] memByteArr = new byte[memBytes];
    Memory uMem = new NativeMemory(memByteArr);
    
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
  public void checkWrapExact() {
    int lgK = 9; //512
    int k = 1 << lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i); //256
    for (int i=k/2; i<k; i++) usk2.update(i); //256 no overlap
    
    double usk1est = usk1.getEstimate();
    double usk2est = usk2.getEstimate();
    
    int memBytes = (k << 4) + (Family.UNION.getMinPreLongs() << 3);
    byte[] memByteArr = new byte[memBytes];
    Memory uMem = new NativeMemory(memByteArr);
    
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = k;
    
    byte[] byteArr1 = union.toByteArray();
    Memory uMem2 = new NativeMemory(byteArr1);
    Union union2 = (Union)SetOperation.wrap(uMem2);
    
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
  public void checkWrapEstNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2*k
    for (int i=u/2; i<u; i++) usk2.update(i); //2*k no overlap
    
//    double usk1est = usk1.getEstimate();
//    double usk2est = usk2.getEstimate();
    
    int memBytes = (k << 4) + (Family.UNION.getMinPreLongs() << 3);
    byte[] memByteArr = new byte[memBytes];
    Memory uMem = new NativeMemory(memByteArr);
    
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = u;
    
    byte[] byteArr1 = union.toByteArray();
    Memory uMem2 = new NativeMemory(byteArr1);
    Union union2 = (Union)SetOperation.wrap(uMem2);
    
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
  public void checkWrapEstNoOverlapOrderedIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2*k
    for (int i=u/2; i<u; i++) usk2.update(i); //2*k no overlap
    
//    double usk1est = usk1.getEstimate();
//    double usk2est = usk2.getEstimate();
    
    CompactSketch compOrdered = usk2.compact(true, null);
    
    int memBytes = (k << 4) + (Family.UNION.getMinPreLongs() << 3);
    byte[] memByteArr = new byte[memBytes];
    Memory uMem = new NativeMemory(memByteArr);
    
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1);
    union.update(compOrdered);
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch);
    union.update(null);
    
    double exactUnionAnswer = u;
    
    byte[] byteArr1 = union.toByteArray();
    Memory uMem2 = new NativeMemory(byteArr1);
    Union union2 = (Union)SetOperation.wrap(uMem2);
    
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
  
  @Test //Himanshu's issue
  public void checkDirectWrap() {
    int nomEntries = 16;
    Memory unionMem = new NativeMemory(new byte[SetOperation.getMaxUnionBytes(nomEntries)]);
    SetOperation.builder().setMemory(unionMem).build(nomEntries, Family.UNION);

    UpdateSketch sk1 = UpdateSketch.builder().build(nomEntries);
    sk1.update("a");
    sk1.update("b");
    
    UpdateSketch sk2 = UpdateSketch.builder().build(nomEntries);
    sk2.update("c");
    sk2.update("d");

    Union union = (Union) SetOperation.wrap(unionMem);
    union.update(sk1);

    union = (Union) SetOperation.wrap(unionMem);
    union.update(sk2);

    CompactSketch sketch = union.getResult(true, null);
    assertEquals(4.0, sketch.getEstimate(), 0.0);
  }
  
  @Test
  public void checkEmptyUnionCompactResult() {
    int k = 64;
    Union union = (Union) SetOperation.builder().build(k, Family.UNION);
    int bytes = Sketch.getMaxCompactSketchBytes(0);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    CompactSketch csk = union.getResult(false, mem); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }
  
  @Test
  public void checkEmptyUnionCompactOrderedResult() {
    int k = 64;
    Union union = (Union) SetOperation.builder().build(k, Family.UNION);
    int bytes = Sketch.getMaxCompactSketchBytes(0);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    CompactSketch csk = union.getResult(true, mem); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }
  
  @Test
  public void checkUnionMemToString() {
    int k = 64;
    int bytes = SetOperation.getMaxUnionBytes(k);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    SetOperation.builder().setMemory(mem).build(k, Family.UNION);
    println(PreambleUtil.toString(mem));
  }
  
  @Test
  public void printlnTest() {
    println("Test");
  }
  
  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //Disable here
  }

}