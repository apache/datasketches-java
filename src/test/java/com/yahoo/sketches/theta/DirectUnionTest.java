/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.ForwardCompatibilityTest.convertSerV3toSerV1;
import static com.yahoo.sketches.theta.ForwardCompatibilityTest.convertSerV3toSerV2;
import static com.yahoo.sketches.theta.HeapUnionTest.*;
import static com.yahoo.sketches.theta.SetOperation.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
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
    int u = k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //256
    for (int i=u/2; i<u; i++) usk2.update(i); //256 no overlap
    
    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch
    
    testAllCompactForms(union, u, 0.0);
    
    Union union2 = (Union)SetOperation.heapify(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.0);
  }
  
  //these parallel the checkHeapifyExact, etc.
  @Test 
  public void checkWrapExact() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u/2; i++) usk1.update(i); //256
    for (int i=u/2; i<u; i++) usk2.update(i); //256 no overlap
    
    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch
    
    testAllCompactForms(union, u, 0.0);
    
    Union union2 = (Union)SetOperation.wrap(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.0);
  }
  
  @Test
  public void checkWrapEstNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2k
    for (int i=u/2; i<u; i++) usk2.update(i); //2k no overlap, exact
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch, early stop not possible
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.wrap(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
  }
  
  @Test
  public void checkWrapEstNoOverlapOrderedIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact for early stop test
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2k estimating
    for (int i=u/2; i<u; i++) usk2.update(i); //2k no overlap, exact, will force early stop
    
    CompactSketch cosk2 = usk2.compact(true, null);
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1);  //update with heap UpdateSketch
    union.update(cosk2); //update with heap Compact, Ordered input, early stop
    
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch); //updates with empty
    emptySketch = null;
    union.update(emptySketch); //updates with null
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.wrap(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
    
    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }
  
  @Test
  public void checkWrapEstNoOverlapOrderedDirectIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k); //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact for early stop test
    
    for (int i=0; i<u/2; i++) usk1.update(i); //2k estimating
    for (int i=u/2; i<u; i++) usk2.update(i);  //2k no overlap, exact, will force early stop
    
    NativeMemory cskMem2 = new NativeMemory(new byte[usk2.getCurrentBytes(true)]);
    CompactSketch cosk2 = usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1);      //updates with heap UpdateSketch
    union.update(cosk2);     //updates with direct CompactSketch, ordered, use early stop
    
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.wrap(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
    
    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }
  
  @Test
  public void checkWrapEstNoOverlapOrderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact for early stop test
    
    for (int i=0; i<u/2; i++) usk1.update(i);  //2k estimating
    for (int i=u/2; i<u; i++) usk2.update(i);  //2k no overlap, exact, will force early stop
    
    NativeMemory cskMem2 = new NativeMemory(new byte[usk2.getCurrentBytes(true)]);
    usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cskMem2);     //updates with direct CompactSketch, ordered, use early stop
    
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.wrap(new NativeMemory(union.toByteArray()));
    
    testAllCompactForms(union2, u, 0.05);
    
    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }
  
  @Test
  public void checkWrapEstNoOverlapUnorderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().build(2*k); //2k exact for early stop test
    
    for (int i=0; i<u/2; i++) usk1.update(i);  //2k estimating
    for (int i=u/2; i<u; i++) usk2.update(i);  //2k no overlap, exact, will force early stop
    
    NativeMemory cskMem2 = new NativeMemory(new byte[usk2.getCurrentBytes(true)]);
    usk2.compact(false, cskMem2); //unordered, loads the cskMem2 as unordered
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cskMem2);     //updates with direct CompactSketch, ordered, use early stop
    
    UpdateSketch emptySketch = UpdateSketch.builder().build(k);
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch
    
    testAllCompactForms(union, u, 0.05);
    
    Union union2 = (Union)SetOperation.wrap(new NativeMemory(union.toByteArray()));
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
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
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    union.update(v1mem1);
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
    
    Memory v2mem1 = convertSerV3toSerV2(v3mem1);
    
    uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    union.update(v2mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
    
    uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    union.update(v3mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
    
    uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    v3mem1 = null;
    union.update(v3mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
  }
  
  //Special DirectUnion cases
  @Test //Himanshu's issue
  public void checkDirectWrap() {
    int nomEntries = 16;
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(nomEntries)]);
    SetOperation.builder().setMemory(uMem).build(nomEntries, Family.UNION);
    
    UpdateSketch sk1 = UpdateSketch.builder().build(nomEntries);
    sk1.update("a");
    sk1.update("b");
    
    UpdateSketch sk2 = UpdateSketch.builder().build(nomEntries);
    sk2.update("c");
    sk2.update("d");
    
    Union union = (Union) SetOperation.wrap(uMem);
    union.update(sk1);
    
    union = (Union) SetOperation.wrap(uMem);
    union.update(sk2);
    
    CompactSketch sketch = union.getResult(true, null);
    assertEquals(4.0, sketch.getEstimate(), 0.0);
  }
  
  @Test
  public void checkEmptyUnionCompactResult() {
    int k = 64;
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    Memory mem = new NativeMemory(new byte[Sketch.getMaxCompactSketchBytes(0)]);
    CompactSketch csk = union.getResult(false, mem); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }
  
  @Test
  public void checkEmptyUnionCompactOrderedResult() {
    int k = 64;
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = (Union)SetOperation.builder().setMemory(uMem).build(k, Family.UNION);
    
    Memory mem = new NativeMemory(new byte[Sketch.getMaxCompactSketchBytes(0)]);
    CompactSketch csk = union.getResult(true, mem); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }
  
  @Test
  public void checkUnionMemToString() {
    int k = 64;
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]); //union memory
    SetOperation.builder().setMemory(uMem).build(k, Family.UNION);

    //println(PreambleUtil.toString(uMem));
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