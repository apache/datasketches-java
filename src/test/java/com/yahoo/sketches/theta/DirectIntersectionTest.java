/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.SetOperation.getMaxIntersectionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 */
public class DirectIntersectionTest {
  
  @Test
  public void checkExactIntersectionNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i);
    for (int i=k/2; i<k; i++) usk2.update(i);
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();

    inter.update(usk1);
    inter.update(usk2);
    
    CompactSketch rsk1;
    boolean ordered = true;
    
    assertTrue(inter.hasResult());
    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);
    
    boolean compact = true;
    int bytes = rsk1.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    
    rsk1 = inter.getResult(!ordered, mem);
    assertEquals(rsk1.getEstimate(), 0.0);
    
    rsk1 = inter.getResult(ordered, mem); //executed twice to fully exercise the internal state machine
    assertEquals(rsk1.getEstimate(), 0.0); 
  }
  
  @Test
  public void checkExactIntersectionFullOverlap() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k; i++) usk1.update(i);
    for (int i=0; i<k; i++) usk2.update(i);
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(usk1);
    inter.update(usk2);
    
    CompactSketch rsk1;
    boolean ordered = true;
    
    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), (double)k);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), (double)k);
    
    boolean compact = true;
    int bytes = rsk1.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);
    
    rsk1 = inter.getResult(!ordered, mem); //executed twice to fully exercise the internal state machine
    assertEquals(rsk1.getEstimate(), (double)k);
    
    rsk1 = inter.getResult(ordered, mem);
    assertEquals(rsk1.getEstimate(), (double)k);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void checkIntersectionEarlyStop() {
    int lgK = 10;
    int k = 1<<lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u; i++) usk1.update(i);
    for (int i=u/2; i<u + u/2; i++) usk2.update(i);
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);
    
    Intersection inter =
        SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(csk1);
    inter.update(csk2);
    
    CompactSketch rsk1 = inter.getResult(true, null);
    println(""+rsk1.getEstimate());
  }
  
  @SuppressWarnings("unused")
  //Calling getResult on a virgin Intersect is illegal
  @Test(expectedExceptions = IllegalStateException.class)
  public void checkNoCall() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    assertFalse(inter.hasResult());
    CompactSketch rsk1 = inter.getResult(false, null);
  }
  
  @Test
  public void check1stCall() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk;
    CompactSketch rsk1;
    double est;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    //1st call = null
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(null);  
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0
    
    //1st call = empty
    sk = UpdateSketch.builder().build(k); //empty
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0
  
    //1st call = valid and not empty
    sk = UpdateSketch.builder().build(k);
    sk.update(1);
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est); // = 1
  }
  
  @Test
  public void check2ndCallAfterNull() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk;
    CompactSketch comp1;
    double est;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    //1st call = null
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(null);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = null
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(null);
    //2nd call = empty
    sk = UpdateSketch.builder().build(); //empty
    inter.update(sk);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = null
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(null);
    //2nd call = valid & not empty
    sk = UpdateSketch.builder().build(); 
    sk.update(1);
    inter.update(sk);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
  }
  
  @Test
  public void check2ndCallAfterEmpty() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    //2nd call = empty
    sk2 = UpdateSketch.builder().build(); //empty
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    //2nd call = valid and not empty
    sk2 = UpdateSketch.builder().build();
    sk2.update(1);
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
  }
  
  @Test
  public void check2ndCallAfterValid() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    //2nd call = empty
    sk2 = UpdateSketch.builder().build(); //empty
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().build(); //empty
    sk2.update(1);
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    //2nd call = valid not intersecting
    sk2 = UpdateSketch.builder().build(); //empty
    sk2.update(2);
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
  }
  
  @Test
  public void checkEstimatingIntersect() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk1.update(i);  //est mode
    println("sk1: "+sk1.getEstimate());
    
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    
    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk2.update(i);  //est mode
    println("sk2: "+sk2.getEstimate());
    
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkOverflow() {
    int lgK = 9; //512
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;
    
    int reqBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[reqBytes];
    Memory iMem = new NativeMemory(memArr);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build(2*k); // bigger sketch
    for (int i=0; i<4*k; i++) sk1.update(i);  //force est mode
    println("sk1est: "+sk1.getEstimate());
    println("sk1cnt: "+sk1.getRetainedEntries(true));
    
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
  }
  
  @Test
  public void checkHeapify() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1, comp2;
    double est, est2;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk1.update(i);  //est mode
    println("sk1: "+sk1.getEstimate());
    
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(sk1);
    
    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk2.update(i);  //est mode
    println("sk2: "+sk2.getEstimate());
    
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);
    
    byte[] byteArray = inter.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    Intersection inter2 = (Intersection) SetOperation.heapify(mem);
    comp2 = inter2.getResult(false, null);
    est2 = comp2.getEstimate();
    println("Est2: "+est2);
  }
  
  @Test
  public void checkWrapNullEmpty() {
    int lgK = 5;
    int k = 1<<lgK;
    Intersection inter1, inter2;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    inter1 = SetOperation.builder().initMemory(iMem).buildIntersection(); //virgin
    inter2 = Sketches.wrapIntersection(iMem);
    //both in virgin state, empty = false
    assertFalse(inter1.hasResult());
    assertFalse(inter2.hasResult());
    
    inter1.update(null);  //A virgin intersection intersected with null => empty = true;
    inter2 = Sketches.wrapIntersection(iMem);
    assertTrue(inter1.hasResult());
    assertTrue(inter2.hasResult());
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.curCount_, 0);
    assertTrue(comp.isEmpty());
  }
  
  @Test
  public void checkWrapNullEmpty2() {
    int lgK = 5;
    int k = 1<<lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    inter1 = SetOperation.builder().initMemory(iMem).buildIntersection(); //virgin
    inter2 = Sketches.wrapIntersection(iMem);
    //both in virgin state, empty = false
    assertFalse(inter1.hasResult());
    assertFalse(inter2.hasResult());
    
    sk1 = UpdateSketch.builder().setP((float) .005).setFamily(Family.QUICKSELECT).build(k);
    sk1.update(1); //very unlikely to go into cache due to p.
    //A virgin intersection (empty = false) intersected with a not-empty zero cache sketch
    //remains empty = false.
    
    inter1.update(sk1);
    inter2 = Sketches.wrapIntersection(iMem);
    assertTrue(inter1.hasResult());
    assertTrue(inter2.hasResult());
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.curCount_, 0);
    assertFalse(comp.isEmpty());
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSizeLowerLimit() {
    int k = 8;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    Intersection inter = SetOperation.builder().initMemory(iMem).buildIntersection();
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSizedTooSmall() {
    int lgK = 5;
    int k = 1<<lgK;
    int u = 4*k;
    
    int memBytes = getMaxIntersectionBytes(k/2);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u; i++) usk1.update(i);
    
    CompactSketch csk1 = usk1.compact(true, null);
    
    Intersection inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(csk1);
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadPreambleLongs() {
    int k = 32;
    Intersection inter1, inter2;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    inter1 = SetOperation.builder().initMemory(iMem).buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    //corrupt:
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 2);//RF not used = 0
    inter2 = Sketches.wrapIntersection(mem);
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadSerVer() {
    int k = 32;
    Intersection inter1, inter2;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    inter1 = SetOperation.builder().initMemory(iMem).buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    //corrupt:
    mem.putByte(SER_VER_BYTE, (byte) 2);
    inter2 = Sketches.wrapIntersection(mem); //throws in SetOperations
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = ClassCastException.class)
  public void checkFamilyID() {
    int k = 32;
    Union union;
    Intersection inter1;
    
    union = SetOperation.builder().buildUnion(k);
    byte[] byteArray = union.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    inter1 = Sketches.wrapIntersection(mem);
  }
  
  @Test
  public void checkWrap() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter, inter2, inter3;
    UpdateSketch sk1, sk2;
    CompactSketch resultComp1, resultComp2;
    double est, est2;
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr1 = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr1);
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk1.update(i);  //est mode
    CompactSketch compSkIn1 = sk1.compact(true, null);
    println("compSkIn1: "+compSkIn1.getEstimate());
    
    inter = SetOperation.builder().initMemory(iMem).buildIntersection();
    inter.update(compSkIn1);
    
    byte[] memArr2 = inter.toByteArray();
    Memory srcMem = new NativeMemory(memArr2);
    inter2 = Sketches.wrapIntersection(srcMem);
    
    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk2.update(i);  //est mode
    CompactSketch compSkIn2 = sk2.compact(true, null);
    println("sk2: "+compSkIn2.getEstimate());
    
    inter2.update(compSkIn2);
    resultComp1 = inter2.getResult(false, null);
    est = resultComp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);
    
    byte[] memArr3 = inter2.toByteArray();
    Memory srcMem2 = new NativeMemory(memArr3);
    inter3 = Sketches.wrapIntersection(srcMem2);
    resultComp2 = inter3.getResult(false, null);
    est2 = resultComp2.getEstimate();
    println("Est2: "+est2);
    
    inter.reset();
    inter2.reset();
    inter3.reset();
  }
  
  @SuppressWarnings("unused")
  @Test
  public void checkDefaultMinSize() {
   DirectIntersection di = new DirectIntersection(9001L, new NativeMemory(new byte[32*8 + 24])); 
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkExceptionSizes2() {
   DirectIntersection di = new DirectIntersection(9001L, new NativeMemory(new byte[16*8 + 24])); 
  }
  
  @Test
  public void checkGetResult() {
    int k = 1024;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    Memory iMem = new NativeMemory(memArr);
    
    Intersection inter = Sketches.setOperationBuilder().initMemory(iMem).buildIntersection();
    inter.update(sk);
    CompactSketch csk = inter.getResult();
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
  
//  public static void main(String[] args) {
//    DirectIntersectionTest dit = new DirectIntersectionTest();
//    dit.check2ndCallAfterValid();
//  }
}
