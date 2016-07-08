/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * @author Lee Rhodes
 */
public class HeapIntersectionTest {

  @Test
  public void checkExactIntersectionNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i);
    for (int i=k/2; i<k; i++) usk2.update(i);
    
    Intersection inter = SetOperation.builder().buildIntersection();

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

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k; i++) usk1.update(i);
    for (int i=0; i<k; i++) usk2.update(i);
    
    Intersection inter = SetOperation.builder().buildIntersection();
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
  
  @Test
  public void checkIntersectionEarlyStop() {
    int lgK = 10;
    int k = 1<<lgK;
    int u = 4*k;
    
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<u; i++) usk1.update(i);
    for (int i=u/2; i<u + u/2; i++) usk2.update(i); //inter 512
    
    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);
    
    Intersection inter = SetOperation.builder().buildIntersection();
    inter.update(csk1);
    inter.update(csk2);
    
    CompactSketch rsk1 = inter.getResult(true, null);
    println(""+rsk1.getEstimate());
  }
  
  //Calling getResult on a virgin Intersect is illegal
  @Test(expectedExceptions = SketchesStateException.class)
  public void checkNoCall() {
    Intersection inter = SetOperation.builder().buildIntersection();
    assertFalse(inter.hasResult());
    inter.getResult(false, null);
  }
  
  @Test
  public void check1stCall() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk;
    CompactSketch rsk1;
    double est;
    
    //1st call = null
    inter = SetOperation.builder().buildIntersection();
    inter.update(null);  
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0
    
    //1st call = empty
    sk = UpdateSketch.builder().build(k); //empty
    inter = SetOperation.builder().buildIntersection();
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0
  
    //1st call = valid and not empty
    sk = UpdateSketch.builder().build(k);
    sk.update(1);
    inter = SetOperation.builder().buildIntersection();
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est); // = 1
  }
  
  @Test
  public void check2ndCallAfterNull() {
    Intersection inter;
    UpdateSketch sk;
    CompactSketch comp1;
    double est;
    
    //1st call = null
    inter = SetOperation.builder().buildIntersection();
    inter.update(null);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = null
    inter = SetOperation.builder().buildIntersection();
    inter.update(null);
    //2nd call = empty
    sk = UpdateSketch.builder().build(); //empty
    inter.update(sk);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = null
    inter = SetOperation.builder().buildIntersection();
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
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;
    
    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection();
    inter.update(sk1);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
    
    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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
    
    //1st call = valid
    sk1 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk1.update(i);  //est mode
    println("sk1: "+sk1.getEstimate());
    
    inter = SetOperation.builder().buildIntersection();
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
  
  @Test
  public void checkHeapify() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter, inter2;
    UpdateSketch sk1, sk2;
    CompactSketch resultComp1, resultComp2;
    double est, est2;
    
    sk1 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk1.update(i);  //est mode
    CompactSketch compSkIn1 = sk1.compact(true, null);
    println("compSkIn1: "+compSkIn1.getEstimate());
    
    //1st call = valid
    inter = SetOperation.builder().buildIntersection();
    inter.update(compSkIn1);
    
    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().build(k);
    for (int i=0; i<2*k; i++) sk2.update(i);  //est mode
    CompactSketch compSkIn2 = sk2.compact(true, null);
    println("compSkIn2: "+compSkIn2.getEstimate());
    
    inter.update(compSkIn2);
    resultComp1 = inter.getResult(false, null);
    est = resultComp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);
    
    byte[] byteArray = inter.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    inter2 = (Intersection) SetOperation.heapify(mem);
    //inter2 = new Intersection(mem, seed);
    resultComp2 = inter2.getResult(false, null);
    est2 = resultComp2.getEstimate();
    println("Est2: "+est2);
    
    inter.reset();
    inter2.reset();
  }
  
  @Test
  public void checkHeapifyNullEmpty() {
    Intersection inter1, inter2;
    
    inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory srcMem = new NativeMemory(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    //inter2 is in virgin state, empty = false
    
    inter1.update(null);  //A virgin intersection intersected with null => empty = true;
    byteArray = inter1.toByteArray(); //update the byteArray
    
    srcMem = new NativeMemory(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertTrue(comp.isEmpty());
  }
  
  @Test
  public void checkHeapifyNullEmpty2() {
    int lgK = 5;
    int k = 1<<lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;
    
    inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory srcMem = new NativeMemory(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    //inter2 is in virgin state
    
    sk1 = UpdateSketch.builder().setP((float) .005).setFamily(Family.QUICKSELECT).build(k);
    sk1.update(1);
    //A virgin intersection (empty = false) intersected with a not-empty zero cache sketch
    //remains empty = false!
    
    inter1.update(sk1);
    byteArray = inter1.toByteArray();
    srcMem = new NativeMemory(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertFalse(comp.isEmpty());
  }
  
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreambleLongs() {
    Intersection inter1;
    
    inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    //corrupt:
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 2);//RF not used = 0
    SetOperation.heapify(mem);
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 32;
    Intersection inter1;
    
    inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    //corrupt:
    mem.putByte(SER_VER_BYTE, (byte) 2);
    SetOperation.heapify(mem);
  }
  
  @Test(expectedExceptions = ClassCastException.class)
  public void checkFamilyID() {
    int k = 32;
    Union union;
    
    union = SetOperation.builder().buildUnion(k);
    byte[] byteArray = union.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    Intersection inter1 = (Intersection) SetOperation.heapify(mem); //bad cast
    inter1.reset();
  }
  
  @Test
  public void checkGetResult() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
  
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.update(sk);
    CompactSketch csk = inter.getResult();
    assertEquals(csk.getCurrentBytes(true), 8);
  }
  
  @Test
  public void checkFamily() {
    //cheap trick
    HeapIntersection heapI = new HeapIntersection(Util.DEFAULT_UPDATE_SEED);
    assertEquals(heapI.getFamily(), Family.INTERSECTION);
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
