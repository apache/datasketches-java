/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.QUICKSELECT;
import static com.yahoo.sketches.ResizeFactor.X1;
import static com.yahoo.sketches.ResizeFactor.X2;
import static com.yahoo.sketches.ResizeFactor.X8;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.HeapQuickSelectSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

/** 
 * @author Lee Rhodes
 */
public class HeapQuickSelectSketchTest {
  private Family fam_ = QUICKSELECT;
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadSerVer() {
    int k = 512;
    int u = k;
    long seed = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks
        
    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) sk1.update(i);

    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);
    
    byte[] byteArray = usk.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    mem.putByte(SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte
    
    Sketch.heapify(mem, seed);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkIllegalSketchID_UpdateSketch() {
    int k = 512;
    int u = k;
    long seed = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks
    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) usk.update(i);
    
    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);
    byte[] byteArray = usk.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte
    
    //try to heapify the corruped mem
    Sketch.heapify(mem, seed);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkHeapifySeedConflict() {
    int k = 512;
    long seed1 = 1021;
    long seed2 = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed1).build(k);
    byte[] byteArray = usk.toByteArray();
    Memory srcMem = new NativeMemory(byteArray);
    Sketch.heapify(srcMem, seed2);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkHeapifyCorruptLgNomLongs() {
    UpdateSketch usk = UpdateSketch.builder().build(16);
    Memory srcMem = new NativeMemory(usk.toByteArray());
    srcMem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(srcMem, DEFAULT_UPDATE_SEED);
  }
  
  @Test
  public void checkHeapifyByteArrayExact() {
    int k = 512;
    int u = k;
    long seed = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).build(k);
    
    for (int i=0; i<u; i++) usk.update(i);
    
    int bytes = usk.getCurrentBytes(false);
    byte[] byteArray = usk.toByteArray();
    assertEquals(bytes, byteArray.length);
    
    Memory srcMem = new NativeMemory(byteArray);
    UpdateSketch usk2 = (UpdateSketch)Sketch.heapify(srcMem, seed);
    assertEquals(usk2.getEstimate(), u, 0.0);
    assertEquals(usk2.getLowerBound(2), u, 0.0);
    assertEquals(usk2.getUpperBound(2), u, 0.0);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), false);
    assertEquals(usk2.getClass().getSimpleName(), usk.getClass().getSimpleName());
    usk2.toString(true, true, 8, true);
  }
  
  @Test
  public void checkHeapifyByteArrayEstimating() {
    int k = 4096;
    int u = 2*k;
    long seed = DEFAULT_UPDATE_SEED;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).build(k);
    
    for (int i=0; i<u; i++) usk.update(i);
    
    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    assertEquals(usk.isEstimationMode(), true);
    byte[] byteArray = usk.toByteArray();
    
    Memory srcMem = new NativeMemory(byteArray);
    UpdateSketch usk2 = (UpdateSketch)Sketch.heapify(srcMem, seed);
    assertEquals(usk2.getEstimate(), uskEst);
    assertEquals(usk2.getLowerBound(2), uskLB);
    assertEquals(usk2.getUpperBound(2), uskUB);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), true);
    assertEquals(usk2.getClass().getSimpleName(), usk.getClass().getSimpleName());
  }
  
  @Test
  public void checkHeapifyMemoryEstimating() {
    int k = 512;
    int u = 2*k;
    long seed = DEFAULT_UPDATE_SEED;
    boolean estimating = (u > k);
    UpdateSketch sk1 = UpdateSketch.builder().setFamily(fam_).setSeed(seed).build(k);
    
    for (int i=0; i<u; i++) sk1.update(i);
    
    double sk1est = sk1.getEstimate();
    double sk1lb  = sk1.getLowerBound(2);
    double sk1ub  = sk1.getUpperBound(2);
    assertEquals(sk1.isEstimationMode(), estimating);
    
    byte[] byteArray = sk1.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    
    UpdateSketch sk2 = (UpdateSketch)Sketch.heapify(mem, DEFAULT_UPDATE_SEED);
    
    assertEquals(sk2.getEstimate(), sk1est);
    assertEquals(sk2.getLowerBound(2), sk1lb);
    assertEquals(sk2.getUpperBound(2), sk1ub);
    assertEquals(sk2.isEmpty(), false);
    assertEquals(sk2.isEstimationMode(), estimating);
    assertEquals(sk2.getClass().getSimpleName(), sk1.getClass().getSimpleName());
  }
  
  @Test
  public void checkHQStoCompactForms() {
    int k = 512;
    int u = 4*k;
    boolean estimating = (u > k);
    
    //boolean compact = false;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks
        
    assertEquals(usk.getClass().getSimpleName(), "HeapQuickSelectSketch");
    assertFalse(usk.isDirect());
    assertFalse(usk.isCompact());
    assertFalse(usk.isOrdered());
    
    for (int i=0; i<u; i++) usk.update(i);
    
    sk1.rebuild(); //forces size back to k
    
    //get baseline values
    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    int uskBytes = usk.getCurrentBytes(false);    //size stored as UpdateSketch
    int uskCompBytes = usk.getCurrentBytes(true); //size stored as CompactSketch
    assertEquals(uskBytes, maxBytes);
    assertEquals(usk.isEstimationMode(), estimating);
    
    CompactSketch comp1, comp2, comp3, comp4;
    
    comp1 = usk.compact(false,  null);
    
    assertEquals(comp1.getEstimate(), uskEst);
    assertEquals(comp1.getLowerBound(2), uskLB);
    assertEquals(comp1.getUpperBound(2), uskUB);
    assertEquals(comp1.isEmpty(), false);
    assertEquals(comp1.isEstimationMode(), estimating);
    assertEquals(comp1.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp1.getClass().getSimpleName(), "HeapCompactSketch");
    
    comp2 = usk.compact(true, null);
    
    assertEquals(comp2.getEstimate(), uskEst);
    assertEquals(comp2.getLowerBound(2), uskLB);
    assertEquals(comp2.getUpperBound(2), uskUB);
    assertEquals(comp2.isEmpty(), false);
    assertEquals(comp2.isEstimationMode(), estimating);
    assertEquals(comp2.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp2.getClass().getSimpleName(), "HeapCompactOrderedSketch");
    
    byte[] memArr2 = new byte[uskCompBytes];
    Memory mem2 = new NativeMemory(memArr2);  //allocate mem for compact form
    
    comp3 = usk.compact(false,  mem2);  //load the mem2
    
    assertEquals(comp3.getEstimate(), uskEst);
    assertEquals(comp3.getLowerBound(2), uskLB);
    assertEquals(comp3.getUpperBound(2), uskUB);
    assertEquals(comp3.isEmpty(), false);
    assertEquals(comp3.isEstimationMode(), estimating);
    assertEquals(comp3.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp3.getClass().getSimpleName(), "DirectCompactSketch");
    
    mem2.clear();
    comp4 = usk.compact(true, mem2);
    
    assertEquals(comp4.getEstimate(), uskEst);
    assertEquals(comp4.getLowerBound(2), uskLB);
    assertEquals(comp4.getUpperBound(2), uskUB);
    assertEquals(comp4.isEmpty(), false);
    assertEquals(comp4.isEstimationMode(), estimating);
    assertEquals(comp4.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp4.getClass().getSimpleName(), "DirectCompactOrderedSketch");
    comp4.toString(false, true, 0, false);
  }
  
  @Test
  public void checkHQStoCompactEmptyForms() {
    int k = 512;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X2).build(k);
    println("lgArr: "+ usk.getLgArrLongs());
    
    
    //empty
    usk.toString(false, true, 0, false);
    boolean estimating = false;
    assertEquals(usk.getClass().getSimpleName(), "HeapQuickSelectSketch");
    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    int currentUSBytes = usk.getCurrentBytes(false);
    assertEquals(currentUSBytes, 32*8 + 24);  // clumsy, but a function of RF and TCF
    int compBytes = usk.getCurrentBytes(true); //compact form
    assertEquals(compBytes, 8);
    assertEquals(usk.isEstimationMode(), estimating);
    
    byte[] arr2 = new byte[compBytes];
    Memory mem2 = new NativeMemory(arr2);
    
    CompactSketch csk2 = usk.compact(false,  mem2);
    assertEquals(csk2.getEstimate(), uskEst);
    assertEquals(csk2.getLowerBound(2), uskLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertEquals(csk2.getClass().getSimpleName(), "DirectCompactSketch");
    
    CompactSketch csk3 = usk.compact(true, mem2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), uskEst);
    assertEquals(csk3.getLowerBound(2), uskLB);
    assertEquals(csk3.getUpperBound(2), uskUB);
    assertEquals(csk3.isEmpty(), true);
    assertEquals(csk3.isEstimationMode(), estimating);
    assertEquals(csk3.getClass().getSimpleName(), "DirectCompactOrderedSketch");
  }
  
  @Test
  public void checkExactMode() { 
    int k = 4096;
    int u = 4096;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) usk.update(i);
   
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);
  }
  
  @Test
  public void checkEstMode() {
    int k = 4096;
    int u = 2*k;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(ResizeFactor.X4).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) usk.update(i);
    
    assertTrue(sk1.getRetainedEntries(false) > k);
  }
  
  @Test
  public void checkSamplingMode() {
    int k = 4096;
    int u = k;
    float p = (float)0.5;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setP(p).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks
    
    for (int i = 0; i < u; i++ ) usk.update(i);

    double p2 = sk1.getP();
    double theta = sk1.getTheta();
    assertTrue(theta <= p2);
    
    double est = usk.getEstimate();
    double kdbl = k;
    assertEquals(kdbl, est, kdbl*.05);
    double ub = usk.getUpperBound(1);
    assertTrue(ub > est);
    double lb = usk.getLowerBound(1);
    assertTrue(lb < est);
  }
  
  @Test
  public void checkErrorBounds() {
    int k = 512;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X1).build(k);
    
    //Exact mode
    for (int i = 0; i < k; i++ ) usk.update(i);
    
    double est = usk.getEstimate();
    double lb = usk.getLowerBound(2);
    double ub = usk.getUpperBound(2);
    assertEquals(est, ub, 0.0);
    assertEquals(est, lb, 0.0);

    //Est mode
    int u = 10*k;
    for (int i = k; i < u; i++ ) {
      usk.update(i);
      usk.update(i); //test duplicate rejection
    }
    est = usk.getEstimate();
    lb = usk.getLowerBound(2);
    ub = usk.getUpperBound(2);
    assertTrue(est <= ub);
    assertTrue(est >= lb);
  }
  
  //Empty Tests
  @Test
  public void checkEmptyAndP() {
    //virgin, p = 1.0
    int k = 1024;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());
    usk.update(1);
    assertEquals(sk1.getRetainedEntries(true), 1);
    assertFalse(usk.isEmpty());
    
    //virgin, p = .001
    UpdateSketch usk2 = UpdateSketch.builder().setFamily(fam_).setP((float)0.001).build(k);
    sk1 = (HeapQuickSelectSketch)usk2;
    assertTrue(usk2.isEmpty());
    usk2.update(1); //will be rejected
    assertEquals(sk1.getRetainedEntries(true), 0);
    assertFalse(usk2.isEmpty());
    double est = usk2.getEstimate();
    //println("Est: "+est);
    assertEquals(est, 0.0, 0.0); //because curCount = 0
    double ub = usk2.getUpperBound(2); //huge because theta is tiny!
    //println("UB: "+ub);
    assertTrue(ub > 0.0);
    double lb = usk2.getLowerBound(2);
    assertTrue(lb <= est);
    //println("LB: "+lb);
  }
  
  @Test
  public void checkUpperAndLowerBounds() {
    int k = 512;
    int u = 2*k;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X2).build(k);
    
    for (int i = 0; i < u; i++ ) usk.update(i);
    
    double est = usk.getEstimate();
    double ub = usk.getUpperBound(1);
    double lb = usk.getLowerBound(1);
    assertTrue(ub > est);
    assertTrue(lb < est);
  }
  
  @Test
  public void checkRebuild() {
    int k = 16;
    int u = 4*k;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) usk.update(i);
    
    assertFalse(usk.isEmpty());
    assertTrue(usk.getEstimate() > 0.0);
    assertTrue(sk1.getRetainedEntries(false) > k);
    
    sk1.rebuild();
    assertEquals(sk1.getRetainedEntries(false), k);
    assertEquals(sk1.getRetainedEntries(true), k);
    sk1.rebuild();
    assertEquals(sk1.getRetainedEntries(false), k);
    assertEquals(sk1.getRetainedEntries(true), k);
  }
  
  @Test
  public void checkResetAndStartingSubMultiple() {
    int k = 1024;
    int u = 4*k;
    
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X8).build(k);
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks
        
    assertTrue(usk.isEmpty());
    
    for (int i=0; i<u; i++) usk.update(i);
    
    assertEquals(1 << sk1.getLgArrLongs(), 2*k);
    sk1.reset();
    ResizeFactor rf = sk1.getResizeFactor();
    int subMul = UpdateSketch.startingSubMultiple(11, rf, 5); //messy
    assertEquals(sk1.getLgArrLongs(), subMul);
    
    UpdateSketch usk2 = UpdateSketch.builder().setFamily(fam_).setResizeFactor(ResizeFactor.X1).build(k);
    sk1 = (HeapQuickSelectSketch)usk2;

    for (int i=0; i<u; i++) usk2.update(i);
    
    assertEquals(1 << sk1.getLgArrLongs(), 2*k);
    sk1.reset();
    rf = sk1.getResizeFactor();
    subMul = UpdateSketch.startingSubMultiple(11, rf, 5); //messy
    assertEquals(sk1.getLgArrLongs(), subMul);
    
    assertNull(sk1.getMemory());
    assertFalse(sk1.isOrdered());
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkNegativeHashes() {
    int k = 512;
    UpdateSketch qs = UpdateSketch.builder().setFamily(QUICKSELECT).build(k);
    qs.hashUpdate(-1L);
  }
  
  @Test
  public void checkFamily() {
    UpdateSketch sketch = Sketches.updateSketchBuilder().build();
    assertEquals(sketch.getFamily(), Family.QUICKSELECT);
  }
  
  @Test
  public void checkMemDeSerExceptions() {
    int k = 1024;
    UpdateSketch sk1 = UpdateSketch.builder().setFamily(QUICKSELECT).build(k);
    sk1.update(1L); //forces preLongs to 3
    byte[] bytearray1 = sk1.toByteArray();
    Memory mem = new NativeMemory(bytearray1);
    long pre0 = mem.getLong(0); 
    
    tryBadMem(mem, PREAMBLE_LONGS_BYTE, 2); //Corrupt PreLongs
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, SER_VER_BYTE, 2); //Corrupt SerVer
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, FAMILY_BYTE, 1); //Corrupt Family
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, FLAGS_BYTE, 2); //Corrupt READ_ONLY to true
    mem.putLong(0, pre0); //restore
    
    try {
      mem.putDouble(16, 0.5); //Corrupt the theta value
      HeapQuickSelectSketch.getInstance(mem, DEFAULT_UPDATE_SEED);
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }
    mem.putDouble(16, 1.0); //restore theta
    byte[] byteArray2 = new byte[bytearray1.length -1];
    Memory mem2 = new NativeMemory(byteArray2);
    NativeMemory.copy(mem, 0, mem2, 0, mem2.getCapacity());
    try {
      HeapQuickSelectSketch.getInstance(mem2, DEFAULT_UPDATE_SEED);
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
  
  private static void tryBadMem(Memory mem, int byteOffset, int byteValue) {
    try {
      mem.putByte(byteOffset, (byte) byteValue); //Corrupt
      HeapQuickSelectSketch.getInstance(mem, DEFAULT_UPDATE_SEED);
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }
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
