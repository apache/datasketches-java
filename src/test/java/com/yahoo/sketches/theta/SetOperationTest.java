/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.Sketch.getMaxUpdateSketchBytes;
import static com.yahoo.sketches.theta.SetOperation.*;
import static com.yahoo.sketches.Family.A_NOT_B;
import static com.yahoo.sketches.Family.INTERSECTION;
import static com.yahoo.sketches.Family.UNION;
import static com.yahoo.sketches.ResizeFactor.X4;
import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 */
public class SetOperationTest {

  @Test
  public void checkBuilder() {
    int k = 2048;
    long seed = 1021;
    
    UpdateSketch usk1 = UpdateSketch.builder().setSeed(seed).build(k);
    UpdateSketch usk2 = UpdateSketch.builder().setSeed(seed).build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i); //256
    for (int i=k/2; i<k; i++) usk2.update(i); //256 no overlap
    
    ResizeFactor rf = X4;
    //use default size
    Union union = SetOperation.builder().setSeed(seed).setResizeFactor(rf).buildUnion();
    
    union.update(usk1);
    union.update(usk2);
    
    double exactUnionAnswer = k;
    
    CompactSketch comp1 = union.getResult(false, null); //ordered: false
    double compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
  }
  
  @Test
  public void checkBuilder2() {
    SetOperationBuilder bldr = SetOperation.builder();
    
    long seed = 12345L;
    bldr.setSeed(seed);
    assertEquals(seed, bldr.getSeed());
    
    float p = (float)0.5;
    bldr.setP(p);
    assertEquals(p, bldr.getP());
    
    ResizeFactor rf = ResizeFactor.X4;
    bldr.setResizeFactor(rf);
    assertEquals(rf, bldr.getResizeFactor());
    
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
  public void checkBuilderNonPowerOf2() {
    SetOperation.builder().buildUnion(1000);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBuilderBadFamily() {
    SetOperation.builder().build(Family.ALPHA);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBuilderIllegalPhi() {
    float p = (float)1.5;
    SetOperation.builder().setP(p).buildUnion();
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBuilderIllegalPlo() {
    float p = 0;
    SetOperation.builder().setP(p).buildUnion();
  }
  
  @Test
  public void checkBuilderValidP() {
    float p = (float).5;
    SetOperation.builder().setP(p).buildUnion();
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBuilderAnotB_noMem() {
    Memory mem = new NativeMemory(new byte[64]);
    SetOperation.builder().initMemory(mem).buildANotB();
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBuilderBadSeedHashes() {
    int k = 2048;
    long seed = 1021;
    
    UpdateSketch usk1 = UpdateSketch.builder().setSeed(seed).build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    
    for (int i=0; i<k/2; i++) usk1.update(i); //256
    for (int i=k/2; i<k; i++) usk2.update(i); //256 no overlap
    
    ResizeFactor rf = X4;
    
    Union union = SetOperation.builder().setSeed(seed).setResizeFactor(rf).buildUnion(k);
    
    union.update(usk1);
    union.update(usk2); //throws seed exception here
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkIllegalSetOpHeapify() {
    int k = 64;
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    for (int i=0; i<k; i++) usk1.update(i); //64
    byte[] byteArray = usk1.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    SetOperation.heapify(mem);
  } 
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkIllegalSetOpWrap() {
    int k = 64;
    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    for (int i=0; i<k; i++) usk1.update(i); //64
    byte[] byteArray = usk1.toByteArray();
    Memory mem = new NativeMemory(byteArray);
    SetOperation.wrap(mem);
  }
  
  @Test
  public void checkBuildSetOps() {
    SetOperationBuilder bldr = Sketches.setOperationBuilder();
    bldr.buildUnion();
    bldr.buildIntersection();
    bldr.buildANotB();
  }
  
  @Test
  public void checkComputeLgArrLongs() {
    assertEquals(computeMinLgArrLongsFromCount(30), 5);
    assertEquals(computeMinLgArrLongsFromCount(31), 6);
  }
  
  /**
   * The objective is to union 3 16K sketches into a union SetOperation and get the result.
   * All operations are to be performed within a single direct ByteBuffer as the backing store. 
   * First we will make the union size large enough so that its answer will be exact (with this
   * specific example).  
   * <p> Next, we recover the Union SetOp and the 3 sketches and the space for the result. Then
   * recompute the union using a Union of the same size as the input sketches, where the end result
   * will be an estimate.  
   */
  @Test
  public void checkDirectUnionExample() {
    //The first task is to compute how much direct memory we need and set the heap large enough.  
    //For the first trial, we will set the Union large enough for an exact result for THIS example.
    int sketchNomEntries = 1 << 14; //16K
    int unionNomEntries = 1 << 15;  //32K
    int[] heapLayout = getHeapLayout(sketchNomEntries, unionNomEntries);
    
    //This BB belongs to you and you always retain a link to it until you are completely
    // done and then let java garbage collect it.
    //I use a heap backing array, because for this example it is easier to peak into it and
    // see what is going on.
    byte[] backingArr = new byte[heapLayout[5]];
    ByteBuffer heapBuf = ByteBuffer.wrap(backingArr);
    
    // Attaches a NM object to the underlying memory of heapBuf.
    // heapMem will have a Read/Write view of the complete backing memory of heapBuf (direct or not).
    // Any R/W action from heapMem will be visible via heapBuf and visa versa.
    // 
    // However, if you had created this NM object directly in raw, off-heap "native" memory
    // you would have the responsibility to clear it, and free it to the OS when you
    // are done.  But, since it was allocated via BB, it does the clearing and freeing for you. 
    NativeMemory heapMem = new NativeMemory(heapBuf);
    
    double result = directUnionTrial1(heapMem, heapLayout, sketchNomEntries, unionNomEntries);
    println("1st est: "+result);
    int expected = sketchNomEntries*2;
    assertEquals(result, expected, 0.0); //est must be exact.
    
    //For trial 2, we will use the same union space but use only part of it.
    unionNomEntries = 1 << 14; //16K
    result = directUnionTrial2(heapMem, heapLayout, sketchNomEntries, unionNomEntries);

    //intentially loose bounds 
    assertEquals(result, expected, expected*0.05); 
    println("2nd est: "+result);
    println("Error %: "+((result/expected -1.0)*100));
    
    //We are done with this NativeMemory. It is a good habit to free it, even though, 
    // in this specific case, the BB backing memory is managed by the JVM, so it is
    // not strictly necessary.
    //Forgetting to do this could lead to segment faults, which are very difficult to troubleshoot.
    heapMem.freeMemory();
  }
  
  /**
   * Compute offsets for MyHeap for Union, sketch1, sketch2, sketch3, resultSketch, total layout.
   * @return array of offsets for Union, sketch1, sketch2, sketch3, resultSketch, total layout
   */
  private static int[] getHeapLayout(int sketchNomEntries, int unionNomEntries) {
    int[] heapLayout = new int[6];
    int unionBytes = SetOperation.getMaxUnionBytes(unionNomEntries);
    int sketchBytes = getMaxUpdateSketchBytes(sketchNomEntries);
    int resultBytes = Sketch.getMaxCompactSketchBytes(unionNomEntries);
    heapLayout[0] = 0;                             //offset for Union
    heapLayout[1] = unionBytes;                    //offset for sketch1
    heapLayout[2] = unionBytes + sketchBytes;      //offset for sketch2
    heapLayout[3] = unionBytes + 2*sketchBytes;    //offset for sketch3
    heapLayout[4] = unionBytes + 3*sketchBytes;    //offset for result
    heapLayout[5] = unionBytes + 3*sketchBytes + resultBytes;  //total
    return heapLayout;
  }
  
  private static double directUnionTrial1(
      Memory heapMem, int[] heapLayout, int sketchNomEntries, int unionNomEntries) {

    int offset = heapLayout[0];
    int bytes = heapLayout[1] - offset;
    Memory unionMem = new MemoryRegion(heapMem, offset, bytes);

    Union union = SetOperation.builder().initMemory(unionMem).buildUnion(unionNomEntries);

    Memory sketch1mem = new MemoryRegion(heapMem, heapLayout[1], heapLayout[2]-heapLayout[1]);
    Memory sketch2mem = new MemoryRegion(heapMem, heapLayout[2], heapLayout[3]-heapLayout[2]);
    Memory sketch3mem = new MemoryRegion(heapMem, heapLayout[3], heapLayout[4]-heapLayout[3]);
    Memory resultMem = new MemoryRegion(heapMem, heapLayout[4], heapLayout[5]-heapLayout[4]);
    
    //Initialize the 3 sketches
    UpdateSketch sk1 = UpdateSketch.builder().initMemory(sketch1mem).build(sketchNomEntries);
    UpdateSketch sk2 = UpdateSketch.builder().initMemory(sketch2mem).build(sketchNomEntries);
    UpdateSketch sk3 = UpdateSketch.builder().initMemory(sketch3mem).build(sketchNomEntries);
    
    //This little trial has sk1 and sk2 distinct and sk2 overlap both.
    //Build the sketches. 
    for (int i=0; i< sketchNomEntries; i++) {
      sk1.update(i);
      sk2.update(i + sketchNomEntries/2);
      sk3.update(i + sketchNomEntries);
    }
    
    //confirm that each of these 3 sketches is exact.
    assertEquals(sk1.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk2.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk3.getEstimate(), sketchNomEntries, 0.0);
    
    //Let's union the first 2 sketches
    union.update(sk1);
    union.update(sk2);
    
    //Let's recover the union and the 3rd sketch
    union = Sketches.wrapUnion(unionMem);
    sk3 = (UpdateSketch) Sketch.wrap(sketch3mem);
    union.update(sk3);
    
    Sketch resSk = union.getResult(true, resultMem);
    double est = resSk.getEstimate();
        
    return est;
  }
  
  private static double directUnionTrial2(
      Memory heapMem, int[] heapLayout, int sketchNomEntries, int unionNomEntries) {
    
    Memory unionMem = new MemoryRegion(heapMem, heapLayout[0], heapLayout[1]-heapLayout[0]);
    Memory sketch1mem = new MemoryRegion(heapMem, heapLayout[1], heapLayout[2]-heapLayout[1]);
    Memory sketch2mem = new MemoryRegion(heapMem, heapLayout[2], heapLayout[3]-heapLayout[2]);
    Memory sketch3mem = new MemoryRegion(heapMem, heapLayout[3], heapLayout[4]-heapLayout[3]);
    Memory resultMem = new MemoryRegion(heapMem, heapLayout[4], heapLayout[5]-heapLayout[4]);
    
    //Recover the 3 sketches
    UpdateSketch sk1 = (UpdateSketch) Sketch.wrap(sketch1mem);
    UpdateSketch sk2 = (UpdateSketch) Sketch.wrap(sketch2mem);
    UpdateSketch sk3 = (UpdateSketch) Sketch.wrap(sketch3mem);
        
    //confirm that each of these 3 sketches is exact.
    assertEquals(sk1.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk2.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk3.getEstimate(), sketchNomEntries, 0.0);

    //Create a new union in the same space with a smaller size.
    unionMem.clear();
    Union union = SetOperation.builder().initMemory(unionMem).buildUnion(unionNomEntries);
    union.update(sk1);
    union.update(sk2);
    union.update(sk3);
    
    Sketch resSk = union.getResult(true, resultMem);
    double est = resSk.getEstimate();
        
    return est;
  }
  
  @Test
  public void checkValidSetOpID() {
    assertFalse(SetOperation.isValidSetOpID(0));
    assertTrue(SetOperation.isValidSetOpID(UNION.getID()));
    assertTrue(SetOperation.isValidSetOpID(INTERSECTION.getID()));
    assertTrue(SetOperation.isValidSetOpID(A_NOT_B.getID()));

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