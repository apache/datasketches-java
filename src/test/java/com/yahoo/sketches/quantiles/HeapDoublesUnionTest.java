/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapDoublesQuantilesSketchTest.buildQS;
import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

public class HeapDoublesUnionTest {

  @Test
  public void checkUnion1() {
    DoublesQuantilesSketch result;
    DoublesQuantilesSketch qs1 = null;
    DoublesUnion union = DoublesUnion.builder().build(128); //virgin
    
    qs1 = buildQS(256, 1000); //first 1000
    union.update(qs1); //copy   me = null,  that = valid, OK
    
    //check copy   me = null,  that = valid
    result = union.getResult();
    assertEquals(result.getN(), 1000);
    assertEquals(result.getK(), 256);
    
    //check merge  me = valid, that = valid, both K's the same
    DoublesQuantilesSketch qs2 = buildQS(256, 1000, 1000); //add 1000
    union.update(qs2); 
    result = union.getResult();
    assertEquals(result.getN(), 2000);
    assertEquals(result.getK(), 256);
  }
  
  @Test
  public void checkUnion2() {
    DoublesQuantilesSketch qs1 = buildQS(256, 1000);
    DoublesQuantilesSketch qs2 = buildQS(128, 1000);
    DoublesUnion union = DoublesUnion.builder().build(128); //virgin
    
    union.update(qs1);
    DoublesQuantilesSketch res1 = union.getResult();
    //println(res1.toString());
    assertEquals(res1.getN(), 1000);
    assertEquals(res1.getK(), 256);
    
    union.update(qs2);
    DoublesQuantilesSketch res2 = union.getResult();
    assertEquals(res2.getN(), 2000);
    assertEquals(res2.getK(), 128);
    //println(union.toString());
  }
  
  @Test
  public void checkUpdateMemory() {
    DoublesQuantilesSketch qs1 = buildQS(256, 1000);
    int bytes = qs1.getStorageBytes();
    Memory dstMem = new NativeMemory(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;
    
    DoublesUnion union = DoublesUnion.builder().build(128); //virgin
    union.update(srcMem);
    for (int i=1000; i<2000; i++) union.update(i);
    DoublesQuantilesSketch qs2 = union.getResult();
    assertEquals(qs2.getMaxValue(), 1999, 0.0);
    String s = union.toString();
    println(s); //enable printing to see
    union.reset(); //sets to null
  }
  
  @Test
  public void checkUnionUpdateLogic() {
    HeapDoublesQuantilesSketch qs1 = null;
    HeapDoublesQuantilesSketch qs2 = (HeapDoublesQuantilesSketch)buildQS(256, 0);
    DoublesQuantilesSketch result = HeapDoublesUnion.updateLogic(qs1, qs2); //null, empty
    result = HeapDoublesUnion.updateLogic(qs2, qs1); //empty, null
    qs2.update(1); //no longer empty
    result = HeapDoublesUnion.updateLogic(qs2, qs1); //valid, null
    assertEquals(result.getMaxValue(), result.getMinValue(), 0.0);
  }
  
  @Test//(expectedExceptions = IllegalStateException.class)
  public void checkResultAndReset() {
    DoublesQuantilesSketch qs1 = buildQS(256, 0);
    DoublesUnion union = DoublesUnion.builder().build(qs1);
    DoublesQuantilesSketch qs2 = union.getResultAndReset();
    assertEquals(qs2.getK(), 256);
  }

  @Test
  public void updateWithDoubleValueOnly() {
    DoublesUnion union = DoublesUnion.builder().build(128);
    union.update(123.456);
    DoublesQuantilesSketch qs = union.getResultAndReset();
    assertEquals(qs.getN(), 1);
  }
  
  @Test
  public void checkEmptyUnion() {
    HeapDoublesUnion union = new HeapDoublesUnion(128);
    DoublesQuantilesSketch sk = union.getResult();
    assertNotNull(sk);
    String s = union.toString();
    assertNotNull(s);
  }
  
  @Test
  public void checkUnionNulls() {
    DoublesUnion union = new HeapDoublesUnion(128);
    DoublesQuantilesSketch sk1 = union.getResultAndReset();
    DoublesQuantilesSketch sk2 = union.getResultAndReset();
    assertNull(sk1);
    assertNull(sk2);
    union.update(sk2);
    DoublesQuantilesSketch sk3 = union.getResultAndReset();
    assertNull(sk3);
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
