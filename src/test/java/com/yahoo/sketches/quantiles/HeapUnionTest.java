/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapQuantilesSketchTest.buildQS;
import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

public class HeapUnionTest {

  @Test
  public void checkUnion1() {
    QuantilesSketch result;
    QuantilesSketch qs1 = null;
    Union union = Union.builder().build(); //virgin
    
    qs1 = buildQS(256, 1000); //first 1000
    union.update(qs1); //copy   me = null,  that = valid, OK
    
    //check copy   me = null,  that = valid
    result = union.getResult();
    assertEquals(result.getN(), 1000);
    assertEquals(result.getK(), 256);
    
    //check merge  me = valid, that = valid, both K's the same
    QuantilesSketch qs2 = buildQS(256, 1000, 1000, 0); //add 1000
    union.update(qs2); 
    result = union.getResult();
    assertEquals(result.getN(), 2000);
    assertEquals(result.getK(), 256);
  }
  
  @Test
  public void checkUnion2() {
    QuantilesSketch qs1 = buildQS(256, 1000);
    QuantilesSketch qs2 = buildQS(128, 1000);
    Union union = Union.builder().build(); //virgin
    
    union.update(qs1);
    QuantilesSketch res1 = union.getResult();
    //println(res1.toString());
    assertEquals(res1.getN(), 1000);
    assertEquals(res1.getK(), 256);
    
    union.update(qs2);
    QuantilesSketch res2 = union.getResult();
    assertEquals(res2.getN(), 2000);
    assertEquals(res2.getK(), 128);
    //println(union.toString());
  }
  
  @Test
  public void checkUpdateMemory() {
    QuantilesSketch qs1 = buildQS(256, 1000);
    int bytes = qs1.getStorageBytes();
    Memory dstMem = new NativeMemory(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;
    
    Union union = Union.builder().build(); //virgin
    union.update(srcMem);
    for (int i=1000; i<2000; i++) union.update(i);
    QuantilesSketch qs2 = union.getResult();
    assertEquals(qs2.getMaxValue(), 1999, 0.0);
    String s = union.toString();
    println(s); //enable printing to see
    union.reset(); //sets to null
  }
  
  @Test
  public void checkUnionUpdateLogic() {
    HeapQuantilesSketch qs1 = null;
    HeapQuantilesSketch qs2 = (HeapQuantilesSketch)buildQS(256, 0);
    QuantilesSketch result = HeapUnion.updateLogic(qs1, qs2); //null, empty
    result = HeapUnion.updateLogic(qs2, qs1); //empty, null
    qs2.update(1); //no longer empty
    result = HeapUnion.updateLogic(qs2, qs1); //valid, null
    assertEquals(result.getMaxValue(), result.getMinValue(), 0.0);
  }
  
  @Test//(expectedExceptions = IllegalStateException.class)
  public void checkResultAndReset() {
    QuantilesSketch qs1 = buildQS(256, 0);
    Union union = Union.builder().build(qs1);
    QuantilesSketch qs2 = union.getResultAndReset();
    assertEquals(qs2.getK(), 256);
  }

  @Test
  public void updateWithDoubleValueOnly() {
    Union union = Union.builder().setK(128).setSeed((short) 1).build();
    union.update(123.456);
    QuantilesSketch qs = union.getResultAndReset();
    assertEquals(qs.getN(), 1);
  }
  
  @Test
  public void checkEmptyUnion() {
    HeapUnion union = new HeapUnion();
    QuantilesSketch sk = union.getResult();
    assertNotNull(sk);
    String s = union.toString();
    assertNotNull(s);
    
    
  }
  
  @Test
  public void checkUnionNulls() {
    Union union = new HeapUnion();
    QuantilesSketch sk1 = union.getResultAndReset();
    QuantilesSketch sk2 = union.getResultAndReset();
    assertNull(sk1);
    assertNull(sk2);
    union.update(sk2);
    QuantilesSketch sk3 = union.getResultAndReset();
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
