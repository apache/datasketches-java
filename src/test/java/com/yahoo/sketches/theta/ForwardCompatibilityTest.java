/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.PreambleUtil;
import com.yahoo.sketches.theta.Sketch;

/**
 * @author Lee Rhodes
 */
public class ForwardCompatibilityTest {
  
  
  @Test
  public void checkSerVer1_24Bytes() {
    byte[] byteArray = new byte[24];
    Memory mem = new NativeMemory(byteArray);
    mem.putByte(0, (byte) 3); //mdLongs
    mem.putByte(1, (byte) 1); //SerVer
    mem.putByte(2, (byte) 3); //SketchType = SetSketch
    //byte 3 lgNomLongs not used with SetSketch
    //byte 4 lgArrLongs not used with SetSketch
    //byte 5 lgRR not used with SetSketch
    //byte 6: Flags: b0: BigEnd, b1: ReadOnly
    mem.putByte(6, (byte) 2);
    //byte 7 Not used
    mem.putInt(8, 0); //curCount = 0
    mem.putLong(16, Long.MAX_VALUE);
    
    Memory srcMem = new NativeMemory(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSerVer1_32Bytes_tooSmall() {
    byte[] byteArray = new byte[32];
    Memory mem = new NativeMemory(byteArray);
    mem.putByte(0, (byte) 3); //mdLongs
    mem.putByte(1, (byte) 1); //SerVer
    mem.putByte(2, (byte) 3); //SketchType = SetSketch
    //byte 3 lgNomLongs not used with SetSketch
    //byte 4 lgArrLongs not used with SetSketch
    //byte 5 lgRR not used with SetSketch
    //byte 6: Flags: b0: BigEnd, b1: ReadOnly
    mem.putByte(6, (byte) 2);
    //byte 7 Not used
    mem.putInt(8, 2); //curCount = 2
    mem.putLong(16, Long.MAX_VALUE);
    
    Memory srcMem = new NativeMemory(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }
  
  
  @Test
  public void checkSerVer1_1Value() {
    byte[] byteArray = new byte[32];
    Memory mem = new NativeMemory(byteArray);
    mem.putByte(0, (byte) 3);
    mem.putByte(1, (byte) 1); //SerVer
    //byte 3 lgNomLongs not used with SetSketch
    //byte 4 lgArrLongs not used with SetSketch
    //byte 5 lgRR not used with SetSketch
    //byte 6: Flags: b0: BigEnd, b1: ReadOnly
    mem.putByte(6, (byte) 2);
    //byte 7 Not used
    mem.putInt(8, 1); //curCount = 1
    mem.putLong(16, Long.MAX_VALUE);
    long[] longArrIn = new long[2];
    longArrIn[0] = 1;
    long hash = hash(longArrIn, 0)[0] >>> 1;
    mem.putLong(24, hash);
    
    Memory srcMem = new NativeMemory(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertEquals(sketch.getEstimate(), 1.0);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }
  
  @Test
  public void checkSerVer2_8Bytes() {
    byte[] byteArray = new byte[8];
    Memory mem = new NativeMemory(byteArray);
    mem.putByte(0, (byte) 1); //mdLongs, RR = 0
    mem.putByte(1, (byte) 2); //SerVer
    mem.putByte(2, (byte) 3); //SketchType = SetSketch
    //byte 3 lgNomLongs not used w SetSketch
    //byte 4 lgArrLongs not used w SetSketch
    //byte 5 Flags: b0: BigEnd, b1: ReadOnly, b2: Empty, b3: NoRebuild, b4, Unordered
    mem.putByte(5, (byte) 2); //Flags
    short seedHash = PreambleUtil.computeSeedHash(Util.DEFAULT_UPDATE_SEED);
    mem.putShort(6, seedHash);
    //mem.putInt(8, 0); //curCount = 0
    //mem.putLong(16, Long.MAX_VALUE);
    
    Memory srcMem = new NativeMemory(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }
  
  @Test
  public void checkSerVer2_24Bytes_1Value() {
    byte[] byteArray = new byte[24];
    Memory mem = new NativeMemory(byteArray);
    mem.putByte(0, (byte) 2); //mdLongs, RR = 0
    mem.putByte(1, (byte) 2); //SerVer
    mem.putByte(2, (byte) 3); //SketchType = SetSketch
    //byte 3 lgNomLongs not used w SetSketch
    //byte 4 lgArrLongs not used w SetSketch
    //byte 5 Flags: b0: BigEnd, b1: ReadOnly, b2: Empty, b3: NoRebuild, b4, Unordered
    mem.putByte(5, (byte) 2); //Flags
    short seedHash = PreambleUtil.computeSeedHash(Util.DEFAULT_UPDATE_SEED);
    mem.putShort(6, seedHash);
    mem.putInt(8, 0); //curCount = 0
    //mem.putLong(16, Long.MAX_VALUE);
    
    Memory srcMem = new NativeMemory(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }
  
  
  
  
  @Test
  public void printlnTest() {
    println("Test");
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}