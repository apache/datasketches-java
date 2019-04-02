/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 */
public class ForwardCompatibilityTest {


  @Test
  public void checkSerVer1_24Bytes() {
    byte[] byteArray = new byte[24];
    WritableMemory mem = WritableMemory.wrap(byteArray);
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

    Memory srcMem = Memory.wrap(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSerVer1_32Bytes_tooSmall() {
    byte[] byteArray = new byte[32];
    WritableMemory mem = WritableMemory.wrap(byteArray);
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

    Memory srcMem = Memory.wrap(byteArray);
    Sketch.heapify(srcMem);
  }


  @Test
  public void checkSerVer1_1Value() {
    byte[] byteArray = new byte[32];
    WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(0, (byte) 3);
    mem.putByte(1, (byte) 1); //SerVer
    //byte 2 Sketch Type, now Family
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

    Memory srcMem = Memory.wrap(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertEquals(sketch.getEstimate(), 1.0);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "SingleItemSketch");
  }

  @Test
  public void checkSerVer2_8Bytes() {
    byte[] byteArray = new byte[8];
    WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(0, (byte) 1); //mdLongs, RR = 0
    mem.putByte(1, (byte) 2); //SerVer
    mem.putByte(2, (byte) 3); //SketchType = SetSketch
    //byte 3 lgNomLongs not used w SetSketch
    //byte 4 lgArrLongs not used w SetSketch
    //byte 5 Flags: b0: BigEnd, b1: ReadOnly, b2: Empty, b3: NoRebuild, b4, Unordered
    mem.putByte(5, (byte) 2); //Flags
    short seedHash = Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED);
    mem.putShort(6, seedHash);
    //mem.putInt(8, 0); //curCount = 0
    //mem.putLong(16, Long.MAX_VALUE);

    Memory srcMem = Memory.wrap(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }

  @Test
  public void checkSerVer2_24Bytes_0Values() {
    byte[] byteArray = new byte[24];
    WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(0, (byte) 2); //mdLongs, RF (RR) = 0
    mem.putByte(1, (byte) 2); //SerVer
    mem.putByte(2, (byte) 3); //SketchType = SetSketch
    //byte 3 lgNomLongs not used w SetSketch
    //byte 4 lgArrLongs not used w SetSketch
    //byte 5 Flags: b0: BigEnd, b1: ReadOnly, b2: Empty, b3: NoRebuild, b4, Unordered
    mem.putByte(5, (byte) 2); //Flags = ReadOnly
    short seedHash = Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED);
    mem.putShort(6, seedHash);
    mem.putInt(8, 0); //curCount = 0
    //mem.putLong(16, Long.MAX_VALUE);

    Memory srcMem = Memory.wrap(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true); //was forced true
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }

  @Test
  public void checkSerVer2_32Bytes_0Values() {
    byte[] byteArray = new byte[32];
    WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(0, (byte) 3); //mdLongs, RF (RR) = 0
    mem.putByte(1, (byte) 2); //SerVer
    mem.putByte(2, (byte) 3); //SketchType = SetSketch
    //byte 3 lgNomLongs not used w SetSketch
    //byte 4 lgArrLongs not used w SetSketch
    //byte 5 Flags: b0: BigEnd, b1: ReadOnly, b2: Empty, b3: NoRebuild, b4, Unordered
    mem.putByte(5, (byte) 2); //Flags = ReadOnly
    short seedHash = Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED);
    mem.putShort(6, seedHash);
    mem.putInt(8, 0); //curCount = 0
    mem.putLong(16, Long.MAX_VALUE);

    Memory srcMem = Memory.wrap(byteArray);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true); //forced true
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    String name = sketch.getClass().getSimpleName();
    assertEquals(name, "HeapCompactOrderedSketch");
  }

  /**
   * Converts a SerVer3 CompactSketch to a SerVer1 SetSketch.
   * Note: SerVer1 does not support SingleItemSketch, so entries will equal zero.
   * @param v3mem a SerVer3 CompactSketch, ordered and with 24 byte preamble.
   * @return a SerVer1 SetSketch as Memory object.
   */
  public static WritableMemory convertSerV3toSerV1(Memory v3mem) {
    //validate that v3mem is in the right form
    int serVer = v3mem.getByte(SER_VER_BYTE);
    int famId = v3mem.getByte(FAMILY_BYTE);
    int flags = v3mem.getByte(FLAGS_BYTE);
    if ((serVer != 3) || (famId != 3) || ((flags & 24) != 24)) {
      throw new SketchesArgumentException("Memory must be V3, Compact, Ordered");
    }
    //must convert v3 preamble to a v1 preamble
    int v3preLongs = v3mem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    int entries;
    long thetaLong;
    if (v3preLongs == 1) {
      //Note: there is no way to safely convert to a SingleItemSketch if empty is false.
      entries = 0;
      thetaLong = Long.MAX_VALUE;
    }
    else if (v3preLongs == 2) {
      entries = v3mem.getInt(RETAINED_ENTRIES_INT);
      thetaLong = Long.MAX_VALUE;
    }
    else { //preLongs == 3
      entries = v3mem.getInt(RETAINED_ENTRIES_INT);
      thetaLong = v3mem.getLong(THETA_LONG);
    }
    //compute size
    int v1preLongs = 3;
    int v1bytes = (v1preLongs+entries) << 3;
    //create new mem and place the fields for SerVer1
    WritableMemory v1mem = WritableMemory.wrap(new byte[v1bytes]);
    v1mem.putByte(0, (byte)3); //Preamble = 3
    v1mem.putByte(1, (byte)1); //SerVer
    v1mem.putByte(2, (byte)3); //SketchType SetSketch = Family CompactSketch
    v1mem.putByte(6, (byte)2); //set bit1 = ReadOnly
    v1mem.putInt(RETAINED_ENTRIES_INT, entries);
    v1mem.putLong(THETA_LONG, thetaLong);
    //copy data
    v3mem.copyTo(v3preLongs << 3, v1mem, v1preLongs << 3, entries << 3);
    return v1mem;
  }
  /**
   * Converts a SerVer3 CompactSketch to a SerVer2 SetSketch.
   * @param v3mem a SerVer3 Compact, Ordered Sketch.
   * @return a SerVer2 SetSketch as Memory object
   */
  public static WritableMemory convertSerV3toSerV2(Memory v3mem) {
    //validate that v3mem is in the right form
    int serVer = v3mem.getByte(SER_VER_BYTE);
    int famId = v3mem.getByte(FAMILY_BYTE);
    int flags = v3mem.getByte(FLAGS_BYTE);
    if ((serVer != 3) || (famId != 3) || ((flags & 24) != 24)) {
      throw new SketchesArgumentException("Memory must be V3, Compact, Ordered");
    }
    //compute size
    int preLongs = v3mem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    //Note: there is no way to safely convert to a SingleItemSketch if empty is false.
    int entries = (preLongs == 1)? 0 : v3mem.getInt(RETAINED_ENTRIES_INT);
    int v2bytes = (preLongs+entries) << 3;
    //create new mem and do complete copy
    WritableMemory v2mem = WritableMemory.wrap(new byte[v2bytes]);
    v3mem.copyTo(0, v2mem, 0, v2bytes);
    //set serVer2
    v2mem.putByte(SER_VER_BYTE, (byte) 2);
    //adjust the flags
    byte v2flags = (byte)(2 | ((preLongs == 1)? 4: 0));
    v2mem.putByte(FLAGS_BYTE, v2flags);
    return v2mem;
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
