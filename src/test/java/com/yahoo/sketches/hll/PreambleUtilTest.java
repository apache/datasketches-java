/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.hll.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.hll.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.hll.PreambleUtil.extractFlags;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void preambleToString() { //TODO Check Visually
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(8, TgtHllType.HLL_4);
    byte[] byteArr1 = new byte[bytes];
    WritableMemory wmem1 = WritableMemory.wrap(byteArr1);
    HllSketch sk = new HllSketch(8, TgtHllType.HLL_4, wmem1);
    byte[] byteArr2 = sk.toCompactByteArray();
    WritableMemory wmem2 = WritableMemory.wrap(byteArr2);

    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertTrue(sk.isEmpty());
    String s = HllSketch.toString(byteArr2); //empty sketch output
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());

    for (int i = 0; i < 7; i++) { sk.update(i); }
    byteArr2 = sk.toCompactByteArray();
    wmem2 = WritableMemory.wrap(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertFalse(sk.isEmpty());
    s = HllSketch.toString(byteArr2);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());

    for (int i = 7; i < 24; i++) { sk.update(i); }
    byteArr2 = sk.toCompactByteArray();
    wmem2 = WritableMemory.wrap(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.SET);
    s = HllSketch.toString(byteArr2);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());

    sk.update(24);
    byteArr2 = sk.toCompactByteArray();
    wmem2 = WritableMemory.wrap(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.HLL);
    s = HllSketch.toString(Memory.wrap(byteArr2));
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());
  }

  @Test
  public void checkCompactFlag() {
    HllSketch sk = new HllSketch(7);
    byte[] memObj = sk.toCompactByteArray();
    WritableMemory wmem = WritableMemory.wrap(memObj);
    boolean compact = PreambleUtil.extractCompactFlag(wmem);
    assertTrue(compact);

    PreambleUtil.insertCompactFlag(wmem, false);
    compact = PreambleUtil.extractCompactFlag(wmem);
    assertFalse(compact);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkCorruptMemoryInput() {
    HllSketch sk = new HllSketch(12);
    byte[] memObj = sk.toCompactByteArray();
    WritableMemory wmem = WritableMemory.wrap(memObj);
    long memAdd = wmem.getCumulativeOffset(0);
    HllSketch bad;

    //checkFamily
    try {
      wmem.putByte(FAMILY_BYTE, (byte) 0); //corrupt, should be 7
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertFamilyId(wmem); //corrected

    //check SerVer
    try {
      wmem.putByte(SER_VER_BYTE, (byte) 0); //corrupt, should be 1
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertSerVer(wmem); //corrected

    //check bad PreInts
    try {
      insertPreInts(wmem, 0); //corrupt, should be 2
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 2); //corrected

    //check wrong PreInts and LIST
    try {
      insertPreInts(wmem, 3); //corrupt, should be 2
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 2); //corrected

    //move to Set mode
    for (int i = 1; i <= 15; i++) { sk.update(i); }
    memObj = sk.toCompactByteArray();
    wmem = WritableMemory.wrap(memObj);
    memAdd = wmem.getCumulativeOffset(0);

    //check wrong PreInts and SET
    try {
      insertPreInts(wmem, 2); //corrupt, should be 3
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 3); //corrected

    //move to HLL mode
    for (int i = 15; i <= 1000; i++) { sk.update(i); }
    memObj = sk.toCompactByteArray();
    wmem = WritableMemory.wrap(memObj);
    memAdd = wmem.getCumulativeOffset(0);

    //check wrong PreInts and HLL
    try {
      insertPreInts(wmem, 2); //corrupt, should be 10
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 10); //corrected
  }

  @SuppressWarnings("unused")
  @Test
  public void checkExtractFlags() {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    Object memObj = wmem.getArray();
    long memAdd = wmem.getCumulativeOffset(0L);
    HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wmem);
    int flags = extractFlags(wmem);
    assertEquals(flags, EMPTY_FLAG_MASK);
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
