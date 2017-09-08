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

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void preambleToString() {
    HllSketch sk = new HllSketch(8);
    byte[] byteArr = sk.toCompactByteArray();
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertTrue(sk.isEmpty());
    String s = PreambleUtil.toString(byteArr); //empty
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmptyFlag(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    for (int i = 0; i < 7; i++) { sk.update(i); }
    byteArr = sk.toCompactByteArray();
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertFalse(sk.isEmpty());
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmptyFlag(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    for (int i = 7; i < 24; i++) { sk.update(i); }
    byteArr = sk.toCompactByteArray();
    assertEquals(sk.getCurMode(), CurMode.SET);
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmptyFlag(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    sk.update(24);
    byteArr = sk.toCompactByteArray();
    assertEquals(sk.getCurMode(), CurMode.HLL);
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmptyFlag(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);
  }

  @Test
  public void checkCompactFlag() {
    HllSketch sk = new HllSketch(7);
    byte[] memObj = sk.toCompactByteArray();
    WritableMemory wmem = WritableMemory.wrap(memObj);
    long memAdd = wmem.getCumulativeOffset(0);
    boolean compact = PreambleUtil.extractCompactFlag(memObj, memAdd);
    assertTrue(compact);

    PreambleUtil.insertCompactFlag(memObj, memAdd, false);
    compact = PreambleUtil.extractCompactFlag(memObj, memAdd);
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
    insertFamilyId(memObj, memAdd); //corrected

    //check SerVer
    try {
      wmem.putByte(SER_VER_BYTE, (byte) 0); //corrupt, should be 1
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertSerVer(memObj, memAdd); //corrected

    //check bad PreInts
    try {
      insertPreInts(memObj, memAdd, 0); //corrupt, should be 2
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(memObj, memAdd, 2); //corrected

    //check wrong PreInts and LIST
    try {
      insertPreInts(memObj, memAdd, 3); //corrupt, should be 2
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(memObj, memAdd, 2); //corrected

    //move to Set mode
    for (int i = 1; i <= 15; i++) { sk.update(i); }
    memObj = sk.toCompactByteArray();
    wmem = WritableMemory.wrap(memObj);
    memAdd = wmem.getCumulativeOffset(0);

    //check wrong PreInts and SET
    try {
      insertPreInts(memObj, memAdd, 2); //corrupt, should be 3
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(memObj, memAdd, 3); //corrected

    //move to HLL mode
    for (int i = 15; i <= 1000; i++) { sk.update(i); }
    memObj = sk.toCompactByteArray();
    wmem = WritableMemory.wrap(memObj);
    memAdd = wmem.getCumulativeOffset(0);

    //check wrong PreInts and HLL
    try {
      insertPreInts(memObj, memAdd, 2); //corrupt, should be 10
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(memObj, memAdd, 10); //corrected
  }

  @SuppressWarnings("unused")
  @Test
  public void checkExtractFlags() {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    Object memObj = wmem.getArray();
    long memAdd = wmem.getCumulativeOffset(0L);
    HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wmem);
    int flags = extractFlags(memObj, memAdd);
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
