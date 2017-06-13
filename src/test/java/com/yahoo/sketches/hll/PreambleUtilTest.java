/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void preambleToString() {
    HllSketch sk = new HllSketch(7);
    byte[] byteArr = sk.toCompactByteArray();
    String s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmptyFlag(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    for (int i = 0; i < 7; i++) { sk.update(i); }
    byteArr = sk.toCompactByteArray();
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmptyFlag(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    for (int i = 7; i < 12; i++) { sk.update(i); }
    byteArr = sk.toCompactByteArray();
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmptyFlag(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    sk.update(12);
    byteArr = sk.toCompactByteArray();
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
