/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void preambleToString() {
    HllSketch sk = new HllSketch(7);
    byte[] byteArr = sk.toByteArray();
    String s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmpty(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    for (int i = 0; i < 7; i++) { sk.update(i); }
    byteArr = sk.toByteArray();
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmpty(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    for (int i = 7; i < 12; i++) { sk.update(i); }
    byteArr = sk.toByteArray();
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmpty(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);

    sk.update(12);
    byteArr = sk.toByteArray();
    s = PreambleUtil.toString(byteArr);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(byteArr, 16));
    println("Empty: " + PreambleUtil.extractEmpty(byteArr, 16));
    println("Serialization Bytes: " + byteArr.length);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
