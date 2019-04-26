/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

import static com.yahoo.sketches.Util.bytesToInt;
import static com.yahoo.sketches.Util.bytesToLong;
import static com.yahoo.sketches.Util.bytesToString;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.ceilingPowerOfBdouble;
import static com.yahoo.sketches.Util.characterPad;
import static com.yahoo.sketches.Util.checkIfMultipleOf8AndGT0;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.Util.checkProbability;
import static com.yahoo.sketches.Util.evenlyLgSpaced;
import static com.yahoo.sketches.Util.floorPowerOf2;
import static com.yahoo.sketches.Util.floorPowerOfBdouble;
import static com.yahoo.sketches.Util.intToBytes;
import static com.yahoo.sketches.Util.isLessThanUnsigned;
import static com.yahoo.sketches.Util.isMultipleOf8AndGT0;
import static com.yahoo.sketches.Util.isPowerOf2;
import static com.yahoo.sketches.Util.milliSecToString;
import static com.yahoo.sketches.Util.nanoSecToString;
import static com.yahoo.sketches.Util.pwr2LawNext;
import static com.yahoo.sketches.Util.pwr2LawPrev;
import static com.yahoo.sketches.Util.pwrLawNextDouble;
import static com.yahoo.sketches.Util.simpleIntLog2;
import static com.yahoo.sketches.Util.zeroPad;
import static java.lang.Math.pow;
import static org.testng.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class UtilTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPowerOf2() {
    checkIfPowerOf2(31, "31");
  }

  @Test
  public void checkIsPowerOf2() {
    Assert.assertEquals(isPowerOf2(0), false);
    Assert.assertEquals(isPowerOf2(1), true);
    Assert.assertEquals(isPowerOf2(2), true);
    Assert.assertEquals(isPowerOf2(4), true);
    Assert.assertEquals(isPowerOf2(8), true);
    Assert.assertEquals(isPowerOf2(1 << 30), true);
    Assert.assertEquals(isPowerOf2(3), false);
    Assert.assertEquals(isPowerOf2(5), false);
    Assert.assertEquals(isPowerOf2( -1), false);
  }

  @Test
  public void checkCheckIfPowerOf2() {
    checkIfPowerOf2(8, "Test 8");
    try {
      checkIfPowerOf2(7, "Test 7");
      Assert.fail("Expected SketchesArgumentException");
    }
    catch (SketchesArgumentException e) {
      //pass
    }
  }

  @Test
  public void checkCeilingPowerOf2() {
    Assert.assertEquals(ceilingPowerOf2(Integer.MAX_VALUE), 1 << 30);
    Assert.assertEquals(ceilingPowerOf2(1 << 30), 1 << 30);
    Assert.assertEquals(ceilingPowerOf2(64), 64);
    Assert.assertEquals(ceilingPowerOf2(65), 128);
    Assert.assertEquals(ceilingPowerOf2(0), 1);
    Assert.assertEquals(ceilingPowerOf2( -1), 1);
  }

  @Test
  public void checkCeilingPowerOf2double() {
    Assert.assertEquals(ceilingPowerOfBdouble(2.0, Integer.MAX_VALUE), pow(2.0, 31));
    Assert.assertEquals(ceilingPowerOfBdouble(2.0, 1 << 30), pow(2.0, 30));
    Assert.assertEquals(ceilingPowerOfBdouble(2.0, 64.0), 64.0);
    Assert.assertEquals(ceilingPowerOfBdouble(2.0, 65.0), 128.0);
    Assert.assertEquals(ceilingPowerOfBdouble(2.0, 0.0), 1.0);
    Assert.assertEquals(ceilingPowerOfBdouble(2.0, -1.0), 1.0);
  }

  @Test
  public void checkFloorPowerOf2() {
    Assert.assertEquals(floorPowerOf2( -1), 1);
    Assert.assertEquals(floorPowerOf2(0), 1);
    Assert.assertEquals(floorPowerOf2(1), 1);
    Assert.assertEquals(floorPowerOf2(2), 2);
    Assert.assertEquals(floorPowerOf2(3), 2);
    Assert.assertEquals(floorPowerOf2(4), 4);

    Assert.assertEquals(floorPowerOf2((1 << 30) - 1), (1 << 29));
    Assert.assertEquals(floorPowerOf2((1 << 30)), (1 << 30));
    Assert.assertEquals(floorPowerOf2((1 << 30) + 1), (1 << 30));
  }

  @Test
  public void checkFloorPowerOf2double() {
    Assert.assertEquals(floorPowerOfBdouble(2.0, -1.0), 1.0);
    Assert.assertEquals(floorPowerOfBdouble(2.0, 0.0), 1.0);
    Assert.assertEquals(floorPowerOfBdouble(2.0, 1.0), 1.0);
    Assert.assertEquals(floorPowerOfBdouble(2.0, 2.0), 2.0);
    Assert.assertEquals(floorPowerOfBdouble(2.0, 3.0), 2.0);
    Assert.assertEquals(floorPowerOfBdouble(2.0, 4.0), 4.0);

    Assert.assertEquals(floorPowerOfBdouble(2.0, (1 << 30) - 1), (double)(1 << 29));
    Assert.assertEquals(floorPowerOfBdouble(2.0, 1 << 30), (double)(1 << 30));
    Assert.assertEquals(floorPowerOfBdouble(2.0, (1 << 30) + 1.0), (double)(1L << 30));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIfMultipleOf8AndGTzero() {
    checkIfMultipleOf8AndGT0(8, "test");
    checkIfMultipleOf8AndGT0(7, "test");
  }

  @Test
  public void checkIsMultipleOf8AndGT0() {
    Assert.assertTrue(isMultipleOf8AndGT0(8));
    Assert.assertFalse(isMultipleOf8AndGT0(7));
    Assert.assertFalse(isMultipleOf8AndGT0(-1));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIfMultipleOf8AndGTzero2() {
    checkIfMultipleOf8AndGT0(8, "test");
    checkIfMultipleOf8AndGT0(-1, "test");
  }

  @Test
  public void checkIsLessThanUnsigned() {
    long n1 = 1;
    long n2 = 3;
    long n3 = -3;
    long n4 = -1;
    Assert.assertTrue(isLessThanUnsigned(n1, n2));
    Assert.assertTrue(isLessThanUnsigned(n2, n3));
    Assert.assertTrue(isLessThanUnsigned(n3, n4));
    Assert.assertFalse(isLessThanUnsigned(n2, n1));
    Assert.assertFalse(isLessThanUnsigned(n3, n2));
    Assert.assertFalse(isLessThanUnsigned(n4, n3));
  }

  @Test
  public void checkZeroPad() {
    long v = 123456789;
    String vHex = Long.toHexString(v);
    String out = zeroPad(vHex, 16);
    println(out);
  }

  @Test
  public void checkCharacterPad() {
    String s = "Sleeping ... ";
    String out = characterPad(s, 20, 'z', true);
    println(out);
  }

  @Test
  public void checkProbabilityFn1() {
    checkProbability(.5, "Good");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkProbabilityFn2() {
    checkProbability(-.5, "Too Low");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkProbabilityFn3() {
    checkProbability(1.5, "Too High");
  }

  @Test
  public void checkEvenlyLgSpaced() {
    int lgStart = 0;
    int lgEnd = 4;
    int ppo = 1;
    int points = (ppo * (lgEnd - lgStart)) +1;
    int[] pts = evenlyLgSpaced(lgStart, lgEnd, points);
    Assert.assertEquals(pts[0], 1);
    Assert.assertEquals(pts[1], 2);
    Assert.assertEquals(pts[2], 4);
    Assert.assertEquals(pts[3], 8);
    Assert.assertEquals(pts[4], 16);
    pts = evenlyLgSpaced(lgStart, lgEnd, 1);
    Assert.assertEquals(pts[0], 1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkEvenlyLgSpacedExcep1() {
    evenlyLgSpaced(1, 2, -1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkEvenlyLgSpacedExcep2() {
    evenlyLgSpaced(-1, 2, 3);
  }

  @Test
  public void checkBytesToInt() {
    byte[] arr = new byte[] {4, 3, 2, 1};
    int result = 4 + (3 << 8) + (2 << 16) + (1 << 24);
    Assert.assertEquals(bytesToInt(arr), result);
    byte[] arr2 = intToBytes(result, new byte[4]);
    Assert.assertEquals(arr, arr2);
  }

  @Test
  public void checkBytesToLong() {
    byte[] arr = new byte[] {8, 7, 6, 5, 4, 3, 2, 1};
    long result = 8L + (7L << 8) + (6L << 16) + (5L << 24)
               + (4L << 32) + (3L << 40) + (2L << 48) + (1L << 56);
    Assert.assertEquals(bytesToLong(arr), result);
  }

  @Test
  public void checkBytesToString() {
    long lng = 0XF8F7F6F504030201L;
    //println(Long.toHexString(lng));
    byte[] bytes = new byte[8];
    bytes = Util.longToBytes(lng, bytes);
    String sep = ".";
    String unsignLE = bytesToString(bytes, false, true, sep);
    String signedLE = bytesToString(bytes, true, true, sep);
    String unsignBE = bytesToString(bytes, false,  false, sep);
    String signedBE = bytesToString(bytes, true,  false, sep);
    Assert.assertEquals(unsignLE, "1.2.3.4.245.246.247.248");
    Assert.assertEquals(signedLE, "1.2.3.4.-11.-10.-9.-8");
    Assert.assertEquals(unsignBE, "248.247.246.245.4.3.2.1");
    Assert.assertEquals(signedBE, "-8.-9.-10.-11.4.3.2.1");
  }

  @Test
  public void checkNsecToString() {
    long nS = 1000000000L + 1000000L + 1000L + 1L;
    String result = nanoSecToString(nS);
    String expected = "1.001_001_001";
    Assert.assertEquals(result, expected);
  }

  @Test
  public void checkMsecToString() {
    long nS = (60L * 60L * 1000L) + (60L * 1000L) + 1000L + 1L;
    String result = milliSecToString(nS);
    String expected = "1:01:01.001";
    Assert.assertEquals(result, expected);
  }

  @Test
  public void checkPwr2LawNext() {
    int next = pwr2LawNext(2, 1);
    Assert.assertEquals(next, 2);
    next = pwr2LawNext(2, 0);
    Assert.assertEquals(next, 1);
  }

  @Test
  public void checkPwr2LawNextDouble() {
    double next = pwrLawNextDouble(2, 1.0, true, 2.0);
    Assert.assertEquals(next, 2.0, 0.0);
    next = pwrLawNextDouble(2, 1.0, false, 2.0);
    Assert.assertEquals(next, Math.sqrt(2), 0.0);
    next = pwrLawNextDouble(2, 0.5, true, 2.0);
    Assert.assertEquals(next, 2.0, 0.0);
    next = pwrLawNextDouble(2, 0.5, false, 2.0);
    Assert.assertEquals(next, Math.sqrt(2), 0.0);
    next = pwrLawNextDouble(2, next, false, 2.0);
    Assert.assertEquals(next, 2.0, 0.0);

  }

  @Test
  public void checkPwr2LawExamples() {
    int maxP = 32;
    int minP = 1;
    int ppo = 4;

    for (int p = minP; p <= maxP; p = pwr2LawNext(ppo, p)) {
      print(p + " ");
    }
    println("");

    for (int p = maxP; p >= minP; p = pwr2LawPrev(ppo, p)) {
      print(p + " ");
    }
    println("");
  }

  @Test
  public void checkSimpleIntLog2() {
    Assert.assertEquals(simpleIntLog2(2), 1);
    Assert.assertEquals(simpleIntLog2(1), 0);
    try {
      simpleIntLog2(0);
      fail();
    } catch (SketchesArgumentException e) {}
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
    print("  Long MAX & MIN: "); print(Long.MAX_VALUE); print(", "); println(Long.MIN_VALUE);
    print("  Doubles:        "); print(1.2345); print(", "); println(5.4321);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s + '\t');
  }

  /**
   * @param d value to print
   */
  static void println(double d) {
    print(Double.toString(d) + '\t');
  }

  /**
   * @param d value to print
   */
  static void print(double d) {
    print(Double.toString(d));
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    //System.out.print(s); //disable here
  }

}
