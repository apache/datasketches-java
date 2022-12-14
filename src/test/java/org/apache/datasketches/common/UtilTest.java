/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.common;

import static java.lang.Math.pow;
import static org.apache.datasketches.common.Util.bytesToInt;
import static org.apache.datasketches.common.Util.bytesToLong;
import static org.apache.datasketches.common.Util.bytesToString;
import static org.apache.datasketches.common.Util.ceilingIntPowerOf2;
import static org.apache.datasketches.common.Util.ceilingLongPowerOf2;
import static org.apache.datasketches.common.Util.ceilingPowerBaseOfDouble;
import static org.apache.datasketches.common.Util.characterPad;
import static org.apache.datasketches.common.Util.checkBounds;
import static org.apache.datasketches.common.Util.checkIfIntPowerOf2;
import static org.apache.datasketches.common.Util.checkIfLongPowerOf2;
import static org.apache.datasketches.common.Util.checkIfMultipleOf8AndGT0;
import static org.apache.datasketches.common.Util.checkProbability;
import static org.apache.datasketches.common.Util.convertToLongArray;
import static org.apache.datasketches.common.Util.exactLog2OfInt;
import static org.apache.datasketches.common.Util.exactLog2OfLong;
import static org.apache.datasketches.common.Util.floorPowerBaseOfDouble;
import static org.apache.datasketches.common.Util.floorPowerOf2;
import static org.apache.datasketches.common.Util.getResourceBytes;
import static org.apache.datasketches.common.Util.getResourceFile;
import static org.apache.datasketches.common.Util.intToBytes;
import static org.apache.datasketches.common.Util.invPow2;
import static org.apache.datasketches.common.Util.isEven;
import static org.apache.datasketches.common.Util.isIntPowerOf2;
import static org.apache.datasketches.common.Util.isLessThanUnsigned;
import static org.apache.datasketches.common.Util.isLongPowerOf2;
import static org.apache.datasketches.common.Util.isMultipleOf8AndGT0;
import static org.apache.datasketches.common.Util.isOdd;
import static org.apache.datasketches.common.Util.longToBytes;
import static org.apache.datasketches.common.Util.milliSecToString;
import static org.apache.datasketches.common.Util.nanoSecToString;
import static org.apache.datasketches.common.Util.numberOfLeadingOnes;
import static org.apache.datasketches.common.Util.numberOfTrailingOnes;
import static org.apache.datasketches.common.Util.powerSeriesNextDouble;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;
import static org.apache.datasketches.common.Util.pwr2SeriesPrev;
import static org.apache.datasketches.common.Util.zeroPad;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class UtilTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  public void numTrailingOnes() {
    long mask = 1L;
    for (int i = 0; i <= 64; i++) {
      final long v = ~mask & -1L;
      mask <<= 1;
      final int numT1s = numberOfTrailingOnes(v);
      final int numL1s = numberOfLeadingOnes(v);
      assertEquals(Long.numberOfTrailingZeros(~v), numT1s);
      assertEquals(Long.numberOfLeadingZeros(~v), numL1s);
      //println(zeroPad(Long.toBinaryString(v),64) + ", " + numL1s + ", " + numT1s);
      continue;
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBoundsTest() {
    checkBounds(999L, 2L, 1000L);
  }

  @Test
  public void checkIsIntPowerOf2() {
    Assert.assertEquals(isIntPowerOf2(0), false);
    Assert.assertEquals(isIntPowerOf2(1), true);
    Assert.assertEquals(isIntPowerOf2(2), true);
    Assert.assertEquals(isIntPowerOf2(4), true);
    Assert.assertEquals(isIntPowerOf2(8), true);
    Assert.assertEquals(isIntPowerOf2(1 << 30), true);
    Assert.assertEquals(isIntPowerOf2(3), false);
    Assert.assertEquals(isIntPowerOf2(5), false);
    Assert.assertEquals(isIntPowerOf2( -1), false);
  }

  @Test
  public void checkIsLongPowerOf2() {
    Assert.assertEquals(isLongPowerOf2(0), false);
    Assert.assertEquals(isLongPowerOf2(1), true);
    Assert.assertEquals(isLongPowerOf2(2), true);
    Assert.assertEquals(isLongPowerOf2(4), true);
    Assert.assertEquals(isLongPowerOf2(8), true);
    Assert.assertEquals(isLongPowerOf2(1L << 62), true);
    Assert.assertEquals(isLongPowerOf2(3), false);
    Assert.assertEquals(isLongPowerOf2(5), false);
    Assert.assertEquals(isLongPowerOf2( -1), false);
  }

  @Test
  public void checkCheckIfIntPowerOf2() {
    checkIfIntPowerOf2(8, "Test 8");
    try {
      checkIfIntPowerOf2(7, "Test 7");
      Assert.fail("Expected SketchesArgumentException");
    }
    catch (final SketchesArgumentException e) {
      //pass
    }
  }

  @Test
  public void checkCheckIfLongPowerOf2() {
    checkIfLongPowerOf2(8L, "Test 8");
    try {
      checkIfLongPowerOf2(7L, "Test 7");
      Assert.fail("Expected SketchesArgumentException");
    }
    catch (final SketchesArgumentException e) {
      //pass
    }
  }

  @Test
  public void checkCeilingIntPowerOf2() {
    Assert.assertEquals(ceilingIntPowerOf2(Integer.MAX_VALUE), 1 << 30);
    Assert.assertEquals(ceilingIntPowerOf2(1 << 30), 1 << 30);
    Assert.assertEquals(ceilingIntPowerOf2(64), 64);
    Assert.assertEquals(ceilingIntPowerOf2(65), 128);
    Assert.assertEquals(ceilingIntPowerOf2(0), 1);
    Assert.assertEquals(ceilingIntPowerOf2( -1), 1);
  }

  @Test
  public void checkCeilingLongPowerOf2() {
    Assert.assertEquals(ceilingLongPowerOf2(Long.MAX_VALUE), 1L << 62);
    Assert.assertEquals(ceilingLongPowerOf2(1L << 62), 1L << 62);
    Assert.assertEquals(ceilingLongPowerOf2(64), 64);
    Assert.assertEquals(ceilingLongPowerOf2(65), 128);
    Assert.assertEquals(ceilingLongPowerOf2(0), 1L);
    Assert.assertEquals(ceilingLongPowerOf2( -1L), 1L);
  }


  @Test
  public void checkCeilingPowerOf2double() {
    Assert.assertEquals(ceilingPowerBaseOfDouble(2.0, Integer.MAX_VALUE), pow(2.0, 31));
    Assert.assertEquals(ceilingPowerBaseOfDouble(2.0, 1 << 30), pow(2.0, 30));
    Assert.assertEquals(ceilingPowerBaseOfDouble(2.0, 64.0), 64.0);
    Assert.assertEquals(ceilingPowerBaseOfDouble(2.0, 65.0), 128.0);
    Assert.assertEquals(ceilingPowerBaseOfDouble(2.0, 0.0), 1.0);
    Assert.assertEquals(ceilingPowerBaseOfDouble(2.0, -1.0), 1.0);
  }

  @Test
  public void checkFloorPowerOf2Int() {
    Assert.assertEquals(floorPowerOf2( -1), 1);
    Assert.assertEquals(floorPowerOf2(0), 1);
    Assert.assertEquals(floorPowerOf2(1), 1);
    Assert.assertEquals(floorPowerOf2(2), 2);
    Assert.assertEquals(floorPowerOf2(3), 2);
    Assert.assertEquals(floorPowerOf2(4), 4);

    Assert.assertEquals(floorPowerOf2((1 << 30) - 1), 1 << 29);
    Assert.assertEquals(floorPowerOf2(1 << 30), 1 << 30);
    Assert.assertEquals(floorPowerOf2((1 << 30) + 1), 1 << 30);
  }
  @Test
  public void checkFloorPowerOf2Long() {
    Assert.assertEquals(floorPowerOf2( -1L), 1L);
    Assert.assertEquals(floorPowerOf2(0L), 1L);
    Assert.assertEquals(floorPowerOf2(1L), 1L);
    Assert.assertEquals(floorPowerOf2(2L), 2L);
    Assert.assertEquals(floorPowerOf2(3L), 2L);
    Assert.assertEquals(floorPowerOf2(4L), 4L);

    Assert.assertEquals(floorPowerOf2((1L << 63) - 1L), 1L << 62);
    Assert.assertEquals(floorPowerOf2(1L << 62), 1L << 62);
    Assert.assertEquals(floorPowerOf2((1L << 62) + 1L), 1L << 62);
  }
  @Test
  public void checkFloorPowerOf2double() {
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, -1.0), 1.0);
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, 0.0), 1.0);
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, 1.0), 1.0);
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, 2.0), 2.0);
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, 3.0), 2.0);
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, 4.0), 4.0);

    Assert.assertEquals(floorPowerBaseOfDouble(2.0, (1 << 30) - 1.0), 1 << 29);
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, 1 << 30), 1 << 30);
    Assert.assertEquals(floorPowerBaseOfDouble(2.0, (1 << 30) + 1.0), 1L << 30);
  }

  @Test
  public void checkCheckIfMultipleOf8AndGT0() {
    checkIfMultipleOf8AndGT0(8, "test 8");
    try { checkIfMultipleOf8AndGT0( 7, "test 7"); fail(); } catch (final SketchesArgumentException e) { }
    try { checkIfMultipleOf8AndGT0(-8, "test -8"); fail(); } catch (final SketchesArgumentException e) { }
    try { checkIfMultipleOf8AndGT0(-1, "test -1"); fail(); } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkIsMultipleOf8AndGT0() {
    Assert.assertTrue(isMultipleOf8AndGT0(8));
    Assert.assertFalse(isMultipleOf8AndGT0(7));
    Assert.assertFalse(isMultipleOf8AndGT0(-8));
    Assert.assertFalse(isMultipleOf8AndGT0(-1));
  }

  @Test
  public void checkInvPow2() {
    Assert.assertEquals(invPow2(1), 0.5);
    Assert.assertEquals(invPow2(0), 1.0);
    try { invPow2(-1); failIAE(); } catch (final AssertionError e) {}
    try {invPow2(1024); failIAE(); } catch (final AssertionError e) {}
    try {invPow2(Integer.MIN_VALUE); failIAE(); } catch (final AssertionError e) {}
  }

  private static void failIAE() { throw new IllegalArgumentException("Test should have failed!"); }

  @Test
  public void checkIsLessThanUnsigned() {
    final long n1 = 1;
    final long n2 = 3;
    final long n3 = -3;
    final long n4 = -1;
    Assert.assertTrue(isLessThanUnsigned(n1, n2));
    Assert.assertTrue(isLessThanUnsigned(n2, n3));
    Assert.assertTrue(isLessThanUnsigned(n3, n4));
    Assert.assertFalse(isLessThanUnsigned(n2, n1));
    Assert.assertFalse(isLessThanUnsigned(n3, n2));
    Assert.assertFalse(isLessThanUnsigned(n4, n3));
  }

  @Test
  public void checkZeroPad() {
    final long v = 123456789;
    final String vHex = Long.toHexString(v);
    final String out = zeroPad(vHex, 16);
    println("Pad 16, prepend 0: " + out);
  }

  @Test
  public void checkCharacterPad() {
    final String s = "Pad 30, postpend z:";
    final String out = characterPad(s, 30, 'z', true);
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
  public void checkEvenOdd() {
    assertTrue(isEven(0));
    assertFalse(isOdd(0));
    assertTrue(isOdd(-1));
    assertFalse(isEven(-1));
  }

  @Test
  public void checkBytesToInt() {
    final byte[] arr = new byte[] {4, 3, 2, 1};
    final int result = 4 + (3 << 8) + (2 << 16) + (1 << 24);
    Assert.assertEquals(bytesToInt(arr), result);
    final byte[] arr2 = intToBytes(result, new byte[4]);
    Assert.assertEquals(arr, arr2);
  }

  @Test
  public void checkBytesToLong() {
    final byte[] arr = new byte[] {8, 7, 6, 5, 4, 3, 2, 1};
    final long result = 8L + (7L << 8) + (6L << 16) + (5L << 24)
               + (4L << 32) + (3L << 40) + (2L << 48) + (1L << 56);
    Assert.assertEquals(bytesToLong(arr), result);
  }

  @Test
  public void checkBytesToString() {
    final long lng = 0XF8F7F6F504030201L;
    //println(Long.toHexString(lng));
    byte[] bytes = new byte[8];
    bytes = longToBytes(lng, bytes);
    final String sep = ".";
    final String unsignLE = bytesToString(bytes, false, true, sep);
    final String signedLE = bytesToString(bytes, true, true, sep);
    final String unsignBE = bytesToString(bytes, false,  false, sep);
    final String signedBE = bytesToString(bytes, true,  false, sep);
    Assert.assertEquals(unsignLE, "1.2.3.4.245.246.247.248");
    Assert.assertEquals(signedLE, "1.2.3.4.-11.-10.-9.-8");
    Assert.assertEquals(unsignBE, "248.247.246.245.4.3.2.1");
    Assert.assertEquals(signedBE, "-8.-9.-10.-11.4.3.2.1");
  }

  @Test
  public void checkNsecToString() {
    final long nS = 1000000000L + 1000000L + 1000L + 1L;
    final String result = nanoSecToString(nS);
    final String expected = "1.001_001_001";
    Assert.assertEquals(result, expected);
  }

  @Test
  public void checkMsecToString() {
    final long nS = 60L * 60L * 1000L + 60L * 1000L + 1000L + 1L;
    final String result = milliSecToString(nS);
    final String expected = "1:01:01.001";
    Assert.assertEquals(result, expected);
  }

  @Test
  public void checkPwr2LawNext() {
    int next = (int)pwr2SeriesNext(2, 1);
    Assert.assertEquals(next, 2);
    next = (int)pwr2SeriesNext(2, 2);
    Assert.assertEquals(next, 3);
    next = (int)pwr2SeriesNext(2, 3);
    Assert.assertEquals(next, 4);

    next = (int)pwr2SeriesNext(2, 0);
    Assert.assertEquals(next, 1);
  }

  @Test
  public void checkPwr2LawNextDouble() {
    double next = powerSeriesNextDouble(2, 1.0, true, 2.0);
    Assert.assertEquals(next, 2.0, 0.0);
    next = powerSeriesNextDouble(2, 2.0, true, 2.0);
    Assert.assertEquals(next, 3.0, 0.0);
    next = powerSeriesNextDouble(2, 3, true, 2.0);
    Assert.assertEquals(next, 4.0, 0.0);

    next = powerSeriesNextDouble(2, 1, false, 2.0);
    Assert.assertEquals(next, Math.sqrt(2), 0.0);
    next = powerSeriesNextDouble(2, 0.5, true, 2.0);
    Assert.assertEquals(next, 2.0, 0.0);
    next = powerSeriesNextDouble(2, 0.5, false, 2.0);
    Assert.assertEquals(next, Math.sqrt(2), 0.0);
    next = powerSeriesNextDouble(2, next, false, 2.0);
    Assert.assertEquals(next, 2.0, 0.0);
  }

  @Test
  public void checkPwr2SeriesExamples() {
    final int maxP = 32;
    final int minP = 1;
    final int ppo = 4;

    for (int p = minP; p <= maxP; p = (int)pwr2SeriesNext(ppo, p)) {
      print(p + " ");
    }
    println("");

    for (int p = maxP; p >= minP; p = pwr2SeriesPrev(ppo, p)) {
      print(p + " ");
    }
    println("");
  }

  @Test
  public void checkExactLog2OfLong() {
    Assert.assertEquals(exactLog2OfLong(2), 1);
    Assert.assertEquals(exactLog2OfLong(1), 0);
    Assert.assertEquals(exactLog2OfLong(1L << 62), 62);
    try {
      exactLog2OfLong(0);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkExactLog2OfInt() {
    Assert.assertEquals(exactLog2OfInt(2), 1);
    Assert.assertEquals(exactLog2OfInt(1), 0);
    Assert.assertEquals(exactLog2OfInt(1 << 30), 30);
    try {
      exactLog2OfInt(0);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkExactLog2OfLongWithArg() {
    Assert.assertEquals(exactLog2OfLong(2, "2"), 1);
    Assert.assertEquals(exactLog2OfLong(1, "1"), 0);
    Assert.assertEquals(exactLog2OfLong(1L << 62,"1L<<62"), 62);
    try {
      exactLog2OfLong(0,"0");
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkExactLog2OfIntWithArg() {
    Assert.assertEquals(exactLog2OfInt(2,"2"), 1);
    Assert.assertEquals(exactLog2OfInt(1,"1"), 0);
    Assert.assertEquals(exactLog2OfInt(1 << 30,"1<<30"), 30);
    try {
      exactLog2OfInt(0,"0");
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  static void checkConvertToLongArray() {
    byte[] arr = {1,2,3,4,5,6,7,8,9,10,11,12};

    long[] out = convertToLongArray(arr, false);
    String s = org.apache.datasketches.common.Util.zeroPad(Long.toHexString(out[0]), 16);
    assertEquals(s, "0807060504030201");
    s = org.apache.datasketches.common.Util.zeroPad(Long.toHexString(out[1]), 16);
    assertEquals(s, "000000000c0b0a09");

    out = convertToLongArray(arr, true);
    s = org.apache.datasketches.common.Util.zeroPad(Long.toHexString(out[0]), 16);
    assertEquals(s, "0102030405060708");
    s = org.apache.datasketches.common.Util.zeroPad(Long.toHexString(out[1]), 16);
    assertEquals(s, "00000000090a0b0c");
  }

  //Resources

  @Test
  public void resourcefileExists() {
    final String shortFileName = "cpc-empty.sk";
    final File file = getResourceFile(shortFileName);
    assertTrue(file.exists());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void resourceFileNotFound() {
    final String shortFileName = "cpc-empty.sk";
    getResourceFile(shortFileName + "123");
  }

  @Test
  public void resourceBytesCorrect() {
    final String shortFileName = "cpc-empty.sk";
    final byte[] bytes = getResourceBytes(shortFileName);
    assertTrue(bytes.length == 8);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void resourceBytesFileNotFound() {
    final String shortFileName = "cpc-empty.sk";
    getResourceBytes(shortFileName + "123");
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  static void println(final Object o) {
    if (o == null) { print(LS); }
    else { print(o.toString() + LS); }
  }

  /**
   * @param o value to print
   */
  static void print(final Object o) {
    if (o != null) {
      //System.out.print(o.toString()); //disable here
    }
  }

}
