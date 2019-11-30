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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.Util.iGoldenU64;
import static org.testng.Assert.assertEquals;

import java.util.Random;

/**
 * Experimentally verifies that the Java leading and trailing zeros uses
 * intrinsic CPU instructions instead of the documented code. The java built=in functions are much
 * faster than even choosing most probable bytes algorithms that were derived from C.
 *
 * <p>These tests are for experimental characterization testing only and are not enabled for
 * normal unit testing.
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class LzTzSpeedTest {
  static final byte[] byteTrailingZerosTable = new byte[256];
  static final byte[] byteLeadingZerosTable = new byte[256];

  private static void fillByteTrailingZerosTable() {
    byteTrailingZerosTable[0] = 8;
    for (int i = 1; i < 256; i++) {
      byteTrailingZerosTable[i] = (byte) Integer.numberOfTrailingZeros(i);
    }
  }

  private static void fillByteLeadingZerosTable() {
    byteLeadingZerosTable[0] = 8;
    for (int i = 1; i < 256; i++) {
      byteLeadingZerosTable[i] = (byte) Integer.numberOfLeadingZeros(i << 24);
    }
  }

  static int countLeadingZerosByByte(final long theInput) {
    long tmp;
    if ((tmp = theInput >>> 56) > 0) { return 0 + byteLeadingZerosTable[(int)tmp]; }
    if ((tmp = theInput >>> 48) > 0) { return 8 + byteLeadingZerosTable[(int)tmp]; }
    if ((tmp = theInput >>> 40) > 0) { return 16 + byteLeadingZerosTable[(int)tmp]; }
    if ((tmp = theInput >>> 32) > 0) { return 24 + byteLeadingZerosTable[(int)tmp]; }
    if ((tmp = theInput >>> 24) > 0) { return 32 + byteLeadingZerosTable[(int)tmp]; }
    if ((tmp = theInput >>> 16) > 0) { return 40 + byteLeadingZerosTable[(int)tmp]; }
    if ((tmp = theInput >>>  8) > 0) { return 48 + byteLeadingZerosTable[(int)tmp]; }
    return 56 + byteLeadingZerosTable[(int) (theInput & 0XFFL)];
  }

  static int countTrailingZerosByByte(final long theInput) {
    long tmp = theInput;
    for (int j = 0; j < 8; j++) {
      final int aByte = (int) (tmp & 0XFFL);
      if (aByte != 0) { return (j << 3) + byteTrailingZerosTable[aByte]; }
      tmp >>>= 8;
    }
    return 64;
  }

  //@Test
  public void checkLeadingTrailingZerosByByte() {
    for (int i = 0; i < 64; i++) {
      long in = 1L << i;
      assertEquals(countTrailingZerosByByte(in), Long.numberOfTrailingZeros(in));
      assertEquals(countLeadingZerosByByte(in), Long.numberOfLeadingZeros(in));
    }
  }

  //@Test
  public void checkLeadingZerosByByteRandom() {
    Random rand = new Random();
    int n = 1 << 10;
    long signBit = 1L << 63;
    for (int i = 0; i < 64; i++) {
      for (int j = 0; j < n; j++) {
        long in = (rand.nextLong() | signBit) >>> i;
        assertEquals(countLeadingZerosByByte(in), Long.numberOfLeadingZeros(in));
      }
    }
  }

  //@Test
  public void checkLeadingZerosSpeed() {
    long signBit = 1L << 63;
    int n = 1 << 28;
    for (int shift = 0; shift < 64; shift++) {
      long sum1 = 0;
      long tmp = 0;
      long t0 = System.nanoTime();
      for (int i = 0; i < n; i++) {
        long in =((tmp += iGoldenU64) | signBit) >>> shift;
        sum1 += countLeadingZerosByByte(in);
      }
      long t1 = System.nanoTime();
      double byteTime = (double)(t1 - t0) / n;

      long sum2 = 0;
      tmp = 0;
      long t2 = System.nanoTime();
      for (int i = 0; i < n; i++) {
        long in =((tmp += iGoldenU64) | signBit) >>> shift;
        sum2 += Long.numberOfLeadingZeros(in);
      }
      long t3 = System.nanoTime();
      double longTime = (double)(t3 - t2) / n;
      assert sum1 == sum2;
      println("shift: " + shift + ", byte: " + byteTime + ", long: " + longTime);
    }
  }

  //@Test
  public void checkTrailingZerosSpeed() {
    long oneBit = 1L;
    int n = 1 << 28;
    for (int shift = 0; shift < 64; shift++) {
      long sum1 = 0;
      long tmp = 0;
      long t0 = System.nanoTime();
      for (int i = 0; i < n; i++) {
        long in =((tmp += iGoldenU64) | oneBit) << shift;
        sum1 += countTrailingZerosByByte(in);
      }
      long t1 = System.nanoTime();
      double byteTime = (double)(t1 - t0) / n;

      long sum2 = 0;
      tmp = 0;
      long t2 = System.nanoTime();
      for (int i = 0; i < n; i++) {
        long in =((tmp += iGoldenU64) | oneBit) << shift;
        sum2 += Long.numberOfTrailingZeros(in);
      }
      long t3 = System.nanoTime();
      double longTime = (double)(t3 - t2) / n;
      assert sum1 == sum2;
      println("shift: " + shift + ", byte: " + byteTime + ", long: " + longTime);
    }
  }

  //@Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.println(s); //disable here
  }

  static {
    fillByteTrailingZerosTable();
    fillByteLeadingZerosTable();
  }

}
