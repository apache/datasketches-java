/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.ByteArrayUtil.getDoubleBE;
import static com.yahoo.sketches.ByteArrayUtil.getDoubleLE;
import static com.yahoo.sketches.ByteArrayUtil.getFloatBE;
import static com.yahoo.sketches.ByteArrayUtil.getFloatLE;
import static com.yahoo.sketches.ByteArrayUtil.getIntBE;
import static com.yahoo.sketches.ByteArrayUtil.getIntLE;
import static com.yahoo.sketches.ByteArrayUtil.getLongBE;
import static com.yahoo.sketches.ByteArrayUtil.getLongLE;
import static com.yahoo.sketches.ByteArrayUtil.getShortBE;
import static com.yahoo.sketches.ByteArrayUtil.getShortLE;
import static com.yahoo.sketches.ByteArrayUtil.putDoubleBE;
import static com.yahoo.sketches.ByteArrayUtil.putDoubleLE;
import static com.yahoo.sketches.ByteArrayUtil.putFloatBE;
import static com.yahoo.sketches.ByteArrayUtil.putFloatLE;
import static com.yahoo.sketches.ByteArrayUtil.putIntBE;
import static com.yahoo.sketches.ByteArrayUtil.putIntLE;
import static com.yahoo.sketches.ByteArrayUtil.putLongBE;
import static com.yahoo.sketches.ByteArrayUtil.putLongLE;
import static com.yahoo.sketches.ByteArrayUtil.putShortBE;
import static com.yahoo.sketches.ByteArrayUtil.putShortLE;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ByteArrayUtilTest {

  @Test
  public void checkGetPutShortLE() {
    byte[] arr = { 79, -93, 124, 117 };
    short out1 = getShortLE(arr, 0);
    short out2 = getShortLE(arr, 2);
    byte[] arr2 = new byte[4];
    putShortLE(arr2, 0, out1);
    putShortLE(arr2, 2, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutShortBE() {
    byte[] arr = { 79, -93, 124, 117 };
    short out1 = getShortBE(arr, 0);
    short out2 = getShortBE(arr, 2);
    byte[] arr2 = new byte[4];
    putShortBE(arr2, 0, out1);
    putShortBE(arr2, 2, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutIntLE() {
    byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77 };
    int out1 = getIntLE(arr, 0);
    int out2 = getIntLE(arr, 4);
    byte[] arr2 = new byte[8];
    putIntLE(arr2, 0, out1);
    putIntLE(arr2, 4, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutIntBE() {
    byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77 };
    int out1 = getIntBE(arr, 0);
    int out2 = getIntBE(arr, 4);
    byte[] arr2 = new byte[8];
    putIntBE(arr2, 0, out1);
    putIntBE(arr2, 4, out2);
    assertEquals(arr2, arr);
  }


  @Test
  public void checkGetPutLongLE() {
    byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77, 5, -95, -15, 41, -89, -124, -26, -87 };
    long out1 = getLongLE(arr, 0);
    long out2 = getLongLE(arr, 8);
    byte[] arr2 = new byte[16];
    putLongLE(arr2, 0, out1);
    putLongLE(arr2, 8, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutLongBE() {
    byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77, 5, -95, -15, 41, -89, -124, -26, -87 };
    long out1 = getLongBE(arr, 0);
    long out2 = getLongBE(arr, 8);
    byte[] arr2 = new byte[16];
    putLongBE(arr2, 0, out1);
    putLongBE(arr2, 8, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutFloatLE() {
    byte[] arr = { -37, 15, 73, 64, 84, -8, 45, 64 }; //PI, E
    float out1 = getFloatLE(arr, 0);
    float out2 = getFloatLE(arr, 4);
    byte[] arr2 = new byte[8];
    putFloatLE(arr2, 0, out1);
    putFloatLE(arr2, 4, out2);
    assertEquals(arr2, arr);
    assertEquals(out1, (float)Math.PI);
    assertEquals(out2, (float)Math.E);
  }

  @Test
  public void checkGetPutFloatBE() {
    byte[] arr = { -37, 15, 73, 64, 84, -8, 45, 64 }; //PI, E
    float out1 = getFloatBE(arr, 0);
    float out2 = getFloatBE(arr, 4);
    byte[] arr2 = new byte[8];
    putFloatBE(arr2, 0, out1);
    putFloatBE(arr2, 4, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutDoubleLE() {
    byte[] arr = { 24, 45, 68, 84, -5, 33, 9, 64, 105, 87, 20, -117, 10, -65, 5, 64 }; //PI, E
    double out1 = getDoubleLE(arr, 0);
    double out2 = getDoubleLE(arr, 8);
    byte[] arr2 = new byte[16];
    putDoubleLE(arr2, 0, out1);
    putDoubleLE(arr2, 8, out2);
    assertEquals(arr2, arr);
    assertEquals(out1, Math.PI);
    assertEquals(out2, Math.E);
  }

  @Test
  public void checkGetPutDoubleBE() {
    byte[] arr = { 24, 45, 68, 84, -5, 33, 9, 64, 105, 87, 20, -117, 10, -65, 5, 64 }; //PI, E
    double out1 = getDoubleBE(arr, 0);
    double out2 = getDoubleBE(arr, 8);
    byte[] arr2 = new byte[16];
    putDoubleBE(arr2, 0, out1);
    putDoubleBE(arr2, 8, out2);
    assertEquals(arr2, arr);
  }

}
