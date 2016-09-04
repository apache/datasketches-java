/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hash;

import static com.yahoo.sketches.hash.MurmurHash3Adaptor.asDouble;
import static com.yahoo.sketches.hash.MurmurHash3Adaptor.asInt;
import static com.yahoo.sketches.hash.MurmurHash3Adaptor.hashToBytes;
import static com.yahoo.sketches.hash.MurmurHash3Adaptor.hashToLongs;
import static com.yahoo.sketches.hash.MurmurHash3Adaptor.modulo;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class MurmurHash3AdaptorTest {

  @Test
  public void checkToBytesLong() {
    byte[] result = hashToBytes(2L, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
  }

  @Test
  public void checkToBytesLongArr() {
    long[] arr = { 1L, 2L };
    byte[] result = hashToBytes(arr, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new long[0];
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);
  }

  @Test
  public void checkToBytesIntArr() {
    int[] arr = { 1, 2 };
    byte[] result = hashToBytes(arr, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new int[0];
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);
  }

  @Test
  public void checkToBytesCharArr() {
    char[] arr = { 1, 2 };
    byte[] result = hashToBytes(arr, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new char[0];
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);
  }
  
  @Test
  public void checkToBytesByteArr() {
    byte[] arr = { 1, 2 };
    byte[] result = hashToBytes(arr, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new byte[0];
    result = hashToBytes(arr, 0L);
    Assert.assertEquals(result, null);

  }

  @Test
  public void checkToBytesDouble() {
    byte[] result = hashToBytes(1.0, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    result = hashToBytes(0.0, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    result = hashToBytes( -0.0, 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
  }

  @Test
  public void checkToBytesString() {
    byte[] result = hashToBytes("1", 0L);
    for (int i = 8; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    result = hashToBytes("", 0L);
    Assert.assertEquals(result, null);

    String s = null;
    result = hashToBytes(s, 0L);
    Assert.assertEquals(result, null);
  }

  /************/

  @Test
  public void checkToLongsLong() {
    long[] result = hashToLongs(2L, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
  }

  @Test
  public void checkToLongsLongArr() {
    long[] arr = { 1L, 2L };
    long[] result = hashToLongs(arr, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new long[0];
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);
  }

  @Test
  public void checkToLongsIntArr() {
    int[] arr = { 1, 2 };
    long[] result = hashToLongs(arr, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new int[0];
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);
  }

  @Test
  public void checkToLongsCharArr() {
    char[] arr = { 1, 2 };
    long[] result = hashToLongs(arr, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new char[0];
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);
  }
  
  @Test
  public void checkToLongsByteArr() {
    byte[] arr = { 1, 2 };
    long[] result = hashToLongs(arr, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    arr = null;
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);

    arr = new byte[0];
    result = hashToLongs(arr, 0L);
    Assert.assertEquals(result, null);

  }

  @Test
  public void checkToLongsDouble() {
    long[] result = hashToLongs(1.0, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    result = hashToLongs(0.0, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    result = hashToLongs( -0.0, 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
  }

  @Test
  public void checkToLongsString() {
    long[] result = hashToLongs("1", 0L);
    for (int i = 2; i-- > 0;) {
      Assert.assertNotEquals(result[i], 0);
    }
    result = hashToLongs("", 0L);
    Assert.assertEquals(result, null);
    String s = null;
    result = hashToLongs(s, 0L);
    Assert.assertEquals(result, null);
  }

  /*************/

  @Test
  public void checkModulo() {
    int div = 7;
    for (int i = 20; i-- > 0;) {
      long[] out = hashToLongs(i, 9001);
      int mod = modulo(out[0], out[1], div);
      Assert.assertTrue((mod < div) && (mod >= 0));
      mod = modulo(out, div);
      Assert.assertTrue((mod < div) && (mod >= 0));
    }
  }

  @Test
  public void checkAsDouble() {
    for (int i = 0; i < 10000; i++ ) {
      double result = asDouble(hashToLongs(i, 0));
      Assert.assertTrue((result >= 0) && (result < 1.0));
    }
  }

  //Check asInt() functions
  
  @Test 
  public void checkAsInt() {
    int lo = (3 << 28);
    int hi = (1 << 30) + 1;
    for (byte i = 0; i < 126; i++ ) {
      long[] longArr = {i, i+1};         //long[]
      int result = asInt(longArr, lo);
      Assert.assertTrue((result >= 0) && (result < lo));
      result = asInt(longArr, hi);
      Assert.assertTrue((result >= 0) && (result < hi));
      
      int[] intArr = {i, i+1};           //int[]
      result = asInt(intArr, lo);
      Assert.assertTrue((result >= 0) && (result < lo));
      result = asInt(intArr, hi);
      Assert.assertTrue((result >= 0) && (result < hi));
      
      byte[] byteArr = {i, (byte)(i+1)}; //byte[]
      result = asInt(byteArr, lo);
      Assert.assertTrue((result >= 0) && (result < lo));
      result = asInt(byteArr, hi);
      Assert.assertTrue((result >= 0) && (result < hi));
      
      long longV = i;                    //long
      result = asInt(longV, lo);
      Assert.assertTrue((result >= 0) && (result < lo));
      result = asInt(longV, hi);
      Assert.assertTrue((result >= 0) && (result < hi));
      
      double v = i;                //double
      result = asInt(v, lo);
      Assert.assertTrue((result >= 0) && (result < lo));
      result = asInt(v, hi);
      Assert.assertTrue((result >= 0) && (result < hi));
      
      String s = Integer.toString(i);    //String
      result = asInt(s, lo);
      Assert.assertTrue((result >= 0) && (result < lo));
      result = asInt(s, hi);
      Assert.assertTrue((result >= 0) && (result < hi));
    }
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseLongNull() {
    long[] arr = null;
    asInt(arr, 1000);
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseLongEmpty() {
    long[] arr = new long[0];
    asInt(arr, 1000);
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseIntNull() {
    int[] arr = null;
    asInt(arr, 1000);
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseIntEmpty() {
    int[] arr = new int[0];
    asInt(arr, 1000);
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseByteNull() {
    byte[] arr = null;
    asInt(arr, 1000);
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseByteEmpty() {
    byte[] arr = new byte[0];
    asInt(arr, 1000);
  }

  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseStringNull() {
    String s = null;
    asInt(s, 1000);
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseStringEmpty() {
    String s = "";
    asInt(s, 1000);
  }
  
  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkAsIntCornerCaseNTooSmall() {
    String s = "abc";
    asInt(s, 1);
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
