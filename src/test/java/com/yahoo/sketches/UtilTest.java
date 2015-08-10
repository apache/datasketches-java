/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.checkIfMultipleOf8AndGT0;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.Util.checkProbability;
import static com.yahoo.sketches.Util.floorPowerOf2;
import static com.yahoo.sketches.Util.isMultipleOf8AndGT0;
import static com.yahoo.sketches.Util.isPowerOf2;
import static com.yahoo.sketches.Util.zeroPad;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 */
public class UtilTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
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
      Assert.fail("Should have thrown IllegalArgumentException");
    } 
    catch (IllegalArgumentException e) {
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
  
  @Test(expectedExceptions = IllegalArgumentException.class)
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
  
  @Test(expectedExceptions = IllegalArgumentException.class)
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
    Assert.assertTrue(Util.isLessThanUnsigned(n1, n2));
    Assert.assertTrue(Util.isLessThanUnsigned(n2, n3));
    Assert.assertTrue(Util.isLessThanUnsigned(n3, n4));
    Assert.assertFalse(Util.isLessThanUnsigned(n2, n1));
    Assert.assertFalse(Util.isLessThanUnsigned(n3, n2));
    Assert.assertFalse(Util.isLessThanUnsigned(n4, n3));
  }
  
  @Test
  public void checkPrintln() {
    print("com.yahoo.sketches.UtilTest"); println(".checkPrintln():");
    print("  Long MAX & MIN: "); print(Long.MAX_VALUE); print(", "); println(Long.MIN_VALUE);
    print("  Doubles:        "); print(1.2345); print(", "); println(5.4321);
  }

  @Test
  public void checkZeroPad() {
    long v = 123456789;
    String vHex = Long.toHexString(v);
    String out = zeroPad(vHex, 16);
    println(out);
  }
  
  @Test
  public void checkProbabilityFn1() {
    checkProbability(.5, "Good");
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkProbabilityFn2() {
    checkProbability(-.5, "Too Low");
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkProbabilityFn3() {
    checkProbability(1.5, "Too High");
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
  /**
   * @param d value to print 
   */
  static void println(double d) {
    //System.out.println(d); //disable here
  }
  
  /**
   * @param s value to print 
   */
  static void print(String s) {
    //System.out.println(s); //disable here
  }
  
  /**
   * @param d value to print 
   */
  static void print(double d) {
    //System.out.println(d); //disable here
  }
  
}