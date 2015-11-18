/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.HashOperations.*;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;


public class HashOperationsTest {
  
  //Not otherwise already covered
  
  @Test(expectedExceptions = IllegalStateException.class)
  public void testThetaCorruption1() {
    checkThetaCorruption(0);
  }
  
  @Test(expectedExceptions = IllegalStateException.class)
  public void testThetaCorruption2() {
    checkThetaCorruption(-1);
  }
  
  @Test(expectedExceptions = IllegalStateException.class)
  public void testHashCorruption() {
    checkHashCorruption(-1);
  }
  
  @Test(expectedExceptions = IllegalStateException.class)
  public void testHashAndThetaCorruption1() {
    checkHashAndThetaCorruption(0, 0); //theta = 0 fails
  }
  
  @Test(expectedExceptions = IllegalStateException.class)
  public void testHashAndThetaCorruption2() {
    checkHashAndThetaCorruption(-1, 0); //theta = -1 fails 
  }
  
  @Test(expectedExceptions = IllegalStateException.class)
  public void testHashAndThetaCorruption3() {
    checkHashAndThetaCorruption(1, -1); //hash = -1 fails
  }
  
  @Test
  public void testContinueCondtion() {
    long thetaLong = Long.MAX_VALUE/2;
    assertTrue(continueCondition(thetaLong, 0));
    assertTrue(continueCondition(thetaLong, thetaLong));
    assertTrue(continueCondition(thetaLong, thetaLong +1));
    assertFalse(continueCondition(thetaLong, thetaLong -1));
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
