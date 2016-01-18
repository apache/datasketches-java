/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

import static com.yahoo.sketches.HashOperations.*;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

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
  
  @Test(expectedExceptions = IllegalArgumentException.class)
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
  public void testHashInsertOnlyNoStride() {
    long[] table = new long[32];
    int index = hashInsertOnly(table, 5, 1);
    assertEquals(index, 1);
    assertEquals(table[1], 1L);
  }

  @Test
  public void testHashInsertOnlyWithStride() {
    long[] table = new long[32];
    table[1] = 1;
    int index = hashInsertOnly(table, 5, 1);
    assertEquals(index, 2);
    assertEquals(table[2], 1L);
  }

  @Test
  public void testHashInsertOnlyMemoryNoStride() {
    long[] table = new long[32];
    Memory mem = new NativeMemory(table);
    int index = hashInsertOnly(mem, 5, 1, 0);
    assertEquals(index, 1);
    assertEquals(table[1], 1L);
  }

  @Test
  public void testHashInsertOnlyMemoryWithStride() {
    long[] table = new long[32];
    table[1] = 1;
    Memory mem = new NativeMemory(table);
    int index = hashInsertOnly(mem, 5, 1, 0);
    assertEquals(index, 2);
    assertEquals(table[2], 1L);
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
