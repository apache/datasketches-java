/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

import static com.yahoo.sketches.HashOperations.checkHashCorruption;
import static com.yahoo.sketches.HashOperations.checkThetaCorruption;
import static com.yahoo.sketches.HashOperations.continueCondition;
import static com.yahoo.sketches.HashOperations.fastHashInsertOnly;
import static com.yahoo.sketches.HashOperations.fastHashSearchOrInsert;
import static com.yahoo.sketches.HashOperations.hashArrayInsert;
import static com.yahoo.sketches.HashOperations.hashInsertOnly;
import static com.yahoo.sketches.HashOperations.hashSearch;
import static com.yahoo.sketches.HashOperations.hashSearchOrInsert;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

public class HashOperationsTest {

  //Not otherwise already covered

  @Test(expectedExceptions = SketchesStateException.class)
  public void testThetaCorruption1() {
    checkThetaCorruption(0);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void testThetaCorruption2() {
    checkThetaCorruption(-1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void testHashCorruption() {
    checkHashCorruption(-1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHashSearch() {
    hashSearch(new long[4], 2, 0);
  }

  @Test
  public void checkHashArrayInsert() {
    long[] hTable = new long[16];
    long[] hashIn = new long[1];
    for (int i=0; i<8; i++) {
      hashIn[0] = i;
      long h = hash(hashIn, 0)[0] >>> 1;
      hashInsertOnly(hTable, 4, h);
      int count = hashArrayInsert(hTable, hTable, 4, Long.MAX_VALUE);
      assertEquals(count, 0);
    }

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
    WritableMemory mem = WritableMemory.wrap(table);
    int index = fastHashInsertOnly(mem, 5, 1, 0);
    assertEquals(index, 1);
    assertEquals(table[1], 1L);
  }

  @Test
  public void testHashInsertOnlyMemoryWithStride() {
    long[] table = new long[32];
    table[1] = 1;
    WritableMemory mem = WritableMemory.wrap(table);
    int index = fastHashInsertOnly(mem, 5, 1, 0);
    assertEquals(index, 2);
    assertEquals(table[2], 1L);
  }

  @Test
  public void checkFullHeapTableCatchesInfiniteLoop() {
    long[] table = new long[32];
    for (int i = 1; i <= 32; ++i) {
      hashInsertOnly(table, 5, i);
    }

    // table full; search returns not found, others throw exception
    final int retVal = hashSearch(table, 5, 33);
    assertEquals(retVal, -1);

    try {
      hashInsertOnly(table, 5, 33);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      hashSearchOrInsert(table, 5, 33);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkFullDirectTableCatchesInfiniteLoop() {
    long[] table = new long[32];
    WritableMemory mem = WritableMemory.wrap(table);
    for (int i = 1; i <= 32; ++i) {
      fastHashInsertOnly(mem, 5, i, 0);
    }

    // table full; search returns not found, others throw exception
    final int retVal = hashSearch(mem, 5, 33, 0);
    assertEquals(retVal, -1);

    try {
      fastHashInsertOnly(mem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      fastHashSearchOrInsert(mem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkFullFastDirectTableCatchesInfiniteLoop() {
    long[] table = new long[32];
    WritableMemory wmem = WritableMemory.wrap(table);

    for (int i = 1; i <= 32; ++i) {
      fastHashInsertOnly(wmem, 5, i, 0);
    }

    // table full; throws exception
    try {
      fastHashInsertOnly(wmem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      fastHashSearchOrInsert(wmem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
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
