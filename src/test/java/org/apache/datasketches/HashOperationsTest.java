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

package org.apache.datasketches;

import static org.apache.datasketches.HashOperations.checkHashCorruption;
import static org.apache.datasketches.HashOperations.checkThetaCorruption;
import static org.apache.datasketches.HashOperations.continueCondition;
import static org.apache.datasketches.HashOperations.hashInsertOnlyMemory;
import static org.apache.datasketches.HashOperations.hashSearchOrInsertMemory;
import static org.apache.datasketches.HashOperations.hashArrayInsert;
import static org.apache.datasketches.HashOperations.hashInsertOnly;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.HashOperations.hashSearchMemory;
import static org.apache.datasketches.HashOperations.hashSearchOrInsert;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
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
    final long[] hTable = new long[16];
    final long[] hashIn = new long[1];
    for (int i = 0; i < 8; i++) {
      hashIn[0] = i;
      final long h = hash(hashIn, 0)[0] >>> 1;
      hashInsertOnly(hTable, 4, h);
      final int count = hashArrayInsert(hTable, hTable, 4, Long.MAX_VALUE);
      assertEquals(count, 0);
    }

  }

  @Test
  public void testContinueCondtion() {
    final long thetaLong = Long.MAX_VALUE / 2;
    assertTrue(continueCondition(thetaLong, 0));
    assertTrue(continueCondition(thetaLong, thetaLong));
    assertTrue(continueCondition(thetaLong, thetaLong + 1));
    assertFalse(continueCondition(thetaLong, thetaLong - 1));
  }

  @Test
  public void testHashInsertOnlyNoStride() {
    final long[] table = new long[32];
    final int index = hashInsertOnly(table, 5, 1);
    assertEquals(index, 1);
    assertEquals(table[1], 1L);
  }

  @Test
  public void testHashInsertOnlyWithStride() {
    final long[] table = new long[32];
    table[1] = 1;
    final int index = hashInsertOnly(table, 5, 1);
    assertEquals(index, 2);
    assertEquals(table[2], 1L);
  }

  @Test
  public void testHashInsertOnlyMemoryNoStride() {
    final long[] table = new long[32];
    final WritableMemory mem = WritableMemory.wrap(table);
    final int index = hashInsertOnlyMemory(mem, 5, 1, 0);
    assertEquals(index, 1);
    assertEquals(table[1], 1L);
  }

  @Test
  public void testHashInsertOnlyMemoryWithStride() {
    final long[] table = new long[32];
    table[1] = 1;
    final WritableMemory mem = WritableMemory.wrap(table);
    final int index = hashInsertOnlyMemory(mem, 5, 1, 0);
    assertEquals(index, 2);
    assertEquals(table[2], 1L);
  }

  @Test
  public void checkFullHeapTableCatchesInfiniteLoop() {
    final long[] table = new long[32];
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
    final long[] table = new long[32];
    final WritableMemory mem = WritableMemory.wrap(table);
    for (int i = 1; i <= 32; ++i) {
      hashInsertOnlyMemory(mem, 5, i, 0);
    }

    // table full; search returns not found, others throw exception
    final int retVal = hashSearchMemory(mem, 5, 33, 0);
    assertEquals(retVal, -1);

    try {
      hashInsertOnlyMemory(mem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      hashSearchOrInsertMemory(mem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkFullFastDirectTableCatchesInfiniteLoop() {
    final long[] table = new long[32];
    final WritableMemory wmem = WritableMemory.wrap(table);

    for (int i = 1; i <= 32; ++i) {
      hashInsertOnlyMemory(wmem, 5, i, 0);
    }

    // table full; throws exception
    try {
      hashInsertOnlyMemory(wmem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      hashSearchOrInsertMemory(wmem, 5, 33, 0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }


  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
