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

package org.apache.datasketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import org.apache.datasketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class AuxHashMapTest {
  static final String LS = System.getProperty("line.separator");

  @Test
  public void checkMustReplace() {
    HeapAuxHashMap map = new HeapAuxHashMap(3, 7);
    map.mustAdd(100, 5);
    int val = map.mustFindValueFor(100);
    assertEquals(val, 5);

    map.mustReplace(100, 10);
    val = map.mustFindValueFor(100);
    assertEquals(val, 10);

    try {
      map.mustReplace(101, 5);
      fail();
    } catch (SketchesStateException e) {
      //expected
    }
  }

  @Test
  public void checkGrowSpace() {
    HeapAuxHashMap map = new HeapAuxHashMap(3, 7);
    assertFalse(map.isMemory());
    assertFalse(map.isOffHeap());
    assertEquals(map.getLgAuxArrInts(), 3);
    for (int i = 1; i <= 7; i++) {
      map.mustAdd(i, i);
    }
    assertEquals(map.getLgAuxArrInts(), 4);
    PairIterator itr = map.getIterator();
    int count1 = 0;
    int count2 = 0;
    while (itr.nextAll()) {
      count2++;
      int pair = itr.getPair();
      if (pair != 0) { count1++; }
    }
    assertEquals(count1, 7);
    assertEquals(count2, 16);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkExceptions1() {
    HeapAuxHashMap map = new HeapAuxHashMap(3, 7);
    map.mustAdd(100, 5);
    map.mustFindValueFor(101);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkExceptions2() {
    HeapAuxHashMap map = new HeapAuxHashMap(3, 7);
    map.mustAdd(100, 5);
    map.mustAdd(100, 6);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
