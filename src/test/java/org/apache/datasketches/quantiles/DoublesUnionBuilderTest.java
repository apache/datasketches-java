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

package org.apache.datasketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

@SuppressWarnings("javadoc")
public class DoublesUnionBuilderTest {

  @Test
  public void checkBuilds() {
    UpdateDoublesSketch qs1 = DoublesSketch.builder().build();
    for (int i=0; i<1000; i++) { qs1.update(i); }

    int bytes = qs1.getCompactStorageBytes();
    WritableMemory dstMem = WritableMemory.wrap(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;

    DoublesUnionBuilder bldr = new DoublesUnionBuilder();
    bldr.setMaxK(128);
    DoublesUnion union = bldr.build(); //virgin union

    union = DoublesUnion.heapify(srcMem);
    DoublesSketch qs2 = union.getResult();
    assertEquals(qs1.getCompactStorageBytes(), qs2.getCompactStorageBytes());

    union = DoublesUnion.heapify(qs2);
    DoublesSketch qs3 = union.getResult();
    assertEquals(qs2.getCompactStorageBytes(), qs3.getCompactStorageBytes());
    assertFalse(qs2 == qs3);
  }


@Test
public void checkDeprecated1() {
  UpdateDoublesSketch qs1 = DoublesSketch.builder().build();
  for (int i=0; i<1000; i++) {
    qs1.update(i);
  }

  int bytes = qs1.getCompactStorageBytes();
  WritableMemory dstMem = WritableMemory.wrap(new byte[bytes]);
  qs1.putMemory(dstMem);
  Memory srcMem = dstMem;

  DoublesUnionBuilder bldr = new DoublesUnionBuilder();
  bldr.setMaxK(128);
  DoublesUnion union = bldr.build(); //virgin union

  union = DoublesUnion.heapify(srcMem); //heapify
  DoublesSketch qs2 = union.getResult();
  assertEquals(qs1.getCompactStorageBytes(), qs2.getCompactStorageBytes());
  assertEquals(qs1.getUpdatableStorageBytes(), qs2.getUpdatableStorageBytes());

  union = DoublesUnion.heapify(qs2);  //heapify again
  DoublesSketch qs3 = union.getResult();
  assertEquals(qs2.getCompactStorageBytes(), qs3.getCompactStorageBytes());
  assertEquals(qs2.getUpdatableStorageBytes(), qs3.getUpdatableStorageBytes());
  assertFalse(qs2 == qs3); //different objects
}

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }

}
