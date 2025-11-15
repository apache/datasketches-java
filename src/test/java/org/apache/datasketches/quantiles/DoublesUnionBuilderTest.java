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

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

public class DoublesUnionBuilderTest {

  @Test
  public void checkBuilds() {
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().build();
    for (int i=0; i<1000; i++) { qs1.update(i); }

    final int bytes = qs1.getCurrentCompactSerializedSizeBytes();
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[bytes]);
    qs1.putIntoMemorySegment(dstSeg);
    final MemorySegment srcSeg = dstSeg;

    final QuantilesDoublesUnionBuilder bldr = new QuantilesDoublesUnionBuilder();
    bldr.setMaxK(128);
    QuantilesDoublesUnion union = bldr.build(); //virgin union

    union = QuantilesDoublesUnion.heapify(srcSeg);
    final QuantilesDoublesSketch qs2 = union.getResult();
    assertEquals(qs1.getCurrentCompactSerializedSizeBytes(), qs2.getCurrentCompactSerializedSizeBytes());

    union = QuantilesDoublesUnion.heapify(qs2);
    final QuantilesDoublesSketch qs3 = union.getResult();
    assertEquals(qs2.getCurrentCompactSerializedSizeBytes(), qs3.getCurrentCompactSerializedSizeBytes());
    assertFalse(qs2 == qs3);
  }


@Test
public void checkDeprecated1() {
  final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().build();
  for (int i=0; i<1000; i++) {
    qs1.update(i);
  }

  final int bytes = qs1.getCurrentCompactSerializedSizeBytes();
  final MemorySegment dstSeg = MemorySegment.ofArray(new byte[bytes]);
  qs1.putIntoMemorySegment(dstSeg);
  final MemorySegment srcSeg = dstSeg;

  final QuantilesDoublesUnionBuilder bldr = new QuantilesDoublesUnionBuilder();
  bldr.setMaxK(128);
  QuantilesDoublesUnion union = bldr.build(); //virgin union

  union = QuantilesDoublesUnion.heapify(srcSeg); //heapify
  final QuantilesDoublesSketch qs2 = union.getResult();
  assertEquals(qs1.getCurrentCompactSerializedSizeBytes(), qs2.getCurrentCompactSerializedSizeBytes());
  assertEquals(qs1.getCurrentUpdatableSerializedSizeBytes(), qs2.getCurrentUpdatableSerializedSizeBytes());

  union = QuantilesDoublesUnion.heapify(qs2);  //heapify again
  final QuantilesDoublesSketch qs3 = union.getResult();
  assertEquals(qs2.getCurrentCompactSerializedSizeBytes(), qs3.getCurrentCompactSerializedSizeBytes());
  assertEquals(qs2.getCurrentUpdatableSerializedSizeBytes(), qs3.getCurrentUpdatableSerializedSizeBytes());
  assertFalse(qs2 == qs3); //different objects
}

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param o value to print
   */
  static void println(final Object o) {
    //System.err.println(o.toString()); //disable here
  }

}
