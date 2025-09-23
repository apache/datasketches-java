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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentRequestExample;
import org.testng.annotations.Test;

public class ClassicQuantilesMemorySegmentRequestApp {

  @Test
  /**
   * This method emulates an application using an off-heap DoublesSketch that needs to expand off-heap.
   * This demonstrates one example of how to manage a growing off-heap DoublesSketch where the
   * expanded MemorySegments are also off-heap.
   */
  public void checkMemorySegmentRequestExtension() {
    final int k = 128; //The default is 128
    final int itemsIn = 40 * k; //will force requests for more space

    //The allocation of the original off-heap MemorySegment for the DoublesSketch
    //Note that this targets the size to only handle 2k values, which is quite small.
    final int initalBytes = DoublesSketch.getUpdatableStorageBytes(k, 2 * k);
    final Arena arena = Arena.ofConfined();
    final MemorySegment seg = arena.allocate(initalBytes);

    //Use the custom extension of the MemorySegmentRequest interface.
    final MemorySegmentRequestExample mSegReqExt = new MemorySegmentRequestExample();

    //Create a new KllLongsSketch and pass the custom extension
    final DoublesSketchBuilder bldr = DoublesSketch.builder().setK(k);
    final DoublesSketch sk = bldr.build(seg, mSegReqExt);

    //Update the sketch with way more data than the original MemorySegment can handle, forcing it to request larger MemorySegments.
    for (int n = 1; n <= itemsIn; n++) {
      sk.update(n);
    }

    //Check to make sure the sketch got all the data:
    assertEquals(sk.getMaxItem(), itemsIn);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getN(), itemsIn);

    //Confirm that the last MemorySegment used by the sketch is, in fact, not the same as the original one that was allocated.
    assertFalse(sk.getMemorySegment().equals(seg));

    //All done with the sketch. Cleanup any unclosed off-heap MemorySegments.
    mSegReqExt.cleanup();

    //Close the original off-heap allocated MemorySegment.
    arena.close();
  }

  static void println(final Object o) { System.out.println(o.toString()); }

}
