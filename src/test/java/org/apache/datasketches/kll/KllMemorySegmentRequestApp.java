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

package org.apache.datasketches.kll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentRequestExample;
import org.testng.annotations.Test;

public class KllMemorySegmentRequestApp {

  @Test
  /**
   * This method emulates an application using an off-heap KLL sketch that needs to expand off-heap.
   * This demonstrates one example of how to manage a growing off-heap KLL sketch where the
   * expanded MemorySegments are also off-heap.
   */
  public void checkMemorySegmentRequestExample() {
    final int k = 200;
    final int itemsIn = 10 * k; //will force requests for more space

    //Use the custom MemorySegmentRequestExample to do the allocations.
    final MemorySegmentRequestExample mSegReqEx = new MemorySegmentRequestExample();

    //The allocation of the original off-heap MemorySegment for the KllLongsSketch
    //Note that this targets the size to only handle k values, which is quite small.
    final int numBytes = KllSketch.getMaxSerializedSizeBytes(k, k, KllSketch.SketchType.LONGS_SKETCH, true);

    final MemorySegment seg = mSegReqEx.request(numBytes);

    //Create a new KllLongsSketch and pass the mSegReqEx
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(k, seg, mSegReqEx);

    //Update the sketch with way more data than the original MemorySegment can handle, forcing it to request larger MemorySegments.
    for (int n = 1; n <= itemsIn; n++) { sk.update(n); }

    //Check to make sure the sketch got all the data:
    assertEquals(sk.getMaxItem(), 10 * k);
    assertEquals(sk.getMinItem(), 1);
    assertEquals(sk.getN(), 10 * k);

    //Confirm that the last MemorySegment used by the sketch is, in fact, not the same as the original one that was allocated.
    assertTrue(sk.getMemorySegment() != seg);

    //All done with the sketch. Cleanup any unclosed off-heap MemorySegments including the original allocation.
    mSegReqEx.cleanup();
  }

}
