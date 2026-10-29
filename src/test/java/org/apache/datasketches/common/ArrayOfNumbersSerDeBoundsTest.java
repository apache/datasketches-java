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

package org.apache.datasketches.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * Checks that ArrayOfNumbersSerDe does not allocate from an item count that the input cannot hold.
 */
public class ArrayOfNumbersSerDeBoundsTest {

  @Test
  public void checkImpossibleItemCount() {
    final ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();
    final MemorySegment seg = MemorySegment.ofArray(serDe.serializeToByteArray(new Number[] {1L, 2.0, 3}));
    final int numItems = 1 << 28; //each item needs at least one byte
    final long allocated = AllocationTracker.allocatedBytes(() -> serDe.deserializeFromMemorySegment(seg, 0, numItems));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> serDe.deserializeFromMemorySegment(seg, 0, numItems));
    assertEquals(serDe.deserializeFromMemorySegment(seg, 0, 3).length, 3);
  }
}
