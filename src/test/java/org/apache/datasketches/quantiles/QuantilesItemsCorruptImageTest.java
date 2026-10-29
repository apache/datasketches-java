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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that a corrupt QuantilesItemsSketch image is rejected.
 */
public class QuantilesItemsCorruptImageTest {
  private static final int N_LONG = 8;
  private final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

  private byte[] image(final int n) {
    final QuantilesItemsSketch<Long> sk = QuantilesItemsSketch.getInstance(Long.class, 32768, Comparator.naturalOrder());
    for (long i = 0; i < n; i++) { sk.update(i); }
    return sk.toByteArray(serDe);
  }

  @Test
  public void checkLargeN() {
    final byte[] bytes = image(1);
    ByteArrayUtil.putLongLE(bytes, N_LONG, 1L << 60);
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    final long allocated = AllocationTracker.allocatedBytes(
        () -> QuantilesItemsSketch.heapify(Long.class, seg, Comparator.naturalOrder(), serDe));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class,
        () -> QuantilesItemsSketch.heapify(Long.class, seg, Comparator.naturalOrder(), serDe));
    final byte[] negative = image(1);
    ByteArrayUtil.putLongLE(negative, N_LONG, -1L);
    expectThrows(SketchesArgumentException.class,
        () -> QuantilesItemsSketch.heapify(Long.class, MemorySegment.ofArray(negative), Comparator.naturalOrder(), serDe));
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final int n : new int[] {1, 100, 100_000}) {
      final QuantilesItemsSketch<Long> sk =
          QuantilesItemsSketch.heapify(Long.class, MemorySegment.ofArray(image(n)), Comparator.naturalOrder(), serDe);
      assertEquals(sk.getN(), n);
    }
  }
}
