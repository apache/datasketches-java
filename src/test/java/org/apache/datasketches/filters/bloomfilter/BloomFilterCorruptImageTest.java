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

package org.apache.datasketches.filters.bloomfilter;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that corrupt BloomFilter images are rejected, or handled, without large allocations.
 */
public class BloomFilterCorruptImageTest {
  private static final int NUM_LONGS_INT = 16;
  private static final int LARGE = 1 << 28; //2 GB of longs

  @Test
  public void checkNonEmptyArrayBeyondImage() {
    final BloomFilter bf = BloomFilterBuilder.createBySize(1000, 3, 123L);
    bf.update(1L);
    final byte[] bytes = bf.toByteArray();
    ByteArrayUtil.putIntLE(bytes, NUM_LONGS_INT, LARGE);
    final long allocated = AllocationTracker.allocatedBytes(() -> BloomFilter.heapify(MemorySegment.ofArray(bytes)));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> BloomFilter.heapify(MemorySegment.ofArray(bytes)));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void checkEmptyImageIsUsable() {
    final BloomFilter bf = BloomFilter.heapify(MemorySegment.ofArray(BloomFilterBuilder.createBySize(1000, 3, 123L).toByteArray()));
    assertFalse(bf.query(1L));
    bf.update(1L);
    assertTrue(bf.query(1L));
    final BloomFilter bf2 = BloomFilter.heapify(MemorySegment.ofArray(bf.toByteArray()));
    assertTrue(bf2.query(1L));
    final BloomFilter empty = BloomFilter.heapify(MemorySegment.ofArray(BloomFilterBuilder.createBySize(1000, 3, 123L).toByteArray()));
    empty.union(bf);
    assertTrue(empty.query(1L));
    final BloomFilter empty2 = BloomFilter.heapify(MemorySegment.ofArray(BloomFilterBuilder.createBySize(1000, 3, 123L).toByteArray()));
    empty2.invert();
    assertTrue(empty2.query(2L));
  }
}
