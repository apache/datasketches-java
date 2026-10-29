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

package org.apache.datasketches.req;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that corrupt ReqSketch images are rejected without large allocations.
 */
public class ReqCorruptImageTest {
  private static final int EST_SECTION_SIZE_FLOAT = 32; //first compactor, estimation mode
  private static final int EST_NUM_SECTIONS_BYTE = 37;
  private static final int EST_COUNT_INT = 40;
  private static final int EXACT_COUNT_INT = 24; //the only compactor, exact mode

  private static byte[] image(final int n) {
    final ReqSketch sk = ReqSketch.builder().setK(12).build();
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk.toByteArray();
  }

  private static void checkRejected(final byte[] bytes) {
    final long allocated = AllocationTracker.allocatedBytes(() -> ReqSketch.heapify(MemorySegment.ofArray(bytes)));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> ReqSketch.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final int n : new int[] {0, 5, 50, 10_000, 1_000_000}) {
      final ReqSketch sk = ReqSketch.heapify(MemorySegment.ofArray(image(n)));
      assertEquals(sk.getN(), n);
      if (n > 0) {
        sk.update(-1f); //the reconstructed buffers grow as needed
        assertEquals(sk.getN(), n + 1);
      }
    }
  }

  @Test
  public void checkCountBeyondImage() {
    final byte[] est = image(10_000);
    ByteArrayUtil.putIntLE(est, EST_COUNT_INT, 1 << 28);
    checkRejected(est);
    final byte[] exact = image(50);
    ByteArrayUtil.putIntLE(exact, EXACT_COUNT_INT, 1 << 28);
    checkRejected(exact);
  }

  @Test
  public void checkSectionsTooLarge() {
    final byte[] size = image(10_000);
    ByteArrayUtil.putFloatLE(size, EST_SECTION_SIZE_FLOAT, 1e8f);
    checkRejected(size);
    final byte[] sections = image(10_000);
    sections[EST_NUM_SECTIONS_BYTE] = 127;
    ByteArrayUtil.putFloatLE(sections, EST_SECTION_SIZE_FLOAT, 12f);
    checkRejected(sections);
    final byte[] negative = image(10_000);
    negative[EST_NUM_SECTIONS_BYTE] = -1;
    checkRejected(negative);
  }

  @Test
  public void checkInvalidK() {
    final byte[] bytes = image(10_000);
    ByteArrayUtil.putShortLE(bytes, 4, (short) 60_000);
    checkRejected(bytes);
  }
}
