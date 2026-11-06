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

package org.apache.datasketches.cpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that corrupt CpcSketch images are rejected without large allocations.
 */
public class CpcCorruptImageTest {

  private static byte[] image(final int n) {
    final CpcSketch sk = new CpcSketch(11);
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk.toByteArray();
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final int n : new int[] {0, 5, 100, 3000, 100_000}) {
      final CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < n; i++) { sk.update(i); }
      assertEquals(CpcSketch.heapify(MemorySegment.ofArray(sk.toByteArray())).getEstimate(), sk.getEstimate());
    }
  }

  private static void checkRejected(final byte[] bytes) {
    final long allocated = AllocationTracker.allocatedBytes(() -> CpcSketch.heapify(MemorySegment.ofArray(bytes)));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> CpcSketch.heapify(MemorySegment.ofArray(bytes)));
  }

  private static int offset(final byte[] bytes, final PreambleUtil.HiField field) {
    return (int) PreambleUtil.getHiFieldOffset(PreambleUtil.getFormat(MemorySegment.ofArray(bytes)), field);
  }

  @Test
  public void checkNumCouponsTooLarge() {
    for (final int n : new int[] {100, 3000, 100_000}) { //sparse, hybrid and pinned or sliding formats
      for (final int numCoupons : new int[] {Integer.MAX_VALUE, 1 << 25, -1}) {
        final byte[] bytes = image(n);
        ByteArrayUtil.putIntLE(bytes, offset(bytes, PreambleUtil.HiField.NUM_COUPONS), numCoupons);
        checkRejected(bytes);
      }
    }
  }

  @Test
  public void checkStreamLengthTooLarge() {
    for (final int n : new int[] {100, 3000, 100_000}) {
      for (final int len : new int[] {Integer.MAX_VALUE, 1 << 28, -1}) {
        final byte[] bytes = image(n);
        final PreambleUtil.HiField field = (n > 3000) ? PreambleUtil.HiField.W_LENGTH_INTS
            : PreambleUtil.HiField.SV_LENGTH_INTS;
        ByteArrayUtil.putIntLE(bytes, offset(bytes, field), len);
        checkRejected(bytes);
      }
    }
  }
}
