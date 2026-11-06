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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;
import java.util.function.Function;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that corrupt HLL images are rejected without large allocations.
 */
public class HllCorruptImageTest {
  private static final int LG_K_BYTE = 3;
  private static final int LG_ARR_BYTE = 4;
  private static final int LIST_COUNT_BYTE = 6;
  private static final int HASH_SET_COUNT_INT = 8;
  private static final int AUX_COUNT_INT = 36;

  private static HllSketch sketch(final TgtHllType type, final int n) {
    final HllSketch sk = new HllSketch(12, type);
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk;
  }

  private static void checkRejected(final byte[] bytes, final Function<MemorySegment, Object> deser) {
    final long allocated = AllocationTracker.allocatedBytes(() -> deser.apply(MemorySegment.ofArray(bytes)));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> deser.apply(MemorySegment.ofArray(bytes)));
  }

  private static void checkAllPaths(final byte[] bytes) {
    checkRejected(bytes, HllSketch::heapify);
    checkRejected(bytes, HllSketch::wrap);
    checkRejected(bytes, HllSketch::writableWrap);
    checkRejected(bytes, HllUnion::heapify);
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final TgtHllType type : TgtHllType.values()) {
      for (final int n : new int[] {0, 5, 200, 10_000}) {
        final HllSketch sk = sketch(type, n);
        for (final byte[] b : new byte[][] {sk.toCompactByteArray(), sk.toUpdatableByteArray()}) {
          assertEquals(HllSketch.heapify(MemorySegment.ofArray(b)).getEstimate(), sk.getEstimate());
          assertEquals(HllSketch.wrap(MemorySegment.ofArray(b)).getEstimate(), sk.getEstimate());
        }
      }
    }
  }

  @Test
  public void checkLgKTooLarge() {
    for (final TgtHllType type : TgtHllType.values()) {
      for (final int n : new int[] {5, 200, 10_000}) { //list, set and HLL modes
        final byte[] bytes = sketch(type, n).toUpdatableByteArray();
        bytes[LG_K_BYTE] = 30;
        checkAllPaths(bytes);
      }
    }
  }

  @Test
  public void checkListCount() {
    final byte[] bytes = sketch(TgtHllType.HLL_8, 5).toUpdatableByteArray();
    bytes[LIST_COUNT_BYTE] = (byte) 200;
    checkAllPaths(bytes);
  }

  @Test
  public void checkSetSizes() {
    final byte[] lgArr = sketch(TgtHllType.HLL_8, 200).toUpdatableByteArray();
    lgArr[LG_ARR_BYTE] = 28;
    checkAllPaths(lgArr);
    final byte[] count = sketch(TgtHllType.HLL_8, 200).toCompactByteArray();
    ByteArrayUtil.putIntLE(count, HASH_SET_COUNT_INT, 1 << 28);
    checkAllPaths(count);
  }

  @Test
  public void checkAuxSizes() {
    final byte[] bytes = sketch(TgtHllType.HLL_4, 10_000).toUpdatableByteArray();
    ByteArrayUtil.putIntLE(bytes, AUX_COUNT_INT, 1 << 28);
    checkAllPaths(bytes);
    final byte[] lgArr = sketch(TgtHllType.HLL_4, 10_000).toUpdatableByteArray();
    if (ByteArrayUtil.getIntLE(lgArr, AUX_COUNT_INT) == 0) { ByteArrayUtil.putIntLE(lgArr, AUX_COUNT_INT, 1); }
    lgArr[LG_ARR_BYTE] = 28;
    checkAllPaths(lgArr);
  }
}
