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

package org.apache.datasketches.theta;

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
 * Checks that corrupt theta sketch, union and intersection images are rejected without large
 * allocations, including when the result of a wrap is used later.
 */
public class ThetaCorruptImageTest {
  private static final int LG_ARR_BYTE = 4;
  private static final int CUR_COUNT_INT = 8;

  private static UpdatableThetaSketch sketch(final int n) {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk;
  }

  private static void checkRejected(final byte[] bytes, final Function<MemorySegment, Object> deser) {
    final long allocated = AllocationTracker.allocatedBytes(() -> deser.apply(MemorySegment.ofArray(bytes)));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> deser.apply(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void checkValidImagesStillWork() {
    final UpdatableThetaSketch sk = sketch(1000);
    final byte[] c = sk.compact().toByteArray();
    assertEquals(CompactThetaSketch.heapify(MemorySegment.ofArray(c)).getRetainedEntries(), 1000);
    assertEquals(CompactThetaSketch.wrap(MemorySegment.ofArray(c)).getRetainedEntries(), 1000);
    assertEquals(CompactThetaSketch.wrap(c).getRetainedEntries(), 1000);
    final byte[] z = sk.compact().toByteArrayCompressed();
    assertEquals(CompactThetaSketch.heapify(MemorySegment.ofArray(z)).getRetainedEntries(), 1000);
    assertEquals(CompactThetaSketch.wrap(MemorySegment.ofArray(z)).getRetainedEntries(), 1000);
    assertEquals(CompactThetaSketch.wrap(z).getRetainedEntries(), 1000);
    assertEquals(UpdatableThetaSketch.heapify(MemorySegment.ofArray(sk.toByteArray())).getRetainedEntries(), 1000);
    final ThetaUnion union = ThetaSetOperation.builder().buildUnion();
    union.union(sk);
    assertEquals(((ThetaUnion) ThetaSetOperation.heapify(MemorySegment.ofArray(union.toByteArray())))
        .getResult().getRetainedEntries(), 1000);
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();
    inter.intersect(sk);
    assertEquals(((ThetaIntersection) ThetaSetOperation.heapify(MemorySegment.ofArray(inter.toByteArray())))
        .getResult().getRetainedEntries(), 1000);
  }

  @Test
  public void checkCompactCountBeyondImage() {
    final byte[] bytes = sketch(100).compact().toByteArray();
    ByteArrayUtil.putIntLE(bytes, CUR_COUNT_INT, 1 << 28);
    checkRejected(bytes, CompactThetaSketch::heapify);
    checkRejected(bytes, ThetaSketch::heapify);
    checkRejected(bytes, seg -> CompactThetaSketch.wrap(seg).getCache());
    checkRejected(bytes, _ -> CompactThetaSketch.wrap(bytes).getCache());
    final byte[] negative = sketch(100).compact().toByteArray();
    ByteArrayUtil.putIntLE(negative, CUR_COUNT_INT, -1);
    checkRejected(negative, CompactThetaSketch::heapify);
  }

  @Test
  public void checkCompressedCountBeyondImage() {
    final byte[] bytes = sketch(100).compact().toByteArrayCompressed();
    final int numEntriesBytes = bytes[PreambleUtil.NUM_ENTRIES_BYTES_BYTE_V4];
    final int offset = (bytes[0] & 0x3F) << 3;
    for (int i = 0; i < numEntriesBytes; i++) { bytes[offset + i] = (byte) 0xFF; }
    checkRejected(bytes, CompactThetaSketch::heapify);
    checkRejected(bytes, seg -> CompactThetaSketch.wrap(seg).getCache());
    checkRejected(bytes, _ -> CompactThetaSketch.wrap(bytes).getCache());
    final byte[] badBits = sketch(100).compact().toByteArrayCompressed();
    badBits[PreambleUtil.ENTRY_BITS_BYTE_V4] = 0;
    checkRejected(badBits, CompactThetaSketch::heapify);
  }

  @Test
  public void checkUpdatableLargeTable() {
    final byte[] bytes = sketch(100).toByteArray();
    bytes[LG_ARR_BYTE] = 30; //(8 << 30) overflows int
    checkRejected(bytes, UpdatableThetaSketch::heapify);
    checkRejected(bytes, ThetaSketch::heapify);
    checkRejected(bytes, UpdatableThetaSketch::wrap);
    final byte[] count = sketch(100).toByteArray();
    ByteArrayUtil.putIntLE(count, CUR_COUNT_INT, 1 << 28);
    checkRejected(count, UpdatableThetaSketch::heapify);
  }

  @Test
  public void checkUnionLargeTable() {
    final ThetaUnion union = ThetaSetOperation.builder().buildUnion();
    union.union(sketch(100));
    final byte[] bytes = union.toByteArray();
    bytes[LG_ARR_BYTE] = 30;
    checkRejected(bytes, ThetaSetOperation::heapify);
    checkRejected(bytes, seg -> ThetaUnion.wrap(seg).getResult());
  }

  @Test
  public void checkIntersectionLargeTable() {
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();
    inter.intersect(sketch(100));
    final byte[] bytes = inter.toByteArray();
    bytes[LG_ARR_BYTE] = 30;
    checkRejected(bytes, ThetaSetOperation::heapify);
    checkRejected(bytes, seg -> ThetaIntersection.wrap(seg).getResult());
    final byte[] count = inter.toByteArray();
    ByteArrayUtil.putIntLE(count, CUR_COUNT_INT, 1 << 20); //more than the table holds
    checkRejected(count, ThetaSetOperation::heapify);
  }
}
