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

package org.apache.datasketches.tuple.arrayofdoubles;

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
 * Checks that corrupt ArrayOfDoubles sketch and union images are rejected without large allocations,
 * including when the result of a wrap is used later.
 */
public class AodCorruptImageTest {
  private static final int NUM_VALUES_BYTE = 5;
  private static final int COMPACT_COUNT_INT = 16;
  private static final int QS_LG_NOM_BYTE = 16;
  private static final int QS_LG_CUR_BYTE = 17;
  private static final int QS_COUNT_INT = 24;
  private static final int UNION_PREAMBLE = 16;

  private static ArrayOfDoublesUpdatableSketch sketch(final int n) {
    final ArrayOfDoublesUpdatableSketch sk = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    for (int i = 0; i < n; i++) { sk.update(i, new double[] {1.0, 2.0}); }
    return sk;
  }

  private static byte[] unionImage() {
    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setNumberOfValues(2).buildUnion();
    union.union(sketch(10));
    return union.toByteArray();
  }

  private static void checkRejected(final byte[] bytes, final Function<MemorySegment, Object> deser) {
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    final long allocated = AllocationTracker.allocatedBytes(() -> deser.apply(seg));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> deser.apply(seg));
  }

  @Test
  public void checkValidImagesStillWork() {
    final ArrayOfDoublesUpdatableSketch sk = sketch(100);
    final byte[] c = sk.compact().toByteArray();
    assertEquals(ArrayOfDoublesSketch.heapifySketch(MemorySegment.ofArray(c)).getRetainedEntries(), 100);
    assertEquals(ArrayOfDoublesSketch.wrapSketch(MemorySegment.ofArray(c)).getRetainedEntries(), 100);
    final byte[] u = sk.toByteArray();
    assertEquals(ArrayOfDoublesSketch.heapifyUpdatableSketch(MemorySegment.ofArray(u)).getRetainedEntries(), 100);
    assertEquals(ArrayOfDoublesSketch.wrapUpdatableSketch(MemorySegment.ofArray(u)).getRetainedEntries(), 100);
    assertEquals(ArrayOfDoublesUnion.heapify(MemorySegment.ofArray(unionImage())).getResult().getRetainedEntries(), 10);
    assertEquals(ArrayOfDoublesUnion.wrap(MemorySegment.ofArray(unionImage())).getResult().getRetainedEntries(), 10);
    final byte[] e = sketch(0).compact().toByteArray();
    assertTrue(ArrayOfDoublesSketch.heapifySketch(MemorySegment.ofArray(e)).isEmpty());
  }

  @Test
  public void checkSmallNominalEntriesStillWork() {
    //for nominal entries below 16 the table starts at the minimum size, which is more than twice nominal.
    //The set operation builder, unlike the sketch builder, accepts nominal entries below 16.
    for (final int k : new int[] {1, 2, 4, 8, 16, 32}) {
      for (final int n : new int[] {0, 1, 3, 100}) {
        final ArrayOfDoublesSetOperationBuilder bldr =
            new ArrayOfDoublesSetOperationBuilder().setNominalEntries(k).setNumberOfValues(2);
        final ArrayOfDoublesUnion heapUnion = bldr.buildUnion();
        final ArrayOfDoublesUnion directUnion = bldr.buildUnion(MemorySegment.ofArray(new byte[1 << 16]));
        heapUnion.union(sketch(n));
        directUnion.union(sketch(n));
        final int expected = heapUnion.getResult().getRetainedEntries();
        for (final byte[] u : new byte[][] {heapUnion.toByteArray(), directUnion.toByteArray()}) {
          assertEquals(ArrayOfDoublesUnion.heapify(MemorySegment.ofArray(u)).getResult().getRetainedEntries(), expected);
          assertEquals(ArrayOfDoublesUnion.wrap(MemorySegment.ofArray(u)).getResult().getRetainedEntries(), expected);
        }
      }
    }
  }

  @Test
  public void checkCompactCountBeyondImage() {
    final byte[] bytes = sketch(10).compact().toByteArray();
    ByteArrayUtil.putIntLE(bytes, COMPACT_COUNT_INT, 1 << 26);
    checkRejected(bytes, ArrayOfDoublesSketch::heapifySketch);
    checkRejected(bytes, seg -> ArrayOfDoublesSketch.wrapSketch(seg).getValues());
    final byte[] negative = sketch(10).compact().toByteArray();
    ByteArrayUtil.putIntLE(negative, COMPACT_COUNT_INT, -1);
    checkRejected(negative, ArrayOfDoublesSketch::heapifySketch);
  }

  @Test
  public void checkCompactNumValues() {
    final byte[] bytes = sketch(10).compact().toByteArray();
    bytes[NUM_VALUES_BYTE] = (byte) 127; //more values than the image holds
    checkRejected(bytes, ArrayOfDoublesSketch::heapifySketch);
    bytes[NUM_VALUES_BYTE] = (byte) -1;
    checkRejected(bytes, ArrayOfDoublesSketch::heapifySketch);
    checkRejected(bytes, ArrayOfDoublesSketch::wrapSketch);
  }

  @Test
  public void checkUpdatableLargeTable() {
    final byte[] bytes = sketch(10).toByteArray();
    bytes[QS_LG_NOM_BYTE] = 26;
    bytes[QS_LG_CUR_BYTE] = 27; //the image does not hold this table
    checkRejected(bytes, ArrayOfDoublesSketch::heapifyUpdatableSketch);
    checkRejected(bytes, seg -> ArrayOfDoublesSketch.wrapUpdatableSketch(seg).getValues());
    final byte[] bytes2 = sketch(10).toByteArray();
    bytes2[QS_LG_CUR_BYTE] = (byte) (bytes2[QS_LG_NOM_BYTE] + 2);
    checkRejected(bytes2, ArrayOfDoublesSketch::heapifyUpdatableSketch);
  }

  @Test
  public void checkUpdatableCount() {
    final byte[] bytes = sketch(10).toByteArray();
    ByteArrayUtil.putIntLE(bytes, QS_COUNT_INT, 1 << 26); //more than the table holds
    checkRejected(bytes, ArrayOfDoublesSketch::heapifyUpdatableSketch);
    checkRejected(bytes, seg -> ArrayOfDoublesSketch.wrapUpdatableSketch(seg).getValues());
  }

  @Test
  public void checkUnion() {
    final byte[] bytes = unionImage();
    bytes[UNION_PREAMBLE + QS_LG_NOM_BYTE] = 26;
    bytes[UNION_PREAMBLE + QS_LG_CUR_BYTE] = 27;
    checkRejected(bytes, ArrayOfDoublesUnion::heapify);
    checkRejected(bytes, seg -> ArrayOfDoublesUnion.wrap(seg).getResult());
    final byte[] bytes2 = unionImage();
    ByteArrayUtil.putIntLE(bytes2, UNION_PREAMBLE + QS_COUNT_INT, 1 << 26);
    checkRejected(bytes2, ArrayOfDoublesUnion::heapify);
    checkRejected(bytes2, seg -> ArrayOfDoublesUnion.wrap(seg).getResult());
  }
}
