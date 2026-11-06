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

package org.apache.datasketches.tuple;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleTupleSketch;
import org.testng.annotations.Test;

/**
 * Checks that corrupt tuple sketch images are rejected without large allocations.
 */
@SuppressWarnings("deprecation")
public class TupleCorruptImageTest {
  private static final int LG_NOM_BYTE = 5;
  private static final int LG_CUR_BYTE = 6;
  private static final int COMPACT_COUNT_INT = 8;
  private final DoubleSummaryDeserializer deser = new DoubleSummaryDeserializer();

  private static UpdatableTupleSketch<Double, DoubleSummary> sketch(final int n) {
    final UpdatableTupleSketch<Double, DoubleSummary> sk =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
    for (int i = 0; i < n; i++) { sk.update(i, 1.0); }
    return sk;
  }

  private void checkRejected(final byte[] bytes) {
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    final long allocated = AllocationTracker.allocatedBytes(() -> TupleSketch.heapifySketch(seg, deser));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> TupleSketch.heapifySketch(seg, deser));
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final int n : new int[] {0, 1, 10, 100, 10_000}) {
      final UpdatableTupleSketch<Double, DoubleSummary> sk = sketch(n);
      assertEquals(TupleSketch.heapifySketch(MemorySegment.ofArray(sk.toByteArray()), deser).getRetainedEntries(),
          sk.getRetainedEntries());
      assertEquals(TupleSketch.heapifySketch(MemorySegment.ofArray(sk.compact().toByteArray()), deser)
          .getRetainedEntries(), sk.getRetainedEntries());
    }
  }

  @Test
  public void checkUpdatableLargeTable() {
    final byte[] bytes = sketch(10).toByteArray();
    bytes[LG_NOM_BYTE] = 25;
    bytes[LG_CUR_BYTE] = 26; //far too large for 10 entries
    checkRejected(bytes);
  }

  @Test
  public void checkSmallNominalEntriesStillWork() {
    //for nominal entries below 16 the table starts at the minimum size, which is more than twice nominal
    for (int lgK = 0; lgK <= 5; lgK++) {
      for (int lgRF = 0; lgRF <= 3; lgRF++) {
        for (final int n : new int[] {0, 1, 3, 100}) {
          final DoubleTupleSketch sk = new DoubleTupleSketch(lgK, lgRF, 1.0F, DoubleSummary.Mode.Sum);
          for (int i = 0; i < n; i++) { sk.update(i, 1.0); }
          final byte[] bytes = sk.toByteArray();
          assertEquals(TupleSketch.heapifySketch(MemorySegment.ofArray(bytes), deser).getRetainedEntries(),
              sk.getRetainedEntries());
          assertEquals(new DoubleTupleSketch(MemorySegment.ofArray(bytes), DoubleSummary.Mode.Sum)
              .getRetainedEntries(), sk.getRetainedEntries());
        }
      }
    }
  }

  @Test
  public void checkUpdatableTableLargerThanNominal() {
    final byte[] bytes = sketch(10).toByteArray();
    bytes[LG_CUR_BYTE] = (byte) (bytes[LG_NOM_BYTE] + 2);
    checkRejected(bytes);
    final byte[] bytes2 = sketch(10).toByteArray();
    bytes2[LG_NOM_BYTE] = 30;
    checkRejected(bytes2);
    final byte[] bytes3 = sketch(10).toByteArray();
    bytes3[LG_NOM_BYTE] = 27; //lgK above the Theta maximum of 26
    checkRejected(bytes3);
  }

  @Test
  public void checkConstructorNominalEntriesLimit() {
    final DoubleSummaryFactory factory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum);
    assertEquals(new UpdatableTupleSketch<Double, DoubleSummary>(1 << 26, 3, 1f, factory).getNominalEntries(), 1 << 26);
    expectThrows(SketchesArgumentException.class,
        () -> new UpdatableTupleSketch<Double, DoubleSummary>((1 << 26) + 1, 3, 1f, factory));
    expectThrows(SketchesArgumentException.class,
        () -> new UpdatableTupleSketch<Double, DoubleSummary>(0, 3, 1f, factory));
    expectThrows(SketchesArgumentException.class, () -> new TupleUnion<DoubleSummary>(1 << 27, null));
    expectThrows(SketchesArgumentException.class,
        () -> new DoubleTupleSketch(27, 3, 1f, DoubleSummary.Mode.Sum));
  }

  @Test
  public void checkUpdatableCountBeyondImage() {
    final byte[] bytes = sketch(10).toByteArray();
    ByteArrayUtil.putIntLE(bytes, 8, 1 << 28); //count: no sampling and no theta, so it is at byte 8
    checkRejected(bytes);
  }

  @Test
  public void checkCompactCountBeyondImage() {
    final byte[] bytes = sketch(10).compact().toByteArray();
    ByteArrayUtil.putIntLE(bytes, COMPACT_COUNT_INT, 1 << 28);
    checkRejected(bytes);
    final byte[] negative = sketch(10).compact().toByteArray();
    ByteArrayUtil.putIntLE(negative, COMPACT_COUNT_INT, -1);
    checkRejected(negative);
  }

  @Test
  public void checkCompactTruncatedPreamble() {
    final byte[] bytes = java.util.Arrays.copyOf(sketch(10).compact().toByteArray(), 12);
    checkRejected(bytes);
  }
}
