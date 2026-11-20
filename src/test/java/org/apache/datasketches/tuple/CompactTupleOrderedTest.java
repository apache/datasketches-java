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

import static org.apache.datasketches.tuple.PreambleUtil.COMPACT_FLAGS_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Map;

import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.testng.annotations.Test;

/**
 * Checks that compact tuple sketches are ordered by hash, as in C++, and that the image says so.
 */
public class CompactTupleOrderedTest {
  private static final int READ_ONLY = 1 << 1;
  private static final int EMPTY = 1 << 2;
  private static final int COMPACT = 1 << 3;
  private static final int ORDERED = 1 << 4;

  private static UpdatableTupleSketch<Double, DoubleSummary> newSketch(final int n, final int offset) {
    final UpdatableTupleSketch<Double, DoubleSummary> sk =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
    for (int i = 0; i < n; i++) { sk.update(i + offset, (double) (i + offset)); }
    return sk;
  }

  //The hashes are strictly increasing. If src is given, every hash keeps the summary it has in src.
  private static void checkOrdered(final CompactTupleSketch<DoubleSummary> csk, final TupleSketch<DoubleSummary> src) {
    assertTrue(csk.isOrdered());
    final Map<Long, Double> expected = new HashMap<>();
    if (src != null) {
      final TupleSketchIterator<DoubleSummary> sit = src.iterator();
      while (sit.next()) { expected.put(sit.getHash(), sit.getSummary().getValue()); }
    }
    final TupleSketchIterator<DoubleSummary> it = csk.iterator();
    long prev = -1;
    int count = 0;
    while (it.next()) {
      assertTrue(it.getHash() > prev);
      prev = it.getHash();
      if (src != null) { assertEquals(it.getSummary().getValue(), expected.get(it.getHash())); }
      count++;
    }
    assertEquals(count, csk.getRetainedEntries());
  }

  @Test
  public void compactIsOrdered() {
    for (final int n : new int[] {0, 1, 2, 17, 1000, 100_000}) {
      final UpdatableTupleSketch<Double, DoubleSummary> sk = newSketch(n, 0);
      final CompactTupleSketch<DoubleSummary> csk = sk.compact();
      checkOrdered(csk, sk);
      final int flags = csk.toByteArray()[COMPACT_FLAGS_BYTE] & 0xFF;
      assertEquals(flags, READ_ONLY | COMPACT | ORDERED | ((n == 0) ? EMPTY : 0), "n = " + n);
    }
  }

  @Test
  public void setOperationResultsAreOrdered() {
    final UpdatableTupleSketch<Double, DoubleSummary> a = newSketch(10_000, 0);
    final UpdatableTupleSketch<Double, DoubleSummary> b = newSketch(10_000, 5_000);
    final DoubleSummarySetOperations ops = new DoubleSummarySetOperations();
    final TupleUnion<DoubleSummary> union = new TupleUnion<>(ops);
    union.union(a);
    union.union(b);
    //union and intersection combine summaries, so only the order is checked
    checkOrdered(union.getResult(), null);
    checkOrdered(new TupleUnion<>(ops).union(a, b), null);
    checkOrdered(new TupleIntersection<>(ops).intersect(a, b), null);
    checkOrdered(TupleAnotB.aNotB(a, b), a);
    final TupleAnotB<DoubleSummary> anotb = new TupleAnotB<>();
    anotb.setA(a);
    anotb.notB(b);
    final CompactTupleSketch<DoubleSummary> r1 = anotb.getResult(false);
    checkOrdered(r1, a);
    assertEquals(anotb.getResult(false).toByteArray(), r1.toByteArray()); //the operator's own state is intact
  }

  @Test
  public void inputArraysAreNotChanged() {
    final long[] hashes = {30, 10, 20};
    final DoubleSummary[] sums = {new DoubleSummary(DoubleSummary.Mode.Sum).update(3.0),
        new DoubleSummary(DoubleSummary.Mode.Sum).update(1.0), new DoubleSummary(DoubleSummary.Mode.Sum).update(2.0)};
    final CompactTupleSketch<DoubleSummary> csk =
        new CompactTupleSketch<>(hashes, sums, Long.MAX_VALUE, false, Util.DEFAULT_SEED_HASH);
    assertEquals(hashes, new long[] {30, 10, 20});
    assertEquals(sums[0].getValue(), 3.0);
    final TupleSketchIterator<DoubleSummary> it = csk.iterator();
    double v = 0;
    while (it.next()) {
      v++;
      assertEquals(it.getHash(), (long) (v * 10));
      assertEquals(it.getSummary().getValue(), v);
    }
  }

  //An image without the ordered flag, as earlier Java releases wrote, reads back unordered and is written back
  //unchanged. With at most one entry it is ordered regardless of the flag.
  @Test
  public void unorderedImageStaysUnordered() {
    final DoubleSummaryDeserializer deser = new DoubleSummaryDeserializer();
    final byte[] bytes = newSketch(1000, 0).compact().toByteArray();
    bytes[COMPACT_FLAGS_BYTE] &= (byte) ~ORDERED;
    final CompactTupleSketch<DoubleSummary> csk =
        (CompactTupleSketch<DoubleSummary>) TupleSketch.heapifySketch(MemorySegment.ofArray(bytes), deser);
    assertFalse(csk.isOrdered());
    assertEquals(csk.toByteArray(), bytes);
    for (final int n : new int[] {0, 1}) {
      final byte[] small = newSketch(n, 0).compact().toByteArray();
      small[COMPACT_FLAGS_BYTE] &= (byte) ~ORDERED;
      final CompactTupleSketch<DoubleSummary> s =
          (CompactTupleSketch<DoubleSummary>) TupleSketch.heapifySketch(MemorySegment.ofArray(small), deser);
      assertTrue(s.isOrdered());
    }
  }
}
