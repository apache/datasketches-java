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

package org.apache.datasketches.kll;

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

public class KllItemsSketchSerDeTest {
  private final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test
  public void serializeDeserializeEmpty() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertTrue(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    try { sk2.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sk2.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllItemsSketch<String> sk3 = KllItemsSketch.wrap(MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);
    assertTrue(sk3.isEmpty());
    assertEquals(sk3.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk3.getN(), sk1.getN());
    assertEquals(sk3.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    try { sk3.getMinItem(); fail(); } catch (final SketchesArgumentException e) {}
    try { sk3.getMaxItem(); fail(); } catch (final SketchesArgumentException e) {}
    assertEquals(sk3.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap -> byte[] -> compare byte[]
    final byte[] bytes2 = sk3.toByteArray();
    assertEquals(bytes, bytes2);
  }

  @Test
  public void serializeDeserializeOneValue() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sk1.update(" 1");
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertFalse(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), 1);
    assertEquals(sk2.getN(), 1);
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk2.getMinItem(), " 1");
    assertEquals(sk2.getMaxItem(), " 1");
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllItemsSketch<String> sk3 = KllItemsSketch.wrap(MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);
    assertFalse(sk3.isEmpty());
    assertEquals(sk3.getNumRetained(), 1);
    assertEquals(sk3.getN(), 1);
    assertEquals(sk3.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk3.getMinItem(), " 1");
    assertEquals(sk3.getMaxItem(), " 1");
    assertEquals(sk3.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap -> byte[] -> compare byte[]
    final byte[] bytes2 = sk3.toByteArray();
    assertEquals(bytes, bytes2);
  }

  @Test
  public void serializeDeserializeMultipleValues() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sk1.update(Util.longToFixedLengthString(i, 4));
    }
    assertEquals(sk1.getMinItem(), "   0");
    assertEquals(sk1.getMaxItem(), " 999");
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertFalse(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk2.getMinItem(), sk1.getMinItem());
    assertEquals(sk2.getMaxItem(), sk1.getMaxItem());
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllItemsSketch<String> sk3 = KllItemsSketch.wrap(MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);
    assertFalse(sk3.isEmpty());
    assertEquals(sk3.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk3.getN(), sk1.getN());
    assertEquals(sk3.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk3.getMinItem(), sk1.getMinItem());
    assertEquals(sk3.getMaxItem(), sk1.getMaxItem());
    assertEquals(sk3.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap -> byte[] -> compare byte[]
    final byte[] bytes2 = sk3.toByteArray();
    assertEquals(bytes, bytes2);
  }

  @Test
  // Querying a heap Items sketch before serialization must not corrupt the round-trip
  // sorted view. CreateSortedView sorted a defensive copy from getTotalItemsArray() but
  // still set levelZeroSorted, so heapify/wrap skipped sorting (#756).
  public void serializeDeserializeAfterQueryHeapifyMatches() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(8, Comparator.naturalOrder(), serDe);
    sk.update("a");
    sk.update("b");
    sk.update("c");
    sk.update("d");
    assertFalse(sk.isLevelZeroSorted());
    sk.getQuantile(0.5, INCLUSIVE); // any query builds the sorted view
    assertFalse(sk.isLevelZeroSorted()); // live level-0 was not sorted

    final KllItemsSketch<String> rt = KllItemsSketch.heapify(
        MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    assertEquals(rt.getNumRetained(), sk.getNumRetained());
    assertEquals(rt.getSortedView().getQuantiles(), sk.getSortedView().getQuantiles());
    assertEquals(rt.getSortedView().getCumulativeWeights(), sk.getSortedView().getCumulativeWeights());
    for (int i = 0; i <= 20; i++) {
      final double rank = i / 20.0;
      assertEquals(rt.getQuantile(rank, INCLUSIVE), sk.getQuantile(rank, INCLUSIVE),
          "rank=" + rank);
    }
  }

  @Test
  public void serializeDeserializeAfterQueryWrapMatches() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(8, Comparator.naturalOrder(), serDe);
    sk.update("a");
    sk.update("b");
    sk.update("c");
    sk.update("d");
    sk.getQuantile(0.5, INCLUSIVE);

    final KllItemsSketch<String> rt = KllItemsSketch.wrap(
        MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    assertEquals(rt.getSortedView().getQuantiles(), sk.getSortedView().getQuantiles());
    assertEquals(rt.getSortedView().getCumulativeWeights(), sk.getSortedView().getCumulativeWeights());
    assertEquals(rt.getQuantile(0.5, INCLUSIVE), sk.getQuantile(0.5, INCLUSIVE));
    assertEquals(rt.getQuantile(0.55, INCLUSIVE), sk.getQuantile(0.55, INCLUSIVE));
  }

  @Test
  public void serializeDeserializeAfterQueryWithCompaction() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(8, Comparator.naturalOrder(), serDe);
    for (int i = 1; i <= 8; i++) {
      sk.update(String.valueOf((char) ('a' + i - 1)));
    }
    sk.getQuantile(0.5, INCLUSIVE);
    assertFalse(sk.isLevelZeroSorted());

    final KllItemsSketch<String> heapified = KllItemsSketch.heapify(
        MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    final KllItemsSketch<String> wrapped = KllItemsSketch.wrap(
        MemorySegment.ofArray(sk.toByteArray()), Comparator.naturalOrder(), serDe);
    for (int i = 0; i <= 20; i++) {
      final double rank = i / 20.0;
      final String expected = sk.getQuantile(rank, INCLUSIVE);
      assertEquals(heapified.getQuantile(rank, INCLUSIVE), expected, "heapify rank=" + rank);
      assertEquals(wrapped.getQuantile(rank, INCLUSIVE), expected, "wrap rank=" + rank);
    }
  }

}
