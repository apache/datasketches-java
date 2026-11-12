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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.testng.annotations.Test;

/**
 * Checks that heap KLL sketches allocate item storage on first use, or when ensureCapacity() is called.
 * KLL compaction is random, so images are compared byte for byte only below k items, before any compaction.
 */
public class KllLazyAllocationTest {
  private static final int K = 200;
  private static final ArrayOfStringsSerDe SERDE = new ArrayOfStringsSerDe();

  private static KllHeapDoublesSketch heapDoubles(final KllDoublesSketch sk) { return (KllHeapDoublesSketch) sk; }

  @Test
  public void doublesEmptySketchHoldsNoStorage() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(K);
    assertFalse(heapDoubles(sk).hasItemStorage());
    sk.update(Double.NaN); //ignored, so not a first use
    assertTrue(sk.isEmpty());
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumRetained(), 0);
    assertFalse(sk.iterator().next());
    sk.toString(true, true);

    final KllDoublesSketch ref = KllDoublesSketch.newHeapInstance(K);
    ref.ensureCapacity();
    assertEquals(sk.toByteArray(), ref.toByteArray());
    assertEquals(KllHelper.toByteArray(sk, true), KllHelper.toByteArray(ref, true));

    sk.merge(KllDoublesSketch.newHeapInstance(K)); //merging an empty sketch is not a first use
    assertFalse(heapDoubles(sk).hasItemStorage());

    final KllDoublesSketch copy = KllDoublesSketch.heapify(MemorySegment.ofArray(sk.toByteArray()));
    assertFalse(heapDoubles(copy).hasItemStorage());
    assertTrue(copy.isEmpty());
    assertFalse(heapDoubles(sk).hasItemStorage());
    assertFalse(heapDoubles(copy).hasItemStorage());
  }

  @Test
  public void doublesEnsureCapacity() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(K);
    sk.ensureCapacity();
    final KllHeapDoublesSketch heap = heapDoubles(sk);
    assertTrue(heap.hasItemStorage());
    final double[] storage = heap.getDoubleItemsArray();
    assertEquals(storage.length, K);
    sk.ensureCapacity(); //no effect once allocated
    assertSame(heap.getDoubleItemsArray(), storage);

    final KllDoublesSketch lazy = KllDoublesSketch.newHeapInstance(K);
    for (int i = 0; i < K; i++) { sk.update(i); lazy.update(i); } //fills the initial storage, no compaction
    assertSame(heap.getDoubleItemsArray(), storage); //nothing more was allocated
    assertEquals(sk.toByteArray(), lazy.toByteArray());
    assertEquals(KllHelper.toByteArray(sk, true), KllHelper.toByteArray(lazy, true));

    final double[] before = heap.getDoubleItemsArray();
    final byte[] image = sk.toByteArray();
    sk.ensureCapacity(); //no effect after updates
    assertSame(heap.getDoubleItemsArray(), before);
    assertEquals(sk.toByteArray(), image);
  }

  @Test
  public void doublesFirstUseAllocates() {
    final KllDoublesSketch single = KllDoublesSketch.newHeapInstance(K);
    single.update(1.0);
    assertTrue(heapDoubles(single).hasItemStorage());
    assertEquals(single.getMinItem(), 1.0);

    final KllDoublesSketch weighted = KllDoublesSketch.newHeapInstance(K);
    weighted.update(2.0, 3);
    assertTrue(heapDoubles(weighted).hasItemStorage());
    assertEquals(weighted.getN(), 3);

    final KllDoublesSketch heavy = KllDoublesSketch.newHeapInstance(K);
    heavy.update(2.0, 10_000); //takes the merge path
    assertEquals(heavy.getN(), 10_000);
    assertEquals(heavy.getRank(2.0, INCLUSIVE), 1.0);

    final KllDoublesSketch vector = KllDoublesSketch.newHeapInstance(K);
    vector.update(new double[] {3.0, Double.NaN, 4.0}, 0, 3);
    assertEquals(vector.getN(), 2);
    assertEquals(vector.getMaxItem(), 4.0);

    final KllDoublesSketch merged = KllDoublesSketch.newHeapInstance(K);
    final KllDoublesSketch other = KllDoublesSketch.newHeapInstance(K);
    for (int i = 0; i < 10_000; i++) { other.update(i); }
    merged.merge(other);
    assertTrue(heapDoubles(merged).hasItemStorage());
    assertEquals(merged.getN(), 10_000);
    assertEquals(merged.getMinItem(), 0.0);
    assertEquals(merged.getMaxItem(), 9999.0);
    final double r = merged.getRank(5000, INCLUSIVE);
    assertTrue(Math.abs(r - 0.5) < 0.05, "rank " + r);

    final KllDoublesSketch fromSingle = KllDoublesSketch.newHeapInstance(K);
    fromSingle.merge(KllDoublesSketch.heapify(MemorySegment.ofArray(single.toByteArray())));
    assertEquals(fromSingle.toByteArray(), single.toByteArray());
  }

  @Test
  public void doublesResetReleasesStorage() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(K);
    for (int i = 0; i < 1000; i++) { sk.update(i); }
    sk.reset();
    assertFalse(heapDoubles(sk).hasItemStorage());
    assertTrue(sk.isEmpty());
    final KllDoublesSketch fresh = KllDoublesSketch.newHeapInstance(K);
    for (int i = 0; i < 100; i++) { sk.update(i); fresh.update(i); }
    assertEquals(sk.toByteArray(), fresh.toByteArray());
  }

  @Test
  public void directSketchIsUnaffected() {
    final int bytes = KllSketch.getMaxSerializedSizeBytes(K, 1000, KllSketch.SketchType.KLL_DOUBLES_SKETCH, true);
    final KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(K, MemorySegment.ofArray(new byte[bytes]), null);
    sk.ensureCapacity(); //no effect
    for (int i = 0; i < 1000; i++) { sk.update(i); }
    sk.reset();
    assertTrue(sk.isEmpty());
    sk.update(1.0);
    assertEquals(sk.getN(), 1);
  }

  @Test
  public void floatsLazy() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(K);
    final KllHeapFloatsSketch heap = (KllHeapFloatsSketch) sk;
    assertFalse(heap.hasItemStorage());
    sk.update(Float.NaN);
    assertFalse(heap.hasItemStorage());
    final KllFloatsSketch ref = KllFloatsSketch.newHeapInstance(K);
    ref.ensureCapacity();
    assertEquals(heap.getFloatItemsArray().length, K); //reading the array for writing allocates it
    assertEquals(sk.toByteArray(), ref.toByteArray());
    assertEquals(KllHelper.toByteArray(sk, true), KllHelper.toByteArray(ref, true));

    final KllFloatsSketch lazy = KllFloatsSketch.newHeapInstance(K);
    assertFalse(((KllHeapFloatsSketch) lazy).hasItemStorage());
    for (int i = 0; i < K; i++) { ref.update(i); lazy.update(i); }
    assertEquals(ref.toByteArray(), lazy.toByteArray());
    lazy.reset();
    assertFalse(((KllHeapFloatsSketch) lazy).hasItemStorage());
    final KllFloatsSketch merged = KllFloatsSketch.newHeapInstance(K);
    merged.merge(ref);
    assertEquals(merged.getN(), K);
  }

  @Test
  public void longsLazy() {
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(K);
    final KllHeapLongsSketch heap = (KllHeapLongsSketch) sk;
    assertFalse(heap.hasItemStorage());
    final KllLongsSketch ref = KllLongsSketch.newHeapInstance(K);
    ref.ensureCapacity();
    assertTrue(((KllHeapLongsSketch) ref).hasItemStorage());
    assertEquals(sk.toByteArray(), ref.toByteArray());
    assertEquals(KllHelper.toByteArray(sk, true), KllHelper.toByteArray(ref, true));
    assertFalse(heap.hasItemStorage());

    for (int i = 0; i < K; i++) { sk.update(i); ref.update(i); }
    assertEquals(sk.toByteArray(), ref.toByteArray());
    sk.reset();
    assertFalse(heap.hasItemStorage());
    final KllLongsSketch merged = KllLongsSketch.newHeapInstance(K);
    merged.merge(ref);
    assertEquals(merged.getN(), K);
    assertEquals(merged.getMaxItem(), K - 1);
  }

  @Test
  public void itemsLazy() {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(K, Comparator.naturalOrder(), SERDE);
    final KllHeapItemsSketch<String> heap = (KllHeapItemsSketch<String>) sk;
    assertFalse(heap.hasItemStorage());
    sk.update(null); //ignored
    assertFalse(heap.hasItemStorage());
    assertFalse(sk.iterator().next());
    sk.toString(true, true);
    assertEquals(heap.getRetainedItemsArray().length, 0);

    final KllItemsSketch<String> ref = KllItemsSketch.newHeapInstance(K, Comparator.naturalOrder(), SERDE);
    ref.ensureCapacity();
    assertTrue(((KllHeapItemsSketch<String>) ref).hasItemStorage());
    assertEquals(sk.toByteArray(), ref.toByteArray());

    sk.merge(KllItemsSketch.newHeapInstance(K, Comparator.naturalOrder(), SERDE));
    assertFalse(heap.hasItemStorage());
    final KllItemsSketch<String> copy = KllItemsSketch.heapify(MemorySegment.ofArray(sk.toByteArray()),
        Comparator.naturalOrder(), SERDE);
    assertFalse(((KllHeapItemsSketch<String>) copy).hasItemStorage());

    for (int i = 0; i < K; i++) { sk.update(Integer.toString(i)); ref.update(Integer.toString(i)); }
    assertTrue(heap.hasItemStorage());
    assertEquals(sk.toByteArray(), ref.toByteArray());

    final KllItemsSketch<String> weighted = KllItemsSketch.newHeapInstance(K, Comparator.naturalOrder(), SERDE);
    weighted.update("a", 10_000);
    assertEquals(weighted.getN(), 10_000);

    final KllItemsSketch<String> merged = KllItemsSketch.newHeapInstance(K, Comparator.naturalOrder(), SERDE);
    merged.merge(ref);
    assertEquals(merged.getN(), K);
    //merging copies level 0 one item at a time, which reverses its order, so compare contents, not bytes
    assertEquals(merged.getMinItem(), ref.getMinItem());
    assertEquals(merged.getMaxItem(), ref.getMaxItem());
    assertEquals(merged.getRank("5", INCLUSIVE), ref.getRank("5", INCLUSIVE));

    sk.reset();
    assertFalse(heap.hasItemStorage());
    assertTrue(sk.isEmpty());
    sk.update("z");
    assertEquals(sk.getMinItem(), "z");
  }
}
