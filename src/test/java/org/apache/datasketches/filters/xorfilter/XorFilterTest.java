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

package org.apache.datasketches.filters.xorfilter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

public class XorFilterTest {

  // builds a filter over the range [0, numItems) and confirms there are never any false negatives
  @Test
  public void noFalseNegativesTest() {
    final long numItems = 100_000;
    final long seed = 8123L;

    final XorFilterBuilder builder = new XorFilterBuilder(8, seed);
    for (long i = 0; i < numItems; ++i) {
      builder.update(i);
    }
    final XorFilter filter = builder.build();

    // a xor filter never produces false negatives
    for (long i = 0; i < numItems; ++i) {
      assertTrue(filter.query(i));
    }
  }

  // confirms the observed false positive rate is close to the theoretical 1 / 2^bits
  @Test
  public void falsePositiveRateTest() {
    final long numItems = 200_000;
    final long seed = 55L;

    final XorFilterBuilder builder = new XorFilterBuilder(8, seed);
    for (long i = 0; i < numItems; ++i) {
      builder.update(i);
    }
    final XorFilter filter = builder.build();

    long falsePositives = 0;
    final long numQueries = 1_000_000;
    for (long i = numItems; i < (numItems + numQueries); ++i) {
      if (filter.query(i)) { ++falsePositives; }
    }
    final double observedFpp = (double) falsePositives / numQueries;
    // theoretical is 1/256 ~= 0.0039; allow generous head-room for noise
    assertTrue(observedFpp < 0.01, "Observed FPP too high: " + observedFpp);
  }

  // 16-bit fingerprints should produce a substantially lower false positive rate than 8-bit
  @Test
  public void sixteenBitLowerFppTest() {
    final long numItems = 200_000;
    final long seed = 909L;

    final double fpp8 = observedFpp(8, seed, numItems);
    final double fpp16 = observedFpp(16, seed, numItems);

    // theoretical is 1/65536 ~= 1.5e-5 for 16-bit versus 1/256 ~= 3.9e-3 for 8-bit
    assertTrue(fpp16 < 0.001, "Observed 16-bit FPP too high: " + fpp16);
    assertTrue(fpp16 < fpp8, "16-bit FPP (" + fpp16 + ") should be lower than 8-bit FPP (" + fpp8 + ")");
  }

  // builds a filter over [0, numItems) and returns the observed false positive rate over an equal
  // number of items that were never added
  private static double observedFpp(final int bits, final long seed, final long numItems) {
    final XorFilterBuilder builder = new XorFilterBuilder(bits, seed);
    for (long i = 0; i < numItems; ++i) {
      builder.update(i);
    }
    final XorFilter filter = builder.build();
    assertEquals(filter.getBitsPerFingerprint(), bits);

    long falsePositives = 0;
    final long numQueries = 1_000_000;
    for (long i = numItems; i < (numItems + numQueries); ++i) {
      if (filter.query(i)) { ++falsePositives; }
    }
    return (double) falsePositives / numQueries;
  }

  // exercises the typed query methods matching the BloomFilter surface
  @Test
  public void typedItemsTest() {
    final long seed = 321L;
    final XorFilterBuilder builder = new XorFilterBuilder(8, seed);
    builder.update(42L);
    builder.update(3.14159);
    builder.update("datasketches");
    builder.update(new byte[] { 1, 2, 3, 4 });
    builder.update(new char[] { 'a', 'b', 'c' });
    builder.update(new short[] { 10, 20, 30 });
    builder.update(new int[] { 100, 200, 300 });
    builder.update(new long[] { 1L, 2L, 3L });
    final XorFilter filter = builder.build();

    assertTrue(filter.query(42L));
    assertTrue(filter.query(3.14159));
    assertTrue(filter.query("datasketches"));
    assertTrue(filter.query(new byte[] { 1, 2, 3, 4 }));
    assertTrue(filter.query(new char[] { 'a', 'b', 'c' }));
    assertTrue(filter.query(new short[] { 10, 20, 30 }));
    assertTrue(filter.query(new int[] { 100, 200, 300 }));
    assertTrue(filter.query(new long[] { 1L, 2L, 3L }));
  }

  // duplicate items must not break the peeling construction
  @Test
  public void duplicateItemsTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 1L);
    for (int i = 0; i < 1000; ++i) {
      builder.update(i % 100); // only 100 distinct values, each added 10 times
    }
    final XorFilter filter = builder.build();
    for (int i = 0; i < 100; ++i) {
      assertTrue(filter.query(i));
    }
  }

  // an empty filter can be built and queried without error; because the fingerprint table is all
  // zeros an absent item is reported present only when its fingerprint folds to zero (~1 / 2^bits)
  @Test
  public void emptyFilterTest() {
    final XorFilter filter = new XorFilterBuilder(8, 7L).build();
    assertTrue(filter.isEmpty());
    assertEquals(filter.getNumItems(), 0);

    long positives = 0;
    for (long i = 0; i < 100_000; ++i) {
      if (filter.query(i)) { ++positives; }
    }
    // the empty filter must not report everything present; the rate stays near the 1/256 baseline
    assertTrue(positives < 2000, "Empty filter reported too many positives: " + positives);
  }

  // building with the same seed and items yields an identical serialized image
  @Test
  public void determinismTest() {
    final long seed = 424242L;
    final XorFilterBuilder b1 = new XorFilterBuilder(8, seed);
    final XorFilterBuilder b2 = new XorFilterBuilder(8, seed);
    for (long i = 0; i < 50_000; ++i) {
      b1.update(i);
      b2.update(i);
    }
    assertEquals(b1.build().toByteArray(), b2.build().toByteArray());
  }

  // serialize on the heap then reconstruct with heapify and confirm membership survives
  @Test
  public void heapifyRoundTripTest() {
    final long numItems = 20_000;
    final XorFilterBuilder builder = new XorFilterBuilder(16, 99L);
    for (long i = 0; i < numItems; ++i) {
      builder.update(i);
    }
    final XorFilter filter = builder.build();

    final byte[] bytes = filter.toByteArray();
    assertEquals((long) bytes.length, filter.getSerializedSizeBytes());

    final XorFilter rebuilt = XorFilter.heapify(MemorySegment.ofArray(bytes));
    assertEquals(rebuilt.getBitsPerFingerprint(), filter.getBitsPerFingerprint());
    assertEquals(rebuilt.getNumItems(), filter.getNumItems());
    assertEquals(rebuilt.getSeed(), filter.getSeed());
    for (long i = 0; i < numItems; ++i) {
      assertTrue(rebuilt.query(i));
    }
  }

  // wrap a serialized image in a read-only, off-heap segment and confirm membership
  @Test
  public void wrapRoundTripTest() {
    final long numItems = 20_000;
    final XorFilterBuilder builder = new XorFilterBuilder(8, 1234L);
    for (long i = 0; i < numItems; ++i) {
      builder.update(i);
    }
    final XorFilter filter = builder.build();
    final byte[] bytes = filter.toByteArray();

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment seg = arena.allocate(bytes.length);
      MemorySegment.copy(bytes, 0, seg, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, bytes.length);
      final XorFilter wrapped = XorFilter.wrap(seg);
      assertTrue(wrapped.hasMemorySegment());
      assertTrue(wrapped.isOffHeap());
      for (long i = 0; i < numItems; ++i) {
        assertTrue(wrapped.query(i));
      }
    }
  }

  // toLongArray must be a valid alternate serialization
  @Test
  public void toLongArrayRoundTripTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 17L);
    for (long i = 0; i < 5000; ++i) {
      builder.update(i);
    }
    final XorFilter filter = builder.build();

    final long[] longs = filter.toLongArray();
    final XorFilter rebuilt = XorFilter.heapify(MemorySegment.ofArray(longs));
    for (long i = 0; i < 5000; ++i) {
      assertTrue(rebuilt.query(i));
    }
  }

  // builder must reject invalid fingerprint widths
  @Test
  public void invalidFingerprintBitsTest() {
    assertThrows(SketchesArgumentException.class, () -> new XorFilterBuilder(7, 1L));
    assertThrows(SketchesArgumentException.class, () -> new XorFilterBuilder(0, 1L));
    assertThrows(SketchesArgumentException.class, () -> new XorFilterBuilder(32, 1L));
  }

  // heapify must reject corrupt or foreign images
  @Test
  public void heapifyCorruptionTest() {
    assertThrows(SketchesArgumentException.class,
        () -> XorFilter.heapify(MemorySegment.ofArray(new byte[8])));
  }

  @Test
  public void toStringTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 5L);
    for (long i = 0; i < 1000; ++i) {
      builder.update(i);
    }
    final String s = builder.build().toString();
    assertTrue(s.contains("XorFilter"));
    assertTrue(s.contains("bitsPerFP"));
  }

  // the static create factories treat each array element as a separate item
  @Test
  public void staticCreateTest() {
    final long[] items = new long[5000];
    for (int i = 0; i < items.length; ++i) {
      items[i] = i;
    }

    final XorFilter f1 = XorFilterBuilder.create(items);
    assertEquals(f1.getBitsPerFingerprint(), 8);
    final XorFilter f2 = XorFilterBuilder.create(items, 16);
    assertEquals(f2.getBitsPerFingerprint(), 16);
    final XorFilter f3 = XorFilterBuilder.create(items, 16, 123L);
    for (final long item : items) {
      assertTrue(f1.query(item));
      assertTrue(f2.query(item));
      assertTrue(f3.query(item));
    }

    assertThrows(SketchesArgumentException.class, () -> XorFilterBuilder.create((long[]) null));
  }

  // wrap a 16-bit image off-heap to exercise the JAVA_SHORT_UNALIGNED read path
  @Test
  public void sixteenBitWrapRoundTripTest() {
    final long numItems = 20_000;
    final XorFilterBuilder builder = new XorFilterBuilder(16, 77L);
    for (long i = 0; i < numItems; ++i) {
      builder.update(i);
    }
    final byte[] bytes = builder.build().toByteArray();

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment seg = arena.allocate(bytes.length);
      MemorySegment.copy(bytes, 0, seg, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, bytes.length);
      final XorFilter wrapped = XorFilter.wrap(seg);
      assertEquals(wrapped.getBitsPerFingerprint(), 16);
      assertTrue(wrapped.isOffHeap());
      for (long i = 0; i < numItems; ++i) {
        assertTrue(wrapped.query(i));
      }
    }
  }

  // build and query through the MemorySegment overloads
  @Test
  public void memorySegmentTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 11L);
    for (long i = 0; i < 2000; ++i) {
      builder.update(MemorySegment.ofArray(new long[] { i }));
    }
    final XorFilter filter = builder.build();
    for (long i = 0; i < 2000; ++i) {
      assertTrue(filter.query(MemorySegment.ofArray(new long[] { i })));
    }
  }

  // heap filters carry no segment; wrapped filters are read-only and track their resource
  @Test
  public void readOnlyAndResourceTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 21L);
    for (long i = 0; i < 5000; ++i) {
      builder.update(i);
    }
    final XorFilter heap = builder.build();
    assertFalse(heap.hasMemorySegment());
    assertFalse(heap.isReadOnly());
    assertFalse(heap.isOffHeap());

    final byte[] bytes = heap.toByteArray();
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment seg = arena.allocate(bytes.length);
      MemorySegment.copy(bytes, 0, seg, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, bytes.length);
      final XorFilter wrapped = XorFilter.wrap(seg);
      assertTrue(wrapped.hasMemorySegment());
      assertTrue(wrapped.isReadOnly());
      assertTrue(wrapped.isSameResource(seg));

      final MemorySegment other = arena.allocate(bytes.length);
      assertFalse(wrapped.isSameResource(other));
    }
  }

  // null updates and queries must be safe no-ops
  @Test
  public void nullArgumentsTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 33L);
    builder.update("present");
    builder.update((String) null);
    builder.update((byte[]) null);
    builder.update((char[]) null);
    builder.update((short[]) null);
    builder.update((int[]) null);
    builder.update((long[]) null);
    builder.update((MemorySegment) null);
    builder.update("");
    assertEquals(builder.getNumItems(), 1L);

    final XorFilter filter = builder.build();
    assertFalse(filter.query((String) null));
    assertFalse(filter.query((byte[]) null));
    assertFalse(filter.query((char[]) null));
    assertFalse(filter.query((short[]) null));
    assertFalse(filter.query((int[]) null));
    assertFalse(filter.query((long[]) null));
    assertFalse(filter.query((MemorySegment) null));
    assertFalse(filter.query(""));
    assertTrue(filter.query("present"));
  }

  // the +32 capacity offset must keep construction stable for very small sets
  @Test
  public void smallSetsTest() {
    for (int n = 1; n <= 5; ++n) {
      final XorFilterBuilder builder = new XorFilterBuilder(8, 100L + n);
      for (long i = 0; i < n; ++i) {
        builder.update(i);
      }
      final XorFilter filter = builder.build();
      assertEquals(filter.getNumItems(), n);
      for (long i = 0; i < n; ++i) {
        assertTrue(filter.query(i));
      }
    }
  }

  // the accessors report the expected metadata
  @Test
  public void gettersTest() {
    final int numItems = 10_000;
    final XorFilterBuilder builder = new XorFilterBuilder(8, 2L);
    for (long i = 0; i < numItems; ++i) {
      builder.update(i);
    }
    final XorFilter filter = builder.build();
    assertEquals(filter.getNumHashes(), 3);
    assertEquals(filter.getBitsPerFingerprint(), 8);
    assertEquals(filter.getNumItems(), numItems);
    final long capacity = filter.getCapacity();
    assertEquals(capacity, 3L * (capacity / 3)); // a multiple of the segment count
    assertTrue(capacity >= numItems);            // roughly 1.23 times the item count
    final double bitsPerItem = filter.getBitsPerItem();
    assertTrue((bitsPerItem > 9.5) && (bitsPerItem < 10.5), "unexpected bits/item: " + bitsPerItem);
  }

  // the builder counts every item added while the filter counts only the distinct ones
  @Test
  public void builderNumItemsTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 4L);
    for (int i = 0; i < 1000; ++i) {
      builder.update(i % 100);
    }
    assertEquals(builder.getNumItems(), 1000L);
    assertEquals(builder.build().getNumItems(), 100);
  }

  // 16-bit construction must also be deterministic for a fixed seed and item set
  @Test
  public void determinism16Test() {
    final long seed = 7L;
    final XorFilterBuilder b1 = new XorFilterBuilder(16, seed);
    final XorFilterBuilder b2 = new XorFilterBuilder(16, seed);
    for (long i = 0; i < 20_000; ++i) {
      b1.update(i);
      b2.update(i);
    }
    assertEquals(b1.build().toByteArray(), b2.build().toByteArray());
  }

  // toLongArray must round-trip a 16-bit filter as well
  @Test
  public void toLongArray16RoundTripTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(16, 8L);
    for (long i = 0; i < 5000; ++i) {
      builder.update(i);
    }
    final long[] longs = builder.build().toLongArray();
    final XorFilter rebuilt = XorFilter.heapify(MemorySegment.ofArray(longs));
    assertEquals(rebuilt.getBitsPerFingerprint(), 16);
    for (long i = 0; i < 5000; ++i) {
      assertTrue(rebuilt.query(i));
    }
  }

  // each corrupted preamble field must be rejected on heapify
  @Test
  public void heapifyCorruptionBranchesTest() {
    final XorFilterBuilder builder = new XorFilterBuilder(8, 3L);
    for (long i = 0; i < 1000; ++i) {
      builder.update(i);
    }
    final byte[] valid = builder.build().toByteArray();

    assertThrows(SketchesArgumentException.class, () -> XorFilter.heapify(corrupt(valid, 1, (byte) 99))); // serVer
    assertThrows(SketchesArgumentException.class, () -> XorFilter.heapify(corrupt(valid, 2, (byte) 7)));  // familyID
    assertThrows(SketchesArgumentException.class, () -> XorFilter.heapify(corrupt(valid, 4, (byte) 5)));  // bitsPerFP
    assertThrows(SketchesArgumentException.class, () -> XorFilter.heapify(corrupt(valid, 5, (byte) 4)));  // numHashes
    assertThrows(SketchesArgumentException.class, () -> XorFilter.heapify(MemorySegment.ofArray(new byte[16])));
  }

  // returns a copy of the image with a single byte replaced
  private static MemorySegment corrupt(final byte[] image, final int index, final byte value) {
    final byte[] copy = image.clone();
    copy[index] = value;
    return MemorySegment.ofArray(copy);
  }
}
