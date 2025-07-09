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

package org.apache.datasketches.filters.bloomfilter;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_CHAR_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;
import org.testng.annotations.Test;

import java.lang.foreign.Arena;

public class BloomFilterTest {

  @Test
  public void createNewFilterTest() throws Exception {
    // construct the same filter multiple ways
    final long numItems = 4000;
    final double targetFpp = 0.01;

    final long numBits = BloomFilterBuilder.suggestNumFilterBits(numItems, targetFpp);
    final short numHashes = BloomFilterBuilder.suggestNumHashes(numItems, numBits);
    final int sizeBytes = (int) BloomFilterBuilder.getSerializedFilterSize(numBits);
    final long seed = 89023L;

    final BloomFilter bf1 = new BloomFilter(numBits, numHashes, seed);
    assertTrue(bf1.isEmpty());
    assertFalse(bf1.hasMemorySegment());
    assertFalse(bf1.isOffHeap());
    assertFalse(bf1.isReadOnly());


    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = arena.allocate(sizeBytes);
      final BloomFilter bf2 = new BloomFilter(numBits, numHashes, seed, wseg);
      assertTrue(bf2.isEmpty());
      assertTrue(bf2.hasMemorySegment());
      assertTrue(bf2.isOffHeap());
      assertFalse(bf2.isReadOnly());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void tooSmallMemorySegmentTest() {
    new BloomFilter(65536, 4, 1L, MemorySegment.ofArray(new byte[32]));
  }

  @Test
  public void wrapEmptyFilterTest() {
    final long numBits = 8192;
    final int numHashes = 4;
    final BloomFilter bf = BloomFilterBuilder.createBySize(numBits, numHashes);
    assertTrue(bf.isEmpty());
    assertEquals(bf.getBitsUsed(), 0);
    assertEquals(bf.getCapacity(), numBits);
    assertEquals(bf.getNumHashes(), numHashes);

    final MemorySegment seg = MemorySegment.ofArray(bf.toByteArray());
    assertEquals(bf.getSerializedSizeBytes(), seg.byteSize());

    final MemorySegment wseg = MemorySegment.ofArray(bf.toByteArray());
    assertEquals(bf.getSerializedSizeBytes(), wseg.byteSize());

    // read-only
    final BloomFilter bfReadOnly = BloomFilter.wrap(seg);
    assertTrue(bfReadOnly.isEmpty());
    assertEquals(bfReadOnly.getBitsUsed(), 0);
    assertEquals(bfReadOnly.getCapacity(), numBits);
    assertEquals(bfReadOnly.getNumHashes(), numHashes);
    assertTrue(bfReadOnly.isReadOnly());

    // writable should fail
    assertThrows(SketchesArgumentException.class, () -> BloomFilter.writableWrap(wseg));
  }

  @Test
  public void wrapNonEmptyFilterTest() {
    final long numBits = 8192;
    final int numHashes = 4;
    final long seed = 52483905L;
    final BloomFilter bf = BloomFilterBuilder.createBySize(numBits, numHashes, seed);

    final long valStart = 1024;
    final long valEnd = 2048;
    for (long i = valStart; i < valEnd; ++i) {
      bf.update(i);
    }

    final MemorySegment wseg = MemorySegment.ofArray(bf.toByteArray());
    assertEquals(bf.getSerializedSizeBytes(), wseg.byteSize());

    // writable
    final BloomFilter bfWritable = BloomFilter.writableWrap(wseg);
    assertFalse(bfWritable.isEmpty());
    assertEquals(bfWritable.getBitsUsed(), bf.getBitsUsed());
    assertEquals(bfWritable.getCapacity(), bf.getCapacity());
    assertEquals(bfWritable.getNumHashes(), bf.getNumHashes());
    assertFalse(bfWritable.isReadOnly());

    // read-only, wrapping the same writable MemorySegment
    final BloomFilter bfReadOnly = BloomFilter.wrap(wseg);
    assertFalse(bfReadOnly.isEmpty());
    assertEquals(bfReadOnly.getBitsUsed(), bf.getBitsUsed());
    assertEquals(bfReadOnly.getCapacity(), bf.getCapacity());
    assertEquals(bfReadOnly.getNumHashes(), bf.getNumHashes());
    assertTrue(bfReadOnly.isReadOnly());

    // update writable, should be reflected in read-only because
    // they use the same underling MemorySegment
    assertFalse(bfWritable.queryAndUpdate(32768));
    assertTrue(bfReadOnly.query(32768));

    assertThrows(SketchesReadOnlyException.class, () -> bfReadOnly.update(99999));
  }

  @Test
  public void basicFilterOperationsTest() {
    final long numBits = 8192;
    final int numHashes = 3;

    final BloomFilter bf = BloomFilterBuilder.createBySize(numBits, numHashes);
    assertTrue(bf.isEmpty());
    assertEquals(bf.getCapacity(), numBits); // n is multiple of 64 so should be exact
    assertEquals(bf.getNumHashes(), numHashes);
    assertEquals(bf.getBitsUsed(), 0);

    final long n = 1000;
    for (long i = 0; i < n; ++i) {
      bf.queryAndUpdate(i);
    }

    assertFalse(bf.isEmpty());
    // these next tests assume the filter isn't too close to capacity
    assertTrue(bf.getBitsUsed() <= (n * numHashes));
    assertTrue(bf.getBitsUsed() >= (n * (numHashes - 1)));

    int numFound = 0;
    for (long i = 0; i < (2 * n); ++i) {
      if (bf.query(i)) {
        ++numFound;
      }
    }
    assertTrue(numFound >= n);
    assertTrue(numFound < (1.1 * n));

    assert(String.valueOf(bf).length() > 0);

    bf.reset();
    assertTrue(bf.isEmpty());
    assertEquals(bf.getCapacity(), numBits); // n is multiple of 64 so should be exact
    assertEquals(bf.getNumHashes(), numHashes);
    assertEquals(bf.getBitsUsed(), 0);
  }

  @Test
  public void inversionTest() {
    final long numBits = 8192;
    final int numHashes = 3;

    final BloomFilter bf = BloomFilterBuilder.createBySize(numBits, numHashes);

    final int n = 500;
    for (int i = 0; i < n; ++i) {
      bf.queryAndUpdate(Integer.toString(i));
    }

    final long numBitsSet = bf.getBitsUsed();
    bf.invert();
    assertEquals(bf.getBitsUsed(), numBits - numBitsSet);

    // original items should be mostly not-present
    int count = 0;
    for (int i = 0; i < n; ++i) {
      count += bf.query(Integer.toString(i)) ? 1 : 0;
    }
    assertTrue(count < (numBits / 10));

    // many other items should be present
    count = 0;
    for (int i = n; i < numBits; ++i) {
      count += bf.query(Integer.toString(i)) ? 1 : 0;
    }
    assertTrue(count > n);
  }

  @Test
  public void incompatibleSetOperationsTest() {
    final int numBits = 128;
    final int numHashes = 4;
    final BloomFilter bf1 = BloomFilterBuilder.createBySize(numBits, numHashes);

    // mismatched num bits
    final BloomFilter bf2 = BloomFilterBuilder.createBySize(numBits * 2, numHashes, bf1.getSeed());
    assertThrows(SketchesArgumentException.class, () -> bf1.union(bf2));

    // mismatched num hashes
    final BloomFilter bf3 = BloomFilterBuilder.createBySize(numBits, numHashes * 2, bf1.getSeed());
    assertThrows(SketchesArgumentException.class, () -> bf1.intersect(bf3));

    // mismatched seed
    final BloomFilter bf4 = BloomFilterBuilder.createBySize(numBits, numHashes, bf1.getSeed() - 1);
    assertThrows(SketchesArgumentException.class, () -> bf1.union(bf4));
  }

  @Test
  public void basicUnionTest() {
    final long numBits = 12288;
    final int numHashes = 4;

    final BloomFilter bf1 = BloomFilterBuilder.createBySize(numBits, numHashes);
    final BloomFilter bf2 = BloomFilterBuilder.createBySize(numBits, numHashes, bf1.getSeed());

    final int n = 1000;
    final int maxItem = ((3 * n) / 2) - 1;
    for (int i = 0; i < n; ++i) {
      bf1.queryAndUpdate(i);
      bf2.queryAndUpdate((n / 2) + i);
    }

    bf1.union(null); // no-op
    bf1.union(bf2);
    for (int i = 0; i < maxItem; ++i) {
      assertTrue(bf1.query(i));
    }

    int count = 0;
    for (int i = maxItem; i < numBits; ++i) {
      count += bf1.query(i) ? 1 : 0;
    }

    assertTrue(count < (numBits / 10)); // not being super strict
  }

  @Test
  public void basicIntersectionTest() {
    final long numBits = 8192;
    final int numHashes = 5;

    final BloomFilter bf1 = BloomFilterBuilder.createBySize(numBits, numHashes);
    final BloomFilter bf2 = BloomFilterBuilder.createBySize(numBits, numHashes, bf1.getSeed());

    final int n = 1024;
    final int maxItem = ((3 * n) / 2) - 1;
    for (int i = 0; i < n; ++i) {
      bf1.queryAndUpdate(i);
      bf2.queryAndUpdate((n / 2) + i);
    }

    bf1.intersect(null); // no-op
    bf1.intersect(bf2);
    for (int i = n / 2; i < n; ++i) {
      assertTrue(bf1.query(i));
    }

    int count = 0;
    for (int i = 0; i < (n / 2); ++i) {
      count += bf1.query(i) ? 1 : 0;
    }
    for (int i = maxItem; i < numBits; ++i) {
      count += bf1.query(i) ? 1 : 0;
    }

    assertTrue(count < (numBits / 10)); // not being super strict
  }

  @Test
  public void emptySerializationTest() {
    final long numBits = 32768;
    final int numHashes = 7;
    final BloomFilter bf = BloomFilterBuilder.createBySize(numBits, numHashes);

    final byte[] bytes = bf.toByteArray();
    final MemorySegment segBytes = MemorySegment.ofArray(bytes);
    final BloomFilter fromBytes = BloomFilter.heapify(segBytes);
    assertTrue(fromBytes.isEmpty());
    assertEquals(fromBytes.getCapacity(), numBits);
    assertEquals(fromBytes.getNumHashes(), numHashes);

    final long[] longs = bf.toLongArray();
    final MemorySegment segLongs = MemorySegment.ofArray(longs);
    final BloomFilter fromLongs = BloomFilter.heapify(segLongs);
    assertTrue(fromLongs.isEmpty());
    assertEquals(fromLongs.getCapacity(), numBits);
    assertEquals(fromLongs.getNumHashes(), numHashes);

    // compare segments
    assertEquals(segBytes.byteSize(), segLongs.byteSize());
    for (long i = 0; i < segBytes.byteSize(); ++i) {
      assertEquals(segBytes.get(JAVA_BYTE, i), segLongs.get(JAVA_BYTE, i));
    }
  }

  @Test
  public void nonEmptySerializationTest() {
    final long numBits = 32768;
    final int numHashes = 5;
    final BloomFilter bf = BloomFilterBuilder.createBySize(numBits, numHashes);

    final int n = 2500;
    for (int i = 0; i < n; ++i) {
      bf.queryAndUpdate(0.5 + i);
    }
    final long numBitsSet = bf.getBitsUsed();

    // test a bunch more items w/o updating
    int count = 0;
    for (int i = n; i < numBits; ++i) {
      count += bf.query(0.5 + i) ? 1 : 0;
    }

    final byte[] bytes = bf.toByteArray();
    final MemorySegment segBytes = MemorySegment.ofArray(bytes);
    final BloomFilter fromBytes = BloomFilter.heapify(segBytes);
    assertFalse(fromBytes.isEmpty());
    assertEquals(fromBytes.getCapacity(), numBits);
    assertEquals(fromBytes.getBitsUsed(), numBitsSet);
    assertEquals(fromBytes.getNumHashes(), numHashes);
    int fromBytesCount = 0;
    for (int i = 0; i < numBits; ++i) {
      final boolean val = fromBytes.query(0.5 + i);
      if (val) {
        ++fromBytesCount;
      }
      if (i < n) {
        assertTrue(val);
      }
    }
    assertEquals(fromBytesCount, n + count); // same numbers of items should match

    final long[] longs = bf.toLongArray();
    final MemorySegment segLongs = MemorySegment.ofArray(longs);
    final BloomFilter fromLongs = BloomFilter.heapify(segLongs);
    assertFalse(fromLongs.isEmpty());
    assertEquals(fromLongs.getCapacity(), numBits);
    assertEquals(fromLongs.getBitsUsed(), numBitsSet);
    assertEquals(fromLongs.getNumHashes(), numHashes);
    int fromLongsCount = 0;
    for (int i = 0; i < numBits; ++i) {
      final boolean val = fromLongs.query(0.5 + i);
      if (val) {
        ++fromLongsCount;
      }
      if (i < n) {
        assertTrue(val);
      }
    }
    assertEquals(fromLongsCount, n + count); // same numbers of items should match

    assertEquals(segBytes.byteSize(), segLongs.byteSize());
    for (long i = 0; i < segBytes.byteSize(); ++i) {
      assertEquals(segBytes.get(JAVA_BYTE, i), segLongs.get(JAVA_BYTE, i));
    }
  }

  @Test
  public void heapifyFromDirectTest() {
    final long numDistinct = 10000;
    final double targetFpp = 0.001;
    final int numBytes = (int) BloomFilterBuilder.getSerializedFilterSizeByAccuracy(numDistinct, targetFpp);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[numBytes]);
    final BloomFilter bf = BloomFilterBuilder.initializeByAccuracy(numDistinct, targetFpp, 89540235L, wseg);

    byte[] bytes = bf.toByteArray();
    assertEquals(bytes.length, Family.BLOOMFILTER.getMinPreLongs() * Long.BYTES);
    BloomFilter bfBytes = BloomFilter.heapify(MemorySegment.ofArray(bytes));
    assertTrue(bfBytes.isEmpty());
    assertEquals(bfBytes.getNumHashes(), bf.getNumHashes());
    assertEquals(bfBytes.getBitsUsed(), 0);
    assertEquals(bfBytes.getCapacity(), bf.getCapacity()); // even though empty

    long[] longs = bf.toLongArray();
    assertEquals(longs.length, Family.BLOOMFILTER.getMinPreLongs());
    BloomFilter bfLongs = BloomFilter.heapify(MemorySegment.ofArray(longs));
    assertTrue(bfLongs.isEmpty());
    assertEquals(bfLongs.getNumHashes(), bf.getNumHashes());
    assertEquals(bfLongs.getBitsUsed(), 0);
    assertEquals(bfLongs.getCapacity(), bf.getCapacity());

    // add some items now
    for (int i = 0; i < 10000; ++i) {
      bf.queryAndUpdate(Integer.toString(i));
    }

    bytes = bf.toByteArray();
    assertEquals(bytes.length, bf.getSerializedSizeBytes());
    bfBytes = BloomFilter.heapify(MemorySegment.ofArray(bytes));
    assertFalse(bfBytes.isEmpty());
    assertEquals(bfBytes.getNumHashes(), bf.getNumHashes());
    assertEquals(bfBytes.getBitsUsed(), bf.getBitsUsed());
    assertEquals(bfBytes.getCapacity(), bf.getCapacity());
    assertTrue(bfBytes.query(Integer.toString(5000)));
    assertFalse(bfBytes.query(Integer.toString(50000)));

    longs = bf.toLongArray();
    assertEquals(longs.length, bf.getSerializedSizeBytes() / Long.BYTES);
    bfLongs = BloomFilter.heapify(MemorySegment.ofArray(longs));
    assertFalse(bfLongs.isEmpty());
    assertEquals(bfLongs.getNumHashes(), bf.getNumHashes());
    assertEquals(bfLongs.getBitsUsed(), bf.getBitsUsed());
    assertEquals(bfLongs.getCapacity(), bf.getCapacity());
    assertTrue(bfBytes.query(Integer.toString(7500)));
    assertFalse(bfBytes.query(Integer.toString(75000)));
  }

  @Test
  public void testBasicUpdateMethods() {
    final int numDistinct = 100;
    final double fpp = 1e-6;
    final BloomFilter bf = BloomFilterBuilder.createByAccuracy(numDistinct, fpp);

    // empty/null String should do nothing
    bf.update("");
    bf.update((String) null);
    assertFalse(bf.queryAndUpdate(""));
    assertFalse(bf.queryAndUpdate((String) null));
    assertEquals(bf.getBitsUsed(), 0);

    bf.update("abc");
    assertFalse(bf.queryAndUpdate("def"));
    bf.update(932);
    assertFalse(bf.queryAndUpdate(543));
    bf.update(Double.NaN);
    assertFalse(bf.queryAndUpdate(Double.POSITIVE_INFINITY));
    assertTrue(bf.getBitsUsed() <= (bf.getNumHashes() * 6));
    assertFalse(bf.isEmpty());
  }

  @Test
  public void testArrayUpdateMethods() {
    // 3 doubles = 24 bytes
    final double rawData[] = { 1.414, 2.71, 3.1415926538 };
    final MemorySegment seg = MemorySegment.ofArray(rawData);

    final int numDistinct = 100;
    final double fpp = 1e-6;

    // for each BloomFilter update type, call update() then queryAndUpdate(), where
    // the latter should return true. query() should likewise return true.
    // A final intersection should have the same number of bits set as the raw input.
    final BloomFilter bfSeg = BloomFilterBuilder.createByAccuracy(numDistinct, fpp);
    bfSeg.update(seg);
    assertTrue(bfSeg.queryAndUpdate(seg));
    assertTrue(bfSeg.query(seg));
    final long numBitsSet = bfSeg.getBitsUsed();
    final long seed = bfSeg.getSeed();

    final BloomFilter bfBytes = BloomFilterBuilder.createByAccuracy(numDistinct, fpp, seed);
    final byte[] bytes = seg.toArray(JAVA_BYTE);
    bfBytes.update(bytes);
    assertTrue(bfBytes.queryAndUpdate(bytes));
    assertTrue(bfBytes.query(bytes));
    assertEquals(bfBytes.getBitsUsed(), numBitsSet);

    final BloomFilter bfChars = BloomFilterBuilder.createByAccuracy(numDistinct, fpp, seed);
    final char[] chars = seg.toArray(JAVA_CHAR_UNALIGNED);
    bfChars.update(chars);
    assertTrue(bfChars.queryAndUpdate(chars));
    assertTrue(bfChars.query(chars));
    assertEquals(bfChars.getBitsUsed(), numBitsSet);

    final BloomFilter bfShorts = BloomFilterBuilder.createByAccuracy(numDistinct, fpp, seed);
    final short[] shorts = seg.toArray(JAVA_SHORT_UNALIGNED);
    bfShorts.update(shorts);
    assertTrue(bfShorts.queryAndUpdate(shorts));
    assertTrue(bfShorts.query(shorts));
    assertEquals(bfShorts.getBitsUsed(), numBitsSet);

    final BloomFilter bfInts = BloomFilterBuilder.createByAccuracy(numDistinct, fpp, seed);
    final int[] ints = seg.toArray(JAVA_INT_UNALIGNED);
    bfInts.update(ints);
    assertTrue(bfInts.queryAndUpdate(ints));
    assertTrue(bfInts.query(ints));
    assertEquals(bfInts.getBitsUsed(), numBitsSet);

    final BloomFilter bfLongs = BloomFilterBuilder.createByAccuracy(numDistinct, fpp, seed);
    final long[] longs = seg.toArray(JAVA_LONG_UNALIGNED);
    bfLongs.update(longs);
    assertTrue(bfLongs.queryAndUpdate(longs));
    assertTrue(bfLongs.query(longs));
    assertEquals(bfLongs.getBitsUsed(), numBitsSet);

    // intersect all the sketches into a new one
    final BloomFilter bf = BloomFilterBuilder.createByAccuracy(numDistinct, fpp, seed);
    bf.intersect(bfSeg);
    bf.intersect(bfBytes);
    bf.intersect(bfChars);
    bf.intersect(bfShorts);
    bf.intersect(bfInts);
    bf.intersect(bfLongs);
    assertEquals(bfLongs.getBitsUsed(), numBitsSet);
  }
}
