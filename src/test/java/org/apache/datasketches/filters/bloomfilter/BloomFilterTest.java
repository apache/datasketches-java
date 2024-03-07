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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class BloomFilterTest {

  @Test
  public void invalidNumHashesTest() {
    assertThrows(SketchesArgumentException.class, () -> new BloomFilter(1000, -1, 123));
    assertThrows(SketchesArgumentException.class, () -> new BloomFilter(1000, 65536, 123));
  }

  @Test
  public void invalidNumBitsTest() {
    assertThrows(SketchesArgumentException.class, () -> new BloomFilter(0, 3, 456));
    assertThrows(SketchesArgumentException.class, () -> new BloomFilter(BloomFilter.MAX_SIZE + 1, 3, 456));
  }

  @Test
  public void basicFilterOperationsTest() {
    final long numBits = 8192;
    final int numHashes = 3;
    
    final BloomFilter bf = BloomFilterBuilder.createFromSize(numBits, numHashes);
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
    assertTrue(bf.getBitsUsed() <= n * numHashes);
    assertTrue(bf.getBitsUsed() >= n * (numHashes - 1));

    int numFound = 0;
    for (long i = 0; i < 2 * n; ++i) {
      if (bf.query(i))
        ++numFound;
    }
    assertTrue(numFound >= n);
    assertTrue(numFound < 1.1 * n);
  }

  @Test
  public void inversionTest() {
    final long numBits = 8192;
    final int numHashes = 3;
    
    final BloomFilter bf = BloomFilterBuilder.createFromSize(numBits, numHashes);

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
    assertTrue(count < numBits / 10);

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
    final BloomFilter bf1 = BloomFilterBuilder.createFromSize(numBits, numHashes);

    // mismatched num bits
    final BloomFilter bf2 = BloomFilterBuilder.createFromSize(numBits * 2, numHashes, bf1.getSeed());
    assertThrows(SketchesArgumentException.class, () -> bf1.union(bf2));

    // mismatched num hashes
    final BloomFilter bf3 = BloomFilterBuilder.createFromSize(numBits, numHashes * 2, bf1.getSeed());
    assertThrows(SketchesArgumentException.class, () -> bf1.intersect(bf3));

    // mismatched seed
    final BloomFilter bf4 = BloomFilterBuilder.createFromSize(numBits, numHashes, bf1.getSeed() - 1);
    assertThrows(SketchesArgumentException.class, () -> bf1.union(bf4));
  }

  @Test
  public void basicUnionTest() {
    final long numBits = 12288;
    final int numHashes = 4;

    final BloomFilter bf1 = BloomFilterBuilder.createFromSize(numBits, numHashes);
    final BloomFilter bf2 = BloomFilterBuilder.createFromSize(numBits, numHashes, bf1.getSeed());

    final int n = 1000;
    final int maxItem = 3 * n / 2 - 1;
    for (int i = 0; i < n; ++i) {
      bf1.queryAndUpdate(i);
      bf2.queryAndUpdate(n / 2 + i);
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

    assertTrue(count < numBits / 10); // not being super strict
  }

  @Test
  public void basicIntersectionTest() {
    final long numBits = 8192;
    final int numHashes = 5;

    final BloomFilter bf1 = BloomFilterBuilder.createFromSize(numBits, numHashes);
    final BloomFilter bf2 = BloomFilterBuilder.createFromSize(numBits, numHashes, bf1.getSeed());

    final int n = 1024;
    final int maxItem = 3 * n / 2 - 1;
    for (int i = 0; i < n; ++i) {
      bf1.queryAndUpdate(i);
      bf2.queryAndUpdate(n / 2 + i);
    }
    
    bf1.intersect(null); // no-op
    bf1.intersect(bf2);
    for (int i = n / 2; i < n; ++i) {
      assertTrue(bf1.query(i));
    }
    
    int count = 0;
    for (int i = 0; i < n / 2; ++i) {
      count += bf1.query(i) ? 1 : 0;
    }
    for (int i = maxItem; i < numBits; ++i) {
      count += bf1.query(i) ? 1 : 0;
    }

    assertTrue(count < numBits / 10); // not being super strict
  }

  @Test
  public void emptySerializationTest() {
    final long numBits = 32768;
    final int numHashes = 7;
    final BloomFilter bf = BloomFilterBuilder.createFromSize(numBits, numHashes);

    final byte[] bytes = bf.toByteArray();
    Memory mem = Memory.wrap(bytes);
    final BloomFilter fromBytes = BloomFilter.heapify(mem);
    assertTrue(fromBytes.isEmpty());
    assertEquals(fromBytes.getCapacity(), numBits);
    assertEquals(fromBytes.getNumHashes(), numHashes);

    final long[] longs = bf.toLongArray();
    mem = Memory.wrap(longs);
    final BloomFilter fromLongs = BloomFilter.heapify(mem);
    assertTrue(fromLongs.isEmpty());
    assertEquals(fromLongs.getCapacity(), numBits);
    assertEquals(fromLongs.getNumHashes(), numHashes);
  }

  @Test
  public void nonEmptySerializationTest() {
    final long numBits = 32768;
    final int numHashes = 5;
    final BloomFilter bf = BloomFilterBuilder.createFromSize(numBits, numHashes);

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
    Memory mem = Memory.wrap(bytes);
    final BloomFilter fromBytes = BloomFilter.heapify(mem);
    assertFalse(fromBytes.isEmpty());
    assertEquals(fromBytes.getCapacity(), numBits);
    assertEquals(fromBytes.getBitsUsed(), numBitsSet);
    assertEquals(fromBytes.getNumHashes(), numHashes);
    int fromBytesCount = 0;
    for (int i = 0; i < numBits; ++i) {
      boolean val = fromBytes.query(0.5 + i);
      if (val) ++fromBytesCount;
      if (i < n) assertTrue(val);
    }
    assertEquals(fromBytesCount, n + count); // same numbers of items should match

    final long[] longs = bf.toLongArray();
    mem = Memory.wrap(longs);
    final BloomFilter fromLongs = BloomFilter.heapify(mem);
    assertFalse(fromLongs.isEmpty());
    assertEquals(fromLongs.getCapacity(), numBits);
    assertEquals(fromLongs.getBitsUsed(), numBitsSet);
    assertEquals(fromLongs.getNumHashes(), numHashes);
    int fromLongsCount = 0;
    for (int i = 0; i < numBits; ++i) {
      boolean val = fromLongs.query(0.5 + i);
      if (val) ++fromLongsCount;
      if (i < n) assertTrue(val);
    }
    assertEquals(fromLongsCount, n + count); // same numbers of items should match
  }

  @Test
  public void testBasicUpdateMethods() {
    final int numDistinct = 100;
    final double fpp = 1e-6;
    final BloomFilter bf = BloomFilterBuilder.create(numDistinct, fpp);

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
    assertTrue(bf.getBitsUsed() <= bf.getNumHashes() * 6);
    assertFalse(bf.isEmpty());
  }

  @Test
  public void testArrayUpdateMethods() {
    // 3 doubles = 24 bytes
    final double rawData[] = { 1.414, 2.71, 3.1415926538 };
    final Memory mem = Memory.wrap(rawData);

    final int numDistinct = 100;
    final double fpp = 1e-6;

    // for each BloomFilter update type, call update() then queryAndUpdate(), where
    // the latter should return true. query() should likewise return true.
    // A final intersection should have the same number of bits set as the raw input.
    final BloomFilter bfMem = BloomFilterBuilder.create(numDistinct, fpp);
    bfMem.update(mem);
    assertTrue(bfMem.queryAndUpdate(mem));
    assertTrue(bfMem.query(mem));
    final long numBitsSet = bfMem.getBitsUsed();
    final long seed = bfMem.getSeed();

    final BloomFilter bfBytes = BloomFilterBuilder.create(numDistinct, fpp, seed);
    final byte[] bytes = new byte[24];
    mem.getByteArray(0, bytes, 0, 24);
    bfBytes.update(bytes);
    assertTrue(bfBytes.queryAndUpdate(bytes));
    assertTrue(bfBytes.query(bytes));
    assertEquals(bfBytes.getBitsUsed(), numBitsSet);

    final BloomFilter bfChars = BloomFilterBuilder.create(numDistinct, fpp, seed);
    final char[] chars = new char[12];
    mem.getCharArray(0, chars, 0, 12);
    bfChars.update(chars);
    assertTrue(bfChars.queryAndUpdate(chars));
    assertTrue(bfChars.query(chars));
    assertEquals(bfChars.getBitsUsed(), numBitsSet);

    final BloomFilter bfShorts = BloomFilterBuilder.create(numDistinct, fpp, seed);
    final short[] shorts = new short[12];
    mem.getShortArray(0, shorts, 0, 12);
    bfShorts.update(shorts);
    assertTrue(bfShorts.queryAndUpdate(shorts));
    assertTrue(bfShorts.query(shorts));
    assertEquals(bfShorts.getBitsUsed(), numBitsSet);

    final BloomFilter bfInts = BloomFilterBuilder.create(numDistinct, fpp, seed);
    final int[] ints = new int[6];
    mem.getIntArray(0, ints, 0, 6);
    bfInts.update(ints);
    assertTrue(bfInts.queryAndUpdate(ints));
    assertTrue(bfInts.query(ints));
    assertEquals(bfInts.getBitsUsed(), numBitsSet);

    final BloomFilter bfLongs = BloomFilterBuilder.create(numDistinct, fpp, seed);
    final long[] longs = new long[3];
    mem.getLongArray(0, longs, 0, 3);
    bfLongs.update(longs);
    assertTrue(bfLongs.queryAndUpdate(longs));
    assertTrue(bfLongs.query(longs));
    assertEquals(bfLongs.getBitsUsed(), numBitsSet);

    // intersect all the sketches into a new one
    final BloomFilter bf = BloomFilterBuilder.create(numDistinct, fpp, seed);
    bf.intersect(bfMem);
    bf.intersect(bfBytes);
    bf.intersect(bfChars);
    bf.intersect(bfShorts);
    bf.intersect(bfInts);
    bf.intersect(bfLongs);
    assertEquals(bfLongs.getBitsUsed(), numBitsSet);
  }
}
