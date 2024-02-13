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
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class BloomFilterTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidNumHashesTest() {
    new BloomFilter(1000, -1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidNumBitsTest() {
    new BloomFilter(0, 3);
  }

  @Test
  public void basicFilterOperationsTest() {
    final long numBits = 8192;
    final int numHashes = 3;
    
    final BloomFilter bf = new BloomFilter(numBits, numHashes);
    assertTrue(bf.isEmpty());
    assertEquals(bf.getCapacity(), numBits); // n is multiple of 64 so should be exact
    assertEquals(bf.getNumHashes(), numHashes);
    assertEquals(bf.getBitsUsed(), 0);
    
    final long n = 1000;
    for (long i = 0; i < n; ++i) {
      bf.checkAndUpdate(i);
    }

    assertFalse(bf.isEmpty());
    // these next tests assume the filter isn't too close to capacity
    assertTrue(bf.getBitsUsed() <= n * numHashes);
    assertTrue(bf.getBitsUsed() >= n * (numHashes - 1));

    int numFound = 0;
    for (long i = 0; i < 2 * n; ++i) {
      if (bf.check(i))
        ++numFound;
    }
    assertTrue(numFound >= n);
    assertTrue(numFound < 1.1 * n);
  }

  @Test
  public void inversionTest() {
    final long numBits = 8192;
    final int numHashes = 3;
    
    final BloomFilter bf = new BloomFilter(numBits, numHashes);

    final int n = 500;
    for (int i = 0; i < n; ++i) {
      bf.checkAndUpdate(Integer.toString(i));
    }
    
    final long numBitsSet = bf.getBitsUsed();
    bf.invert();
    assertEquals(bf.getBitsUsed(), numBits - numBitsSet);

    // original items should be mostly not-present
    int count = 0;
    for (int i = 0; i < n; ++i) {
      count += bf.check(Integer.toString(i)) ? 1 : 0;
    }
    assertTrue(count < numBits / 10);

    // many other items should be present
    count = 0;
    for (int i = n; i < numBits; ++i) {
      count += bf.check(Integer.toString(i)) ? 1 : 0;
    }
    assertTrue(count > n);

  }

  @Test
  public void basicUnionTest() {
    final long numBits = 12288;
    final int numHashes = 4;

    final BloomFilter bf1 = new BloomFilter(numBits, numHashes);
    final BloomFilter bf2 = new BloomFilter(numBits, numHashes, bf1.getSeed());

    final int n = 1000;
    final int maxItem = 3 * n / 2 - 1;
    for (int i = 0; i < n; ++i) {
      bf1.checkAndUpdate(i);
      bf2.checkAndUpdate(n / 2 + i);
    }
    
    bf1.union(bf2);
    for (int i = 0; i < maxItem; ++i) {
      assertTrue(bf1.check(i));
    }

    int count = 0;
    for (int i = maxItem; i < numBits; ++i) {
      count += bf1.check(i) ? 1 : 0;
    }

    assertTrue(count < numBits / 10); // not being super strict
  }

  @Test
  public void basicIntersectionTest() {
    final long numBits = 8192;
    final int numHashes = 5;

    final BloomFilter bf1 = new BloomFilter(numBits, numHashes);
    final BloomFilter bf2 = new BloomFilter(numBits, numHashes, bf1.getSeed());

    final int n = 1024;
    final int maxItem = 3 * n / 2 - 1;
    for (int i = 0; i < n; ++i) {
      bf1.checkAndUpdate(i);
      bf2.checkAndUpdate(n / 2 + i);
    }
    
    bf1.intersect(bf2);
    for (int i = n / 2; i < n; ++i) {
      assertTrue(bf1.check(i));
    }
    
    int count = 0;
    for (int i = 0; i < n / 2; ++i) {
      count += bf1.check(i) ? 1 : 0;
    }
    for (int i = maxItem; i < numBits; ++i) {
      count += bf1.check(i) ? 1 : 0;
    }

    assertTrue(count < numBits / 10); // not being super strict
  }

  @Test
  public void emptySerializationTest() {
    final long numBits = 32768;
    final int numHashes = 7;
    final BloomFilter bf = new BloomFilter(numBits, numHashes);

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
    final BloomFilter bf = new BloomFilter(numBits, numHashes);

    final int n = 2500;
    for (int i = 0; i < n; ++i) {
      bf.checkAndUpdate(0.5 + i);
    }
    final long numBitsSet = bf.getBitsUsed();

    // test a bunch more items w/o updating
    int count = 0;
    for (int i = n; i < numBits; ++i) {
      count += bf.check(0.5 + i) ? 1 : 0;
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
      boolean val = fromBytes.check(0.5 + i);
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
      boolean val = fromLongs.check(0.5 + i);
      if (val) ++fromLongsCount;
      if (i < n) assertTrue(val);
    }
    assertEquals(fromLongsCount, n + count); // same numbers of items should match
  }
}

