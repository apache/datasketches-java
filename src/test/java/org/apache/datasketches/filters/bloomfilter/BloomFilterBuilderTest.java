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
import static org.testng.Assert.assertThrows;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;
import org.testng.annotations.Test;

public class BloomFilterBuilderTest {

  @Test
  public void testSuggestHashesFromSizes() {
    // invalid distinct items
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.suggestNumHashes(0, 32768));

    // invalid filter size
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.suggestNumHashes(10_000, -1));

    // manually computed values based on formula
    assertEquals(BloomFilterBuilder.suggestNumHashes(100, 1L << 16), 455);
    assertEquals(BloomFilterBuilder.suggestNumHashes(10_000, 1L << 12), 1);
    assertEquals(BloomFilterBuilder.suggestNumHashes(1_000_000_000, Integer.MAX_VALUE * 4L), 6);
    assertEquals(BloomFilterBuilder.suggestNumHashes(1_500_000, 1L << 24), 8);
  }

  @Test
  public void testSuggestHashesFromProbability() {
    // negative probability
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.suggestNumHashes(-0.5));

    // probability > 1
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.suggestNumHashes(2.5));

    // manually computed values based on formula
    assertEquals(BloomFilterBuilder.suggestNumHashes(0.333), 2);
    assertEquals(BloomFilterBuilder.suggestNumHashes(0.01), 7);
    assertEquals(BloomFilterBuilder.suggestNumHashes(1e-12), 40);
  }

  @Test
  public void testCreateFromSize() {
    // invalid number of hashes
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createBySize(1000, -1, 123));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createBySize(1000, 65536, 123));

    // invalid number of bits
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createBySize(0, 3, 456));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createBySize(BloomFilter.MAX_SIZE_BITS + 1, 3, 456));

    final BloomFilter bf = BloomFilterBuilder.createBySize(1L << 21, 3);
    assertEquals(bf.getCapacity(), 1 << 21L);
    assertEquals(bf.getNumHashes(), 3);
    assertEquals(bf.getBitsUsed(), 0);
  }

  @Test
  public void testCreateFromAccuracy() {
    // invalid number of distinct items
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createByAccuracy(-1, 0.01));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createByAccuracy(1L << 40, 0.01));

    // invalid probabilities
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createByAccuracy(20000, -0.5));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.createByAccuracy(20000, 2.0));

    final long numDistinct = 30000;
    final double fpp = 0.001;
    final BloomFilter bf = BloomFilterBuilder.createByAccuracy(numDistinct, fpp);
    // filter rounds size up to nearest multiple of 64
    assertEquals(bf.getCapacity(), (long) Math.ceil(BloomFilterBuilder.suggestNumFilterBits(numDistinct, fpp) / 64.0) * 64);
    assertEquals(bf.getNumHashes(), BloomFilterBuilder.suggestNumHashes(fpp));
  }

  @Test
  public void testInitializeFromSize() {
    final long numBits = 50000;
    final short numHashes = 7;
    final long numBytes = BloomFilterBuilder.getSerializedFilterSize(numBits);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[(int) numBytes]);

    // invalid number of bits
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeBySize(-1, numHashes, wseg));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeBySize(1L << 40, numHashes, wseg));

    // invalid number of hashes
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeBySize(numBits, -3, wseg));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeBySize(numBits, 100_000, wseg));

    // MemorySegment too small
    assertThrows(SketchesArgumentException.class, ()
        -> BloomFilterBuilder.initializeBySize(numBits, numHashes, MemorySegment.ofArray(new byte[32])));

    final BloomFilter bf = BloomFilterBuilder.initializeBySize(numBits, numHashes, wseg);
    // filter rounds size up to nearest multiple of 64
    assertEquals(bf.getCapacity(), (long) Math.ceil(numBits / 64.0) * 64);
    assertEquals(bf.getNumHashes(), numHashes);
  }

  @Test
  public void testInitializeFromAccuracy() {
    final long numDistinct = 30000;
    final double fpp = 0.001;
    final long numBytes = BloomFilterBuilder.getSerializedFilterSizeByAccuracy(numDistinct, fpp);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[(int) numBytes]);

    // invalid number of distinct items
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeByAccuracy(-1, fpp, wseg));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeByAccuracy(1L << 40, fpp, wseg));

    // invalid probabilities
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeByAccuracy(numDistinct, -0.5, wseg));
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.initializeByAccuracy(numDistinct, 2.0, wseg));

    // MemorySegment too small
    assertThrows(SketchesArgumentException.class, ()
        -> BloomFilterBuilder.initializeByAccuracy(numDistinct, fpp, MemorySegment.ofArray(new byte[32])));

    final BloomFilter bf = BloomFilterBuilder.initializeByAccuracy(numDistinct, fpp, wseg);
    // filter rounds size up to nearest multiple of 64
    assertEquals(bf.getCapacity(), (long) Math.ceil(BloomFilterBuilder.suggestNumFilterBits(numDistinct, fpp) / 64.0) * 64);
    assertEquals(bf.getNumHashes(), BloomFilterBuilder.suggestNumHashes(fpp));
  }

  @Test
  public void testSuggestNumFilterBits() {
    // non-positive number distincts
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.suggestNumFilterBits(0, 0.01));

    // non-positive probability
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.suggestNumFilterBits(2500, 0.0));

    // probability > 1
    assertThrows(SketchesArgumentException.class, () -> BloomFilterBuilder.suggestNumFilterBits(1000, 2.5));

    // manually computed values based on formula
    assertEquals(BloomFilterBuilder.suggestNumFilterBits(250_000, 0.01), 2396265);
    BloomFilter bf = BloomFilterBuilder.createByAccuracy(250_000, 0.01);
    assertEquals(bf.getCapacity(), 2396288); // next smallest multiple of 64
    assertEquals(bf.getNumHashes(), BloomFilterBuilder.suggestNumHashes(250_000, 2396288));

    assertEquals(BloomFilterBuilder.suggestNumFilterBits(5_000_000, 1e-4), 95850584);
    final long seed = 19805243;
    bf = BloomFilterBuilder.createByAccuracy(5_000_000, 1e-4, seed);
    assertEquals(bf.getCapacity(), 95850624); // next smallest multiple of 64
    assertEquals(bf.getNumHashes(), BloomFilterBuilder.suggestNumHashes(5_000_000, 95850624));
    assertEquals(bf.getSeed(), seed);
  }
}
