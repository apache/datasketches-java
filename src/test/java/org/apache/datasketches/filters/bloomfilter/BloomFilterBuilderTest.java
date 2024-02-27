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
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

public class BloomFilterBuilderTest {

  @Test
  public void testSuggestHashesFromSizes() {
    // invalid distinct items
    try {
      BloomFilterBuilder.suggestNumHashes(0, 32768);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // invalid filter size
    try {
      BloomFilterBuilder.suggestNumHashes(10_000, -1);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // manually computed values based on formula
    assertEquals(BloomFilterBuilder.suggestNumHashes(100, 1L << 16), 455);
    assertEquals(BloomFilterBuilder.suggestNumHashes(10_000, 1L << 12), 1);
    assertEquals(BloomFilterBuilder.suggestNumHashes(1_000_000_000, Integer.MAX_VALUE * 4L), 6);
    assertEquals(BloomFilterBuilder.suggestNumHashes(1_500_000, 1L << 24), 8);
  }

  @Test
  public void testSuggestHashesFromProbability() {
    // negative probability
    try {
      BloomFilterBuilder.suggestNumHashes(-0.5);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // probability > 1
    try {
      BloomFilterBuilder.suggestNumHashes(2.5);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // manually computed values based on formula
    assertEquals(BloomFilterBuilder.suggestNumHashes(0.333), 2);
    assertEquals(BloomFilterBuilder.suggestNumHashes(0.01), 7);
    assertEquals(BloomFilterBuilder.suggestNumHashes(1e-12), 40);
  }

  @Test
  public void testSuggestNumFilterBits() {
    // non-positive number distincts
    try {
      BloomFilterBuilder.suggestNumFilterBits(0, 0.01);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // non-positive probability
    try {
      BloomFilterBuilder.suggestNumFilterBits(2500, 0.0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // probability > 1
    try {
      BloomFilterBuilder.suggestNumFilterBits(1000, 2.5);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // manually computed values based on formula
    assertEquals(BloomFilterBuilder.suggestNumFilterBits(250_000, 0.01), 2396265);
    BloomFilter bf = BloomFilterBuilder.newBloomFilter(250_000, 0.01);
    assertEquals(bf.getCapacity(), 2396288); // next smallest multiple of 64
    assertEquals(bf.getNumHashes(), BloomFilterBuilder.suggestNumHashes(250_000, 2396288));

    assertEquals(BloomFilterBuilder.suggestNumFilterBits(5_000_000, 1e-4), 95850584);
    final long seed = 19805243;
    bf = BloomFilterBuilder.newBloomFilter(5_000_000, 1e-4, seed);
    assertEquals(bf.getCapacity(), 95850624); // next smallest multiple of 64
    assertEquals(bf.getNumHashes(), BloomFilterBuilder.suggestNumHashes(5_000_000, 95850624));
    assertEquals(bf.getSeed(), seed);
  }
}
