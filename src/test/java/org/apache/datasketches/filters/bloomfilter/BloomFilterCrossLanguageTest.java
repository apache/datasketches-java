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

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class BloomFilterCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generatBloomFilterBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 10_000, 2_000_000, 300_000_00};
    final short[] hArr = {3, 5};
    for (final int n : nArr) {
      for (final short numHashes : hArr) {
        final long configBits = Math.max(n, 1000L); // so empty still has valid bit size
        final BloomFilter bf = BloomFilterBuilder.createBySize(configBits, numHashes);
        for (int i = 0; i < (n / 10); ++i) {
          bf.update(i);
        }
        if (n > 0) { bf.update(Float.NaN); }
        assertEquals(bf.isEmpty(), n == 0);
        assertTrue(bf.isEmpty() || (bf.getBitsUsed() > (n / 10)));
        Files.newOutputStream(javaPath.resolve("bf_n" + n + "_h" + numHashes + "_java.sk")).write(bf.toByteArray());
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void readBloomFilterBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 10_000, 2_000_000, 300_000_00};
    final short[] hArr = {3, 5};
    for (final int n : nArr) {
      for (final short numHashes : hArr) {
        final byte[] bytes = Files.readAllBytes(cppPath.resolve("bf_n" + n + "_h" + numHashes + "_cpp.sk"));
        final BloomFilter bf = BloomFilter.heapify(MemorySegment.ofArray(bytes));
        assertEquals(bf.isEmpty(), n == 0);
        assertTrue(bf.isEmpty() || (bf.getBitsUsed() > (n / 10)));

        for (int i = 0; i < (n / 10); ++i) {
          assertTrue(bf.query(i));
        }
        if (n > 0) {
          assert(bf.query(Double.NaN));
        }
      }
    }
  }
}
