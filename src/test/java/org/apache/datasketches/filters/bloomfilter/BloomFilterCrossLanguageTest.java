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

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class BloomFilterCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void generateBloomFilterBinaries() {
    final int[] nArr = {0, 10_000, 2_000_000, 30_000_000};
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
        putBytesToJavaPath("bf_n" + n + "_h" + numHashes + "_java.sk", bf.toByteArray());
      }
    }
  }

  @Test(groups = {CHECK_JAVA_FILES}, priority = 1)
  public void checkJava() {
    readBloomFilterBinaries(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    readBloomFilterBinaries(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    readBloomFilterBinaries(GroupLanguage.GO);
  }

  private static void readBloomFilterBinaries(final GroupLanguage lang) {
    final int[] nArr = {0, 10_000, 2_000_000, 30_000_000};
    final short[] hArr = {3, 5};
    for (final int n : nArr) {
      for (final short numHashes : hArr) {
        final String fileName = "bf_n" + n + "_h" + numHashes + lang.sfx + ".sk";
        final byte[] bytes = getFileBytes(lang.pth, fileName);
        if (bytes.length == 0) { continue;}
        //System.out.println(fileName);
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
