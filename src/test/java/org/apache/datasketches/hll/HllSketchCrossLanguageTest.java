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

package org.apache.datasketches.hll;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.getFileBytes;
import static org.apache.datasketches.common.TestUtil.putBytesToJavaPath;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class HllSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final HllSketch hll4 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_4);
      final HllSketch hll6 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_6);
      final HllSketch hll8 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_8);
      for (int i = 0; i < n; i++) {
        hll4.update(i);
      }
      for (int i = 0; i < n; i++) {
        hll6.update(i);
      }
      for (int i = 0; i < n; i++) {
        hll8.update(i);
      }
      putBytesToJavaPath("hll4_n" + n + "_java.sk", hll4.toCompactByteArray());
      putBytesToJavaPath("hll6_n" + n + "_java.sk", hll6.toCompactByteArray());
      putBytesToJavaPath("hll8_n" + n + "_java.sk", hll8.toCompactByteArray());
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void hll4() throws IOException {
    final int[] nArr = {0, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final byte[] bytes = getFileBytes(cppPath, "hll4_n" + n + "_cpp.sk");
      final HllSketch sketch = HllSketch.heapify(MemorySegment.ofArray(bytes));
      assertEquals(sketch.getLgConfigK(), 12);
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.02);
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void hll6() throws IOException {
    final int[] nArr = {0, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final byte[] bytes = getFileBytes(cppPath, "hll6_n" + n + "_cpp.sk");
      final HllSketch sketch = HllSketch.heapify(MemorySegment.ofArray(bytes));
      assertEquals(sketch.getLgConfigK(), 12);
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.02);
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void hll8() throws IOException {
    final int[] nArr = {0, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final byte[] bytes = getFileBytes(cppPath, "hll8_n" + n + "_cpp.sk");
      final HllSketch sketch = HllSketch.heapify(MemorySegment.ofArray(bytes));
      assertEquals(sketch.getLgConfigK(), 12);
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.02);
    }
  }

}
