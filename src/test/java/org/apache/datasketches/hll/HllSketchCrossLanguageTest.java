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

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.UtilityIO.GroupLanguage;
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

  @Test(groups = {CHECK_JAVA_FILES})
  public void checkJava() {
    deserializeHll(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    deserializeHll(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    deserializeHll(GroupLanguage.GO);
  }

  private static void deserializeHll(final GroupLanguage lang) {
    final String[] sArr = {"hll4", "hll6", "hll8"};
    final int[] nArr = {0, 10, 100, 1000, 10000, 100000, 1000000};
    for (final String s: sArr) {
      for (final int n: nArr) {
        final String fileName = s + "_n" + n + lang.sfx + ".sk";
        final byte[] bytes = getFileBytes(lang.pth, fileName);
        if (bytes.length == 0) { continue;}
        //System.out.println(fileName);
        final HllSketch sketch = HllSketch.heapify(MemorySegment.ofArray(bytes));
        assertEquals(sketch.getLgConfigK(), 12);
        assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
        assertEquals(sketch.getEstimate(), n, n * 0.02);
      }
    }
  }

}
