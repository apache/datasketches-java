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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.CHECK_GO_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.goPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;

import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class CpcSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 100, 200, 2000, 20_000};
    final Flavor[] flavorArr = {Flavor.EMPTY, Flavor.SPARSE, Flavor.HYBRID, Flavor.PINNED, Flavor.SLIDING};
    int flavorIdx = 0;
    for (final int n: nArr) {
      final CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < n; i++) {
        sk.update(i);
      }
      assertEquals(sk.getFlavor(), flavorArr[flavorIdx++]);
      Files.newOutputStream(javaPath.resolve("cpc_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  void negativeIntEquivalence() throws Exception {
    final CpcSketch sk = new CpcSketch();
    final byte v1 = (byte) -1;
    sk.update(v1);
    final short v2 = -1;
    sk.update(v2);
    final int v3 = -1;
    sk.update(v3);
    final long v4 = -1;
    sk.update(v4);
    assertEquals(sk.getEstimate(), 1, 0.01);
    Files.newOutputStream(javaPath.resolve("cpc_negative_one_java.sk")).write(sk.toByteArray());
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void allFlavors() throws IOException {
    final int[] nArr = {0, 100, 200, 2000, 20000};
    final Flavor[] flavorArr = {Flavor.EMPTY, Flavor.SPARSE, Flavor.HYBRID, Flavor.PINNED, Flavor.SLIDING};
    int flavorIdx = 0;
    for (final int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("cpc_n" + n + "_cpp.sk"));
      final CpcSketch sketch = CpcSketch.heapify(MemorySegment.ofArray(bytes));
      assertEquals(sketch.getFlavor(), flavorArr[flavorIdx++]);
      assertEquals(sketch.getEstimate(), n, n * 0.02);
    }
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkAllFlavorsGo() throws IOException {
    final int[] nArr = {0, 100, 200, 2000, 20000};
    final Flavor[] flavorArr = {Flavor.EMPTY, Flavor.SPARSE, Flavor.HYBRID, Flavor.PINNED, Flavor.SLIDING};
    int flavorIdx = 0;
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(goPath.resolve("cpc_n" + n + "_go.sk"));
      final CpcSketch sketch = CpcSketch.heapify(MemorySegment.ofArray(bytes));
      assertEquals(sketch.getFlavor(), flavorArr[flavorIdx++]);
      assertEquals(sketch.getEstimate(), n, n * 0.02);
    }
  }

}
