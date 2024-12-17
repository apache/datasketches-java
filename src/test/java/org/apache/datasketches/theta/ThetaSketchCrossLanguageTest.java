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

package org.apache.datasketches.theta;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class ThetaSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final UpdateSketch sk = UpdateSketch.builder().build();
      for (int i = 0; i < n; i++) sk.update(i);
      Files.newOutputStream(javaPath.resolve("theta_n" + n + "_java.sk")).write(sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingCompressed() throws IOException {
    final int[] nArr = {10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final UpdateSketch sk = UpdateSketch.builder().build();
      for (int i = 0; i < n; i++) sk.update(i);
      Files.newOutputStream(javaPath.resolve("theta_compressed_n" + n + "_java.sk")).write(sk.compact().toByteArrayCompressed());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingNonEmptyNoEntries() throws IOException {
    final UpdateSketch sk = UpdateSketch.builder().setP(0.01f).build();
    sk.update(1);
    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 0);
    Files.newOutputStream(javaPath.resolve("theta_non_empty_no_entries_java.sk")).write(sk.compact().toByteArray());
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCpp() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("theta_n" + n + "_cpp.sk"));
      final CompactSketch sketch = CompactSketch.wrap(Memory.wrap(bytes));
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      assertTrue(sketch.isOrdered());
      final HashIterator it = sketch.iterator();
      long previous = 0;
      while (it.next()) {
        assertTrue(it.get() < sketch.getThetaLong());
        assertTrue(it.get() > previous);
        previous = it.get();
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppCompressed() throws IOException {
    final int[] nArr = {10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("theta_compressed_n" + n + "_cpp.sk"));
      final CompactSketch sketch = CompactSketch.wrap(Memory.wrap(bytes));
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      assertTrue(sketch.isOrdered());
      final HashIterator it = sketch.iterator();
      long previous = 0;
      while (it.next()) {
        assertTrue(it.get() < sketch.getThetaLong());
        assertTrue(it.get() > previous);
        previous = it.get();
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppNonEmptyNoEntries() throws IOException {
    final byte[] bytes = Files.readAllBytes(cppPath.resolve("theta_non_empty_no_entries_cpp.sk"));
    final CompactSketch sketch = CompactSketch.wrap(Memory.wrap(bytes));
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
  }

}
