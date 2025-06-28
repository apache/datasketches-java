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

package org.apache.datasketches.tuple.arrayofdoubles;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class AodSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingOneValue() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final ArrayOfDoublesUpdatableSketch sk = new ArrayOfDoublesUpdatableSketchBuilder().build();
      for (int i = 0; i < n; i++) {
        sk.update(i, new double[] {i});
      }
      Files.newOutputStream(javaPath.resolve("aod_1_n" + n + "_java.sk")).write(sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingThreeValues() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final ArrayOfDoublesUpdatableSketch sk = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(3).build();
      for (int i = 0; i < n; i++) {
        sk.update(i, new double[] {i, i, i});
      }
      Files.newOutputStream(javaPath.resolve("aod_3_n" + n + "_java.sk")).write(sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingNonEmptyNoEntries() throws IOException {
    final ArrayOfDoublesUpdatableSketch sk =
        new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(0.01f).build();
    sk.update(1, new double[] {1});
    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 0);
    Files.newOutputStream(javaPath.resolve("aod_1_non_empty_no_entries_java.sk")).write(sk.compact().toByteArray());
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppOneValue() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("aod_1_n" + n + "_cpp.sk"));
      final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketch.wrap(MemorySegment.ofArray(bytes));
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      assertEquals(sketch.getNumValues(), 1);
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      while (it.next()) {
        assertTrue(it.getKey() < sketch.getThetaLong());
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppThreeValues() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("aod_3_n" + n + "_cpp.sk"));
      final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketch.wrap(MemorySegment.ofArray(bytes));
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      assertEquals(sketch.getNumValues(), 3);
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      while (it.next()) {
        assertTrue(it.getKey() < sketch.getThetaLong());
        assertEquals(it.getValues()[0], it.getValues()[1]);
        assertEquals(it.getValues()[0], it.getValues()[2]);
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppOneValueNonEmptyNoEntries() throws IOException {
    final byte[] bytes = Files.readAllBytes(cppPath.resolve("aod_1_non_empty_no_entries_cpp.sk"));
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketch.wrap(MemorySegment.ofArray(bytes));
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
  }

}
