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

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_HISTORICAL_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.UtilityIO;
import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class AodSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void serializeOneValue() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final ArrayOfDoublesUpdatableSketch sk = new ArrayOfDoublesUpdatableSketchBuilder().build();
      for (int i = 0; i < n; i++) {
        sk.update(i, new double[] {i});
      }
      putBytesToJavaPath("aod_1_n" + n + "_java.sk",  sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void serializeThreeValues() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final ArrayOfDoublesUpdatableSketch sk = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(3).build();
      for (int i = 0; i < n; i++) {
        sk.update(i, new double[] {i, i, i});
      }
      putBytesToJavaPath("aod_3_n" + n + "_java.sk",  sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void serializeNonEmptyNoEntries() throws IOException {
    final ArrayOfDoublesUpdatableSketch sk =
        new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(0.01f).build();
    sk.update(1, new double[] {1});
    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 0);
    putBytesToJavaPath("aod_1_non_empty_no_entries_java.sk",  sk.compact().toByteArray());
  }

  @Test(groups = {CHECK_JAVA_FILES}, priority = 1)
  public void checkJava() {
    deserializeOneValue(GroupLanguage.JAVA);
    deserializeThreeValues(GroupLanguage.JAVA);
    deserializeOneValueNonEmptyNoEntries(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    deserializeOneValue(GroupLanguage.CPP);
    deserializeThreeValues(GroupLanguage.CPP);
    deserializeOneValueNonEmptyNoEntries(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    deserializeOneValue(GroupLanguage.GO);
    deserializeThreeValues(GroupLanguage.GO);
    deserializeOneValueNonEmptyNoEntries(GroupLanguage.GO);
  }

  private static void deserializeOneValue(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final String fileName = "aod_1_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
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

  private static void deserializeThreeValues(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final String fileName = "aod_3_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
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

  private static void deserializeOneValueNonEmptyNoEntries(final GroupLanguage lang) {
    final String fileName = "aod_1_non_empty_no_entries" + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return; }
    //System.err.println(fileName);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketch.wrap(MemorySegment.ofArray(bytes));
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
  }

  @Test(expectedExceptions = RuntimeException.class, groups = {CHECK_CPP_HISTORICAL_FILES})
  public void noSupportHeapifyV0_9_1() throws Exception {
    final byte[] byteArr = UtilityIO.getTestResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk"); //Used Twice
    try {
      ArrayOfDoublesUnion.heapify(MemorySegment.ofArray(byteArr));
    }
    catch (final SketchesArgumentException e) {
      throw new RuntimeException(
        "EXPECTED EXCEPTION: Sketch Type mismatch. Expected ArrayOfDoublesUnion, got ArrayOfDoublesQuickSelectSketch");
    }
  }

  @Test(expectedExceptions = RuntimeException.class, groups = {CHECK_CPP_HISTORICAL_FILES})
  public void noSupportWrapV0_9_1() throws Exception {
    final byte[] byteArr = UtilityIO.getTestResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk");
    try {
      ArrayOfDoublesUnion.wrap(MemorySegment.ofArray(byteArr));
    }
    catch (final SketchesArgumentException e) {
      throw new RuntimeException(
        "EXPECTED EXCEPTION: Sketch Type mismatch. Expected ArrayOfDoublesUnion, got ArrayOfDoublesQuickSelectSketch");
    }
  }

}
