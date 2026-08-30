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

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
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

import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class ThetaSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void serializeSketches() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
      for (int i = 0; i < n; i++) {
        sk.update(i);
      }
      putBytesToJavaPath("theta_n" + n + "_java.sk",  sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void serializeCompressedSketches() throws IOException {
    final int[] nArr = {10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
      for (int i = 0; i < n; i++) {
        sk.update(i);
      }
      putBytesToJavaPath("theta_compressed_n" + n + "_java.sk",  sk.compact().toByteArrayCompressed());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void serializeNonEmptyNoEntries() throws IOException {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().setP(0.01f).build();
    sk.update(1); //ignored because of p = .01.
    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 0);
    putBytesToJavaPath("theta_non_empty_no_entries_java.sk",  sk.compact().toByteArray());
  }

  @Test(groups = {CHECK_JAVA_FILES}, priority = 1)
  public void checkJava() {
    deserializeSketchesUsingSegment(GroupLanguage.JAVA);
    deserializeSketchesFromFile(GroupLanguage.JAVA);
    deserializeCompressedUsingSegment(GroupLanguage.JAVA);
    deserializeCompressedFromFile(GroupLanguage.JAVA);
    deserializeNonEmptyNoEntriesUsingSegment(GroupLanguage.JAVA);
    deserializeNonEmptyNoEntriesFromFile(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    deserializeSketchesUsingSegment(GroupLanguage.CPP);
    deserializeSketchesFromFile(GroupLanguage.CPP);
    deserializeCompressedUsingSegment(GroupLanguage.CPP);
    deserializeCompressedFromFile(GroupLanguage.CPP);
    deserializeNonEmptyNoEntriesUsingSegment(GroupLanguage.CPP);
    deserializeNonEmptyNoEntriesFromFile(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    deserializeSketchesUsingSegment(GroupLanguage.GO);
    deserializeSketchesFromFile(GroupLanguage.GO);
    deserializeCompressedUsingSegment(GroupLanguage.GO);
    deserializeCompressedFromFile(GroupLanguage.GO);
    deserializeNonEmptyNoEntriesUsingSegment(GroupLanguage.GO);
    deserializeNonEmptyNoEntriesFromFile(GroupLanguage.GO);
  }

  private static void deserializeSketchesUsingSegment(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final String fileName = "theta_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
      final CompactThetaSketch sketch = CompactThetaSketch.wrap(MemorySegment.ofArray(bytes));
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

  private static void deserializeSketchesFromFile(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final String fileName = "theta_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
      final CompactThetaSketch sketch = CompactThetaSketch.wrap(bytes);
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

  private static void deserializeCompressedUsingSegment(final GroupLanguage lang) {
    final int[] nArr = {10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final String fileName = "theta_compressed_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
      final CompactThetaSketch sketch = CompactThetaSketch.wrap(MemorySegment.ofArray(bytes));
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

  private static void deserializeCompressedFromFile(final GroupLanguage lang) {
    final int[] nArr = {10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final String fileName = "theta_compressed_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
      final CompactThetaSketch sketch = CompactThetaSketch.wrap(bytes);
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

  private static void deserializeNonEmptyNoEntriesUsingSegment(final GroupLanguage lang) {
    final String fileName = "theta_non_empty_no_entries" + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return; }
    //System.out.println(fileName);
    final CompactThetaSketch sketch = CompactThetaSketch.wrap(MemorySegment.ofArray(bytes));
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
  }

  private static void deserializeNonEmptyNoEntriesFromFile(final GroupLanguage lang) {
    final String fileName = "theta_non_empty_no_entries" + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return; }
    //System.out.println(fileName);
    final CompactThetaSketch sketch = CompactThetaSketch.wrap(bytes);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
  }

}
