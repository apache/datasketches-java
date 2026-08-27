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

package org.apache.datasketches.tuple.strings;

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by other language code.
 * Test deserialization of binary sketches serialized by other language code.
 */
public class AosSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void serializeOneString() throws IOException {
    int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();
      for (int i = 0; i < n; i++) {
        sk.update(new String[] {String.valueOf(i)}, new String[] {"value" + i});
      }
      putBytesToJavaPath("aos_1_n" + n + "_java.sk", sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void serializeThreeStrings() throws IOException {
    int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();
      for (int i = 0; i < n; i++) {
        sk.update(new String[] {String.valueOf(i)}, new String[] {"a" + i, "b" + i, "c" + i});
      }
      putBytesToJavaPath("aos_3_n" + n + "_java.sk", sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void serializeOneStringNonEmptyNoEntries() throws IOException {
    ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch(12,
        ResizeFactor.X8, 0.01f);
    sk.update(new String[] {"key1"}, new String[] {"value1"});
    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 0);
    putBytesToJavaPath("aos_1_non_empty_no_entries_java.sk", sk.compact().toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void serializeMultiKeyStrings() throws IOException {
    int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();
      for (int i = 0; i < n; i++) {
        sk.update(new String[] {"key" + i, "subkey" + (i % 10)}, new String[] {"value" + i});
      }
      putBytesToJavaPath("aos_multikey_n" + n + "_java.sk", sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void serializeUnicodeStrings() throws IOException {
    ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();

    sk.update(new String[]{"키", "열쇠"}, new String[]{"밸류", "값"});
    //These are emojis that are outside the Basic Multilingual Plane and explicitly coded here
    // as 16-bit surrogate pairs to fix a bug in the TestNG Eclipse Plugin (7.11.0).
    // These 4 emojis are the Unicode Code Points (in order):
    //"Key", U+1F511; "Old Key", U+1F5DD,U+FE0F; "Package", U+1F4E6; "Gift", U+1F381.
    sk.update(new String[]{"\uD83D\uDD11", "\uD83D\uDDDD\uFE0F"}, new String[]{"\uD83D\uDCE6", "\uD83C\uDF81"});
    sk.update(new String[]{"ключ1", "ключ2"}, new String[]{"ценить1", "ценить2"});

    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 3);

    putBytesToJavaPath("aos_unicode_java.sk", sk.compact().toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void serializeEmptyStrings() throws IOException {
    ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();

    sk.update(new String[]{""}, new String[]{"empty_key_value"});
    sk.update(new String[]{"empty_value_key"}, new String[]{""});
    sk.update(new String[]{"", ""}, new String[]{"", ""});

    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 3);

    putBytesToJavaPath("aos_empty_strings_java.sk", sk.compact().toByteArray());
  }

  @Test(groups = {CHECK_JAVA_FILES})
  public void checkJava() {
    deserializeOneString(GroupLanguage.JAVA);
    deserializeFromThreeStrings(GroupLanguage.JAVA);
    deserializeOneStringNonEmptyNoEntries(GroupLanguage.JAVA);
    deserializeMultiKeyStrings(GroupLanguage.JAVA);
    deserializeUnicodeStrings(GroupLanguage.JAVA);
    deserializeEmptyStrings(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    deserializeOneString(GroupLanguage.CPP);
    deserializeFromThreeStrings(GroupLanguage.CPP);
    deserializeOneStringNonEmptyNoEntries(GroupLanguage.CPP);
    deserializeMultiKeyStrings(GroupLanguage.CPP);
    deserializeUnicodeStrings(GroupLanguage.CPP);
    deserializeEmptyStrings(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    deserializeOneString(GroupLanguage.GO);
    deserializeFromThreeStrings(GroupLanguage.GO);
    deserializeOneStringNonEmptyNoEntries(GroupLanguage.GO);
    deserializeMultiKeyStrings(GroupLanguage.GO);
    deserializeUnicodeStrings(GroupLanguage.GO);
    deserializeEmptyStrings(GroupLanguage.GO);
  }

  private static void deserializeOneString(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      final String fileName = "aos_1_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
      final TupleSketch<ArrayOfStringsSummary> sketch =
         ArrayOfStringsTupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new ArrayOfStringsSummaryDeserializer());
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      assertTrue(n > 1000? sketch.isEstimationMode() : !sketch.isEstimationMode());

      final TupleSketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
      while (it.next()) {
        assertTrue(it.getHash() < sketch.getThetaLong());
        final String[] summary = it.getSummary().getValue();
        assertEquals(summary.length, 1);
      }
    }
  }

  private static void deserializeFromThreeStrings(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      final String fileName = "aos_3_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
      final TupleSketch<ArrayOfStringsSummary> sketch =
         ArrayOfStringsTupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new ArrayOfStringsSummaryDeserializer());
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      assertTrue(n > 1000? sketch.isEstimationMode() : !sketch.isEstimationMode());

      final TupleSketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
      while (it.next()) {
        assertTrue(it.getHash() < sketch.getThetaLong());
        final String[] summary = it.getSummary().getValue();
        assertEquals(summary.length, 3);
      }
    }
  }

  private static void deserializeOneStringNonEmptyNoEntries(final GroupLanguage lang) {
    final String fileName = "aos_1_non_empty_no_entries" + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return; }
    //System.out.println(fileName);
    final TupleSketch<ArrayOfStringsSummary> sketch =
        ArrayOfStringsTupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new ArrayOfStringsSummaryDeserializer());
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
  }

  private static void deserializeMultiKeyStrings(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      final String fileName = "aos_multikey_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue; }
      //System.out.println(fileName);
      final TupleSketch<ArrayOfStringsSummary> sketch =
         ArrayOfStringsTupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new ArrayOfStringsSummaryDeserializer());
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      assertTrue(n > 1000? sketch.isEstimationMode() : !sketch.isEstimationMode());

      final TupleSketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
      while (it.next()) {
        assertTrue(it.getHash() < sketch.getThetaLong());
        final String[] summary = it.getSummary().getValue();
        assertEquals(summary.length, 1);
      }
    }
  }

  private static void deserializeUnicodeStrings(final GroupLanguage lang) {
    final String fileName = "aos_unicode" + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return; }
    //System.out.println(fileName);
    final TupleSketch<ArrayOfStringsSummary> sketch =
        ArrayOfStringsTupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new ArrayOfStringsSummaryDeserializer());
    assertFalse(sketch.isEmpty());
    assertFalse(sketch.isEstimationMode());
    assertEquals(sketch.getEstimate(), 3.0);

    final Set<List<String>> summaries = getSummaries(sketch);
    assertTrue(summaries.contains(Arrays.asList("밸류", "값")));
    assertTrue(summaries.contains(Arrays.asList("📦", "🎁")));
    assertTrue(summaries.contains(Arrays.asList("ценить1", "ценить2")));
  }

  private static void deserializeEmptyStrings(final GroupLanguage lang) {
    final String fileName = "aos_empty_strings" + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return; }
    //System.out.println(fileName);
    final TupleSketch<ArrayOfStringsSummary> sketch =
        ArrayOfStringsTupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new ArrayOfStringsSummaryDeserializer());
    assertFalse(sketch.isEmpty());
    assertFalse(sketch.isEstimationMode());
    assertEquals(sketch.getEstimate(), 3.0);

    final Set<List<String>> summaries = getSummaries(sketch);
    assertTrue(summaries.contains(Arrays.asList("empty_key_value")));
    assertTrue(summaries.contains(Arrays.asList("")));
    assertTrue(summaries.contains(Arrays.asList("", "")));
  }

  private static Set<List<String>> getSummaries(final TupleSketch<ArrayOfStringsSummary> sketch) {
    final Set<List<String>> summaries = new HashSet<>();
    final TupleSketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
    while (it.next()) {
      assertTrue(it.getHash() < sketch.getThetaLong());
      summaries.add(Arrays.asList(it.getSummary().getValue()));
    }
    return summaries;
  }
}
