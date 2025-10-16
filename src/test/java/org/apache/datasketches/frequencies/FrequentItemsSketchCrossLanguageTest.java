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

package org.apache.datasketches.frequencies;

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

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.FrequentItemsSketch;
import org.apache.datasketches.frequencies.FrequentLongsSketch;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class FrequentItemsSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingLongsSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final FrequentLongsSketch sk = new FrequentLongsSketch(64);
      for (int i = 1; i <= n; i++) {
        sk.update(i);
      }
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      if (n > 10) { assertTrue(sk.getMaximumError() > 0); }
      else { assertEquals(sk.getMaximumError(), 0); }
      Files.newOutputStream(javaPath.resolve("frequent_long_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingStringsSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final FrequentItemsSketch<String> sk = new FrequentItemsSketch<>(64);
      for (int i = 1; i <= n; i++) {
        sk.update(Integer.toString(i));
      }
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      if (n > 10) { assertTrue(sk.getMaximumError() > 0); }
      else { assertEquals(sk.getMaximumError(), 0); }
      Files.newOutputStream(javaPath.resolve("frequent_string_n" + n + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingStringsSketchAscii() throws IOException {
    final FrequentItemsSketch<String> sk = new FrequentItemsSketch<>(64);
    sk.update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1);
    sk.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2);
    sk.update("ccccccccccccccccccccccccccccc", 3);
    sk.update("ddddddddddddddddddddddddddddd", 4);
    Files.newOutputStream(javaPath.resolve("frequent_string_ascii_java.sk"))
      .write(sk.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingStringsSketchUtf8() throws IOException {
    final FrequentItemsSketch<String> sk = new FrequentItemsSketch<>(64);
    sk.update("абвгд", 1);
    sk.update("еёжзи", 2);
    sk.update("йклмн", 3);
    sk.update("опрст", 4);
    sk.update("уфхцч", 5);
    sk.update("шщъыь", 6);
    sk.update("эюя", 7);
    Files.newOutputStream(javaPath.resolve("frequent_string_utf8_java.sk"))
      .write(sk.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void longs() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("frequent_long_n" + n + "_cpp.sk"));
      final FrequentLongsSketch sketch = FrequentLongsSketch.getInstance(MemorySegment.ofArray(bytes));
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      if (n > 10) {
        assertTrue(sketch.getMaximumError() > 0);
      } else {
        assertEquals(sketch.getMaximumError(), 0);
      }
      assertEquals(sketch.getStreamLength(), n);
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void strings() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("frequent_string_n" + n + "_cpp.sk"));
      final FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      if (n > 10) {
        assertTrue(sketch.getMaximumError() > 0);
      } else {
        assertEquals(sketch.getMaximumError(), 0);
      }
      assertEquals(sketch.getStreamLength(), n);
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void stringsAscii() throws IOException {
    final byte[] bytes = Files.readAllBytes(cppPath.resolve("frequent_string_ascii_cpp.sk"));
    final FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getMaximumError(), 0);
    assertEquals(sketch.getStreamLength(), 10);
    assertEquals(sketch.getEstimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 1);
    assertEquals(sketch.getEstimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), 2);
    assertEquals(sketch.getEstimate("ccccccccccccccccccccccccccccc"), 3);
    assertEquals(sketch.getEstimate("ddddddddddddddddddddddddddddd"), 4);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void stringsUtf8() throws IOException {
    final byte[] bytes = Files.readAllBytes(cppPath.resolve("frequent_string_utf8_cpp.sk"));
    final FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getMaximumError(), 0);
    assertEquals(sketch.getStreamLength(), 28);
    assertEquals(sketch.getEstimate("абвгд"), 1);
    assertEquals(sketch.getEstimate("еёжзи"), 2);
    assertEquals(sketch.getEstimate("йклмн"), 3);
    assertEquals(sketch.getEstimate("опрст"), 4);
    assertEquals(sketch.getEstimate("уфхцч"), 5);
    assertEquals(sketch.getEstimate("шщъыь"), 6);
    assertEquals(sketch.getEstimate("эюя"), 7);
  }

}
