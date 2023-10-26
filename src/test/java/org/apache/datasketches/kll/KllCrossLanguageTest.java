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

package org.apache.datasketches.kll;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.CHECK_CPP_HISTORICAL_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.TestUtil;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;
import org.testng.annotations.Test;

/**
 * Methods for cross language integration testing
 */
public class KllCrossLanguageTest {
  private ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateKllDoublesSketchBinaries() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
      for (int i = 1; i <= n; i++) { sk.update(i); }
      Files.newOutputStream(javaPath.resolve("kll_double_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateKllFloatsSketchBinaries() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
      for (int i = 1; i <= n; i++) { sk.update(i); }
      Files.newOutputStream(javaPath.resolve("kll_float_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateKllItemsSketchBinaries() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final int digits = Util.numDigits(n);
      final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
      for (int i = 1; i <= n; i++) { sk.update(Util.intToFixedLengthString(i, digits)); }
      Files.newOutputStream(javaPath.resolve("kll_string_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  public void checkCppKllDoublesSketchOneItemVersion1() {
    final byte[] byteArr = TestUtil.getResourceBytes("kll_sketch_double_one_item_v1.sk");
    final KllDoublesSketch sk = KllDoublesSketch.heapify(Memory.wrap(byteArr));
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getMaxItem(), 1.0);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  public void checkCppKllFloatsSketchOneItemVersion1() {
    final byte[] byteArr = TestUtil.getResourceBytes("kll_sketch_float_one_item_v1.sk");
    final KllFloatsSketch sk = KllFloatsSketch.heapify(Memory.wrap(byteArr));
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertEquals(sk.getMinItem(), 1.0F);
    assertEquals(sk.getMaxItem(), 1.0F);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void kllFloat() throws IOException {
    final int[] nArr = {0, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("kll_float_n" + n + "_cpp.sk"));
      final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(bytes));
      assertEquals(sketch.getK(), 200);
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertTrue(n > 100 ? sketch.isEstimationMode() : !sketch.isEstimationMode());
      assertEquals(sketch.getN(), n);
      if (n > 0) {
        assertEquals(sketch.getMinItem(), 1);
        assertEquals(sketch.getMaxItem(), n);
        long weight = 0;
        QuantilesFloatsSketchIterator it = sketch.iterator();
        while (it.next()) {
          assertTrue(it.getQuantile() >= sketch.getMinItem());
          assertTrue(it.getQuantile() <= sketch.getMaxItem());
          weight += it.getWeight();
        }
        assertEquals(weight, n);
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void kllDouble() throws IOException {
    final int[] nArr = {0, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("kll_double_n" + n + "_cpp.sk"));
      final KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(bytes));
      assertEquals(sketch.getK(), 200);
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertTrue(n > 100 ? sketch.isEstimationMode() : !sketch.isEstimationMode());
      assertEquals(sketch.getN(), n);
      if (n > 0) {
        assertEquals(sketch.getMinItem(), 1);
        assertEquals(sketch.getMaxItem(), n);
        long weight = 0;
        QuantilesDoublesSketchIterator it = sketch.iterator();
        while (it.next()) {
          assertTrue(it.getQuantile() >= sketch.getMinItem());
          assertTrue(it.getQuantile() <= sketch.getMaxItem());
          weight += it.getWeight();
        }
        assertEquals(weight, n);
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void kllString() throws IOException {
    // sketch contains numbers in strings to make meaningful assertions
    Comparator<String> numericOrder = new Comparator<String>() {
      @Override
      public int compare(final String s1, final String s2) {
        try {
          final int i1 = Integer.parseInt(s1);
          final int i2 = Integer.parseInt(s2);
          return Integer.compare(i1, i2);
        } catch (NumberFormatException e) {
          throw new RuntimeException(e);
        }
      }
    };
    final int[] nArr = {0, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("kll_string_n" + n + "_cpp.sk"));
      final KllHeapItemsSketch<String> sketch = new KllHeapItemsSketch<>(
        Memory.wrap(bytes),
        numericOrder,
        new ArrayOfStringsSerDe()
      );
      assertEquals(sketch.getK(), 200);
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertTrue(n > 100 ? sketch.isEstimationMode() : !sketch.isEstimationMode());
      assertEquals(sketch.getN(), n);
      if (n > 0) {
        assertEquals(sketch.getMinItem(), Integer.toString(1));
        assertEquals(sketch.getMaxItem(), Integer.toString(n));
        long weight = 0;
        QuantilesGenericSketchIterator<String> it = sketch.iterator();
        while (it.next()) {
          assertTrue(numericOrder.compare(it.getQuantile(), sketch.getMinItem()) >= 0);
          assertTrue(numericOrder.compare(it.getQuantile(), sketch.getMaxItem()) <= 0);
          weight += it.getWeight();
        }
        assertEquals(weight, n);
      }
    }
  }

}
