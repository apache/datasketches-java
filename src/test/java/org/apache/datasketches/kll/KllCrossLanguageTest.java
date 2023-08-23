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

import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * Methods for cross language integration testing
 */
public class KllCrossLanguageTest {
  private ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test(groups = {"generate"})
  public void generateKllDoublesSketchBinaries() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
      for (int i = 1; i <= n; i++) { sk.update(i); }
      Files.newOutputStream(javaPath.resolve("kll_double_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {"generate"})
  public void generateKllFloatsSketchBinaries() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
      for (int i = 1; i <= n; i++) { sk.update(i); }
      Files.newOutputStream(javaPath.resolve("kll_float_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {"generate"})
  public void generateKllItemsSketchBinaries() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final int digits = Util.numDigits(n);
      final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
      for (int i = 1; i <= n; i++) { sk.update(Util.intToFixedLengthString(i, digits)); }
      Files.newOutputStream(javaPath.resolve("kll_string_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {"check_cpp_files"})
  public void checkCppKllDoublesSketchEstimationMode() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("kll_double_estimation_cpp.sk"));
    final KllDoublesSketch sk = KllDoublesSketch.heapify(Memory.wrap(byteArr));
    assertEquals(sk.getMinItem(), 0);
    assertEquals(sk.getMaxItem(), 999);
    assertEquals(sk.getN(), 1000);
  }

  @Test(groups = {"check_cpp_files"})
  public void checkCppKllDoublesSketchOneItemVersion1() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("kll_sketch_double_one_item_v1.sk"));
    final KllDoublesSketch sk = KllDoublesSketch.heapify(Memory.wrap(byteArr));
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getMaxItem(), 1.0);
  }

  @Test(groups = {"check_cpp_files"})
  public void checkCppKllFloatsSketchEstimationMode() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("kll_float_estimation_cpp.sk"));
    final KllFloatsSketch sk = KllFloatsSketch.heapify(Memory.wrap(byteArr));
    assertEquals(sk.getMinItem(), 0);
    assertEquals(sk.getMaxItem(), 999);
    assertEquals(sk.getN(), 1000);
  }

  @Test(groups = {"check_cpp_files"})
  public void checkCppKllFloatsSketchOneItemVersion1() throws IOException {
    final byte[] byteArr = Files.readAllBytes(cppPath.resolve("kll_sketch_float_one_item_v1.sk"));
    final KllFloatsSketch sk = KllFloatsSketch.heapify(Memory.wrap(byteArr));
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertEquals(sk.getMinItem(), 1.0F);
    assertEquals(sk.getMaxItem(), 1.0F);
  }

}
