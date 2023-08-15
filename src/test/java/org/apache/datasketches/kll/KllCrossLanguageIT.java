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

import static org.apache.datasketches.common.Util.getResourceBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * Methods for cross language integration testing
 */
public class KllCrossLanguageIT {
  private ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test(groups = {"generate"})
  public void generateKllDoublesSketchBinaries() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
      for (int i = 1; i <= n; i++) sk.update(i);
      try (final FileOutputStream file = new FileOutputStream("kll_double_n" + n + "_java.sk")) {
        file.write(sk.toByteArray());
      }
    }
  }

  @Test(groups = {"generate"})
  public void generateKllFloatsSketchBinaries() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
      for (int i = 1; i <= n; i++) sk.update(i);
      try (final FileOutputStream file = new FileOutputStream("kll_float_n" + n + "_java.sk")) {
        file.write(sk.toByteArray());
      }
    }
  }

  @Test(groups = {"generate"})
  public void generateKllItemsSketchBinaries() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final int digits = Util.numDigits(n);
      final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
      for (int i = 1; i <= n; i++) sketch.update(Util.intToFixedLengthString(i, digits));
      try (final FileOutputStream file = new FileOutputStream("kll_string_n" + n + "_java.sk")) {
        file.write(sketch.toByteArray());
      }
    }
  }

  @Test(groups = {"cross_language_check"})
  public void checkCppKllDoublesSketchEstimationMode() throws Exception {
    final File file = Util.getResourceFile("kll_double_estimation_cpp.sk");
    try (MapHandle mh = Memory.map(file)) {
      final KllDoublesSketch sk = KllDoublesSketch.heapify(mh.get());
      assertEquals(sk.getMinItem(), 0);
      assertEquals(sk.getMaxItem(), 999);
      assertEquals(sk.getN(), 1000);
    }
  }

  @Test(groups = {"cross_language_check"})
  public void checkCppKllDoublesSketchOneItemVersion1() throws Exception {
    final byte[] bytes = getResourceBytes("kll_sketch_double_one_item_v1.sk");
    final KllDoublesSketch sk = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertEquals(sk.getMinItem(), 1.0);
    assertEquals(sk.getMaxItem(), 1.0);
  }

  @Test(groups = {"cross_language_check"})
  public void checkCppKllFloatsSketchEstimationMode() throws Exception {
    final File file = Util.getResourceFile("kll_float_estimation_cpp.sk");
    try (MapHandle mh = Memory.map(file)) {
      final KllFloatsSketch sk = KllFloatsSketch.heapify(mh.get());
      assertEquals(sk.getMinItem(), 0);
      assertEquals(sk.getMaxItem(), 999);
      assertEquals(sk.getN(), 1000);
    }
  }

  @Test(groups = {"cross_language_check"})
  public void checkCppKllFloatsSketchOneItemVersion1() throws Exception {
    final byte[] bytes = getResourceBytes("kll_sketch_float_one_item_v1.sk");
    final KllFloatsSketch sk = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertEquals(sk.getMinItem(), 1.0F);
    assertEquals(sk.getMaxItem(), 1.0F);
  }

}
