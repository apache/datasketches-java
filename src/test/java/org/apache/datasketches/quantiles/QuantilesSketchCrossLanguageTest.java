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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class QuantilesSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateDoublesSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final UpdateDoublesSketch sk = DoublesSketch.builder().build();
      for (int i = 1; i <= n; i++) sk.update(i);
      Files.newOutputStream(javaPath.resolve("quantiles_double_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateItemsSketchWithStrings() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final ItemsSketch<String> sk = ItemsSketch.getInstance(String.class, new Comparator<String>() {
        @Override
        public int compare(final String s1, final String s2) {
          final Integer i1 = Integer.parseInt(s1);
          final Integer i2 = Integer.parseInt(s2);
          return i1.compareTo(i2);
        }
      });
      for (int i = 1; i <= n; i++) sk.update(Integer.toString(i));
      if (n > 0) {
        assertEquals(sk.getMinItem(), "1");
        assertEquals(sk.getMaxItem(), Integer.toString(n));
      }
      Files.newOutputStream(javaPath.resolve("quantiles_string_n" + n + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkDoublesSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] byteArr = Files.readAllBytes(cppPath.resolve("quantiles_double_n" + n + "_cpp.sk"));
      final DoublesSketch sk = DoublesSketch.wrap(Memory.wrap(byteArr));
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      assertTrue(n > 128 ? sk.isEstimationMode() : !sk.isEstimationMode());
      assertEquals(sk.getN(), n);
      if (n > 0) {
        assertEquals(sk.getMinItem(), 1);
        assertEquals(sk.getMaxItem(), n);
        QuantilesDoublesSketchIterator it = sk.iterator();
        long weight = 0;
        while(it.next()) {
          assertTrue(it.getQuantile() >= sk.getMinItem());
          assertTrue(it.getQuantile() <= sk.getMaxItem());
          weight += it.getWeight();
        }
        assertEquals(weight, n);
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkItemsSketchWithStrings() throws IOException {
    // sketch contains numbers in strings to make meaningful assertions
    Comparator<String> numericOrder = new Comparator<String>() {
      @Override
      public int compare(final String s1, final String s2) {
        final Integer i1 = Integer.parseInt(s1);
        final Integer i2 = Integer.parseInt(s2);
        return i1.compareTo(i2);
      }
    };
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] byteArr = Files.readAllBytes(cppPath.resolve("quantiles_string_n" + n + "_cpp.sk"));
      final ItemsSketch<String> sk = ItemsSketch.getInstance(
          String.class,
          Memory.wrap(byteArr),
          numericOrder,
          new ArrayOfStringsSerDe()
      );
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      assertTrue(n > 128 ? sk.isEstimationMode() : !sk.isEstimationMode());
      assertEquals(sk.getN(), n);
      if (n > 0) {
        assertEquals(sk.getMinItem(), "1");
        assertEquals(sk.getMaxItem(), Integer.toString(n));
        QuantilesGenericSketchIterator<String> it = sk.iterator();
        long weight = 0;
        while(it.next()) {
          assertTrue(numericOrder.compare(it.getQuantile(), sk.getMinItem()) >= 0);
          assertTrue(numericOrder.compare(it.getQuantile(), sk.getMaxItem()) <= 0);
          weight += it.getWeight();
        }
        assertEquals(weight, n);
      }
    }
  }

}
