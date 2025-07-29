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

package org.apache.datasketches.quantiles2;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.CHECK_CPP_HISTORICAL_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe2;
import org.apache.datasketches.common.TestUtil;
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class QuantilesSketchCrossLanguageTest {
  private static final String LS = System.getProperty("line.separator");

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateDoublesSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final UpdateDoublesSketch sk = DoublesSketch.builder().build();
      for (int i = 1; i <= n; i++) {
        sk.update(i);
      }
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
          try {
            final int i1 = Integer.parseInt(s1);
            final int i2 = Integer.parseInt(s2);
            return Integer.compare(i1,i2);
          } catch (final NumberFormatException e) {
            throw new RuntimeException(e);
          }
        }
      });
      for (int i = 1; i <= n; i++) {
        sk.update(Integer.toString(i));
      }
      if (n > 0) {
        assertEquals(sk.getMinItem(), "1");
        assertEquals(sk.getMaxItem(), Integer.toString(n));
      }
      Files.newOutputStream(javaPath.resolve("quantiles_string_n" + n + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfStringsSerDe2()));
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkDoublesSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final byte[] byteArr = Files.readAllBytes(cppPath.resolve("quantiles_double_n" + n + "_cpp.sk"));
      final DoublesSketch sk = DoublesSketch.wrap(MemorySegment.ofArray(byteArr));
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      assertTrue(n > 128 ? sk.isEstimationMode() : !sk.isEstimationMode());
      assertEquals(sk.getN(), n);
      if (n > 0) {
        assertEquals(sk.getMinItem(), 1);
        assertEquals(sk.getMaxItem(), n);
        final QuantilesDoublesSketchIterator it = sk.iterator();
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
    final Comparator<String> numericOrder = new Comparator<String>() {
      @Override
      public int compare(final String s1, final String s2) {
        try {
          final int i1 = Integer.parseInt(s1);
          final int i2 = Integer.parseInt(s2);
          return Integer.compare(i1, i2);
        } catch (final NumberFormatException e) {
          throw new RuntimeException(e);
        }
      }
    };
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final byte[] byteArr = Files.readAllBytes(cppPath.resolve("quantiles_string_n" + n + "_cpp.sk"));
      final ItemsSketch<String> sk = ItemsSketch.getInstance(
          String.class,
          MemorySegment.ofArray(byteArr),
          numericOrder,
          new ArrayOfStringsSerDe2()
      );
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      assertTrue(n > 128 ? sk.isEstimationMode() : !sk.isEstimationMode());
      assertEquals(sk.getN(), n);
      if (n > 0) {
        assertEquals(sk.getMinItem(), "1");
        assertEquals(sk.getMaxItem(), Integer.toString(n));
        final QuantilesGenericSketchIterator<String> it = sk.iterator();
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

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.3.0.sk
  //Median2: 26.0
  public void check030_50() {
    final int n = 50;
    final String ver = "0.3.0";
    final double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.3.0.sk
  //Median2: 501.0
  public void check030_1000() {
    final int n = 1000;
    final String ver = "0.3.0";
    final double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.6.0.sk
  //Median2: 26.0
  public void check060_50() {
    final int n = 50;
    final String ver = "0.6.0";
    final double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.6.0.sk
  //Median2: 501.0
  public void check060_1000() {
    final int n = 1000;
    final String ver = "0.6.0";
    final double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.8.0.sk
  //Median2: 26.0
  public void check080_50() {
    final int n = 50;
    final String ver = "0.8.0";
    final double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.8.0.sk
  //Median2: 501.0
  public void check080_1000() {
    final int n = 1000;
    final String ver = "0.8.0";
    final double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.8.3.sk
  //Median2: 26.0
  public void check083_50() {
    final int n = 50;
    final String ver = "0.8.3";
    final double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.8.0.sk
  //Median2: 501.0
  public void check083_1000() {
    final int n = 1000;
    final String ver = "0.8.3";
    final double expected = 501;
    getAndCheck(ver, n, expected);
  }

  private static void getAndCheck(final String ver, final int n, final double quantile) {
    DoublesSketch.rand.setSeed(131); //make deterministic
    //create fileName
    final int k = 128;
    final double nf = 0.5;
    final String fileName = String.format("Qk%d_n%d_v%s.sk", k, n, ver);
    println("fullName: "+ fileName);
    println("Old Median: " + quantile);
    //Read File bytes
    final byte[] byteArr = TestUtil.getResourceBytes(fileName);
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArr);

    // heapify as update sketch
    DoublesSketch qs2 = UpdateDoublesSketch.heapify(srcSeg);
    //Test the quantile
    double q2 = qs2.getQuantile(nf, EXCLUSIVE);
    println("New Median: " + q2);
    Assert.assertEquals(q2, quantile, 0.0);

    // same thing with compact sketch
    qs2 = HeapCompactDoublesSketch.heapifyInstance(srcSeg);
    //Test the quantile
    q2 = qs2.getQuantile(nf, EXCLUSIVE);
    println("New Median: " + q2);
    Assert.assertEquals(q2, quantile, 0.0);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  static void println(final Object o) {
    if (o == null) { print(LS); }
    else { print(o.toString() + LS); }
  }

  /**
   * @param o value to print
   */
  static void print(final Object o) {
    if (o != null) {
      //System.out.print(o.toString()); //disable here
    }
  }

}
