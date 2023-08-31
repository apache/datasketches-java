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

package org.apache.datasketches.sampling;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class VarOptCrossLanguageTest {
  static final double EPS = 1e-13;

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateSketchesLong() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final VarOptItemsSketch<Long> sk = VarOptItemsSketch.newInstance(32);
      for (int i = 1; i <= n; i++) sk.update(Long.valueOf(i), 1.0);
      Files.newOutputStream(javaPath.resolve("varopt_sketch_long_n" + n + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateSketchStringExact() throws IOException {
    final VarOptItemsSketch<String> sketch = VarOptItemsSketch.newInstance(1024);
    for (int i = 1; i <= 200; ++i) {
      sketch.update(Integer.toString(i), 1000.0 / i);
    }
    Files.newOutputStream(javaPath.resolve("varopt_sketch_string_exact_java.sk"))
      .write(sketch.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateSketchLongSampling() throws IOException {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(1024);
    for (long i = 0; i < 2000; ++i) {
      sketch.update(i, 1.0);
    }
    // negative heavy items to allow a simple predicate to filter
    sketch.update(-1L, 100000.0);
    sketch.update(-2L, 110000.0);
    sketch.update(-3L, 120000.0);
    Files.newOutputStream(javaPath.resolve("varopt_sketch_long_sampling_java.sk"))
      .write(sketch.toByteArray(new ArrayOfLongsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateUnionDoubleSampling() throws IOException {
    final int kSmall = 16;
    final int n1 = 32;
    final int n2 = 64;
    final int kMax = 128;

    // small k sketch, but sampling
    VarOptItemsSketch<Double> sketch = VarOptItemsSketch.newInstance(kSmall);
    for (int i = 0; i < n1; ++i) {
      sketch.update(1.0 * i, 1.0);
    }
    sketch.update(-1.0, n1 * n1); // negative heavy item to allow a simple predicate to filter


    final VarOptItemsUnion<Double> union = VarOptItemsUnion.newInstance(kMax);
    union.update(sketch);

    // another one, but different n to get a different per-item weight
    sketch = VarOptItemsSketch.newInstance(kSmall);
    for (int i = 0; i < n2; ++i) {
      sketch.update(1.0 * i, 1.0);
    }
    union.update(sketch);
    Files.newOutputStream(javaPath.resolve("varopt_union_double_sampling_java.sk"))
      .write(union.toByteArray(new ArrayOfDoublesSerDe()));
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppSketchLongs() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("varopt_sketch_long_n" + n + "_cpp.sk"));
      final VarOptItemsSketch<Long> sk = VarOptItemsSketch.heapify(Memory.wrap(bytes), new ArrayOfLongsSerDe());
      assertEquals(sk.getK(), 32);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n > 10 ? 32 : n);
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppSketchStringsExact() throws IOException {
    final byte[] bytes = Files.readAllBytes(cppPath.resolve("varopt_sketch_string_exact_cpp.sk"));
    final VarOptItemsSketch<String> sk = VarOptItemsSketch.heapify(Memory.wrap(bytes), new ArrayOfStringsSerDe());
    assertEquals(sk.getK(), 1024);
    assertEquals(sk.getN(), 200);
    assertEquals(sk.getNumSamples(), 200);
    final SampleSubsetSummary ss = sk.estimateSubsetSum(item -> true);
    double weight = 0;
    for (int i = 1; i <= 200; ++i) weight += 1000.0 / i;
    assertEquals(ss.getTotalSketchWeight(), weight, EPS);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppSketchLongsSampling() throws IOException {
    final byte[] bytes = Files.readAllBytes(cppPath.resolve("varopt_sketch_long_sampling_cpp.sk"));
    final VarOptItemsSketch<Long> sk = VarOptItemsSketch.heapify(Memory.wrap(bytes), new ArrayOfLongsSerDe());
    assertEquals(sk.getK(), 1024);
    assertEquals(sk.getN(), 2003);
    assertEquals(sk.getNumSamples(), 1024);
    SampleSubsetSummary ss = sk.estimateSubsetSum(item -> true);
    assertEquals(ss.getTotalSketchWeight(), 332000.0, EPS);

    ss = sk.estimateSubsetSum(item -> item < 0);
    assertEquals(ss.getEstimate(), 330000.0); // heavy item, weight is exact

    ss = sk.estimateSubsetSum(item -> item >= 0);
    assertEquals(ss.getEstimate(), 2000.0, EPS);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppUnionDoubleSampling() throws IOException {
    final byte[] bytes = Files.readAllBytes(cppPath.resolve("varopt_union_double_sampling_cpp.sk"));
    final VarOptItemsUnion<Double> u = VarOptItemsUnion.heapify(Memory.wrap(bytes), new ArrayOfDoublesSerDe());

    // must reduce k in the process
    final VarOptItemsSketch<Double> sk = u.getResult();
    assertTrue(sk.getK() < 128);
    assertEquals(sk.getN(), 97);

    // light items, ignoring the heavy one
    SampleSubsetSummary ss = sk.estimateSubsetSum(item -> item >= 0);
    assertEquals(ss.getEstimate(), 96.0, EPS);
    assertEquals(ss.getTotalSketchWeight(), 96.0 + 1024.0, EPS);
  }

}
