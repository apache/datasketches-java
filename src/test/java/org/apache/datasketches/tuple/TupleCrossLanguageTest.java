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

package org.apache.datasketches.tuple;

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.CHECK_CPP_HISTORICAL_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.TestUtil;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUnion;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TupleCrossLanguageTest {

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  public void serialVersion1Compatibility() {
    final byte[] byteArr = TestUtil.getResourceBytes("CompactSketchWithDoubleSummary4K_serialVersion1.sk");
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(byteArr), new DoubleSummaryDeserializer());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch.getRetainedEntries(), 4096);
    int count = 0;
    TupleSketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  public void version2Compatibility() {
    final byte[] byteArr = TestUtil.getResourceBytes("TupleWithTestIntegerSummary4kTrimmedSerVer2.sk");
    Sketch<IntegerSummary> sketch1 = Sketches.heapifySketch(Memory.wrap(byteArr), new IntegerSummaryDeserializer());

    // construct the same way
    final int lgK = 12;
    final int K = 1 << lgK;
    final UpdatableSketchBuilder<Integer, IntegerSummary> builder =
            new UpdatableSketchBuilder<>(new IntegerSummaryFactory());
    final UpdatableSketch<Integer, IntegerSummary> updatableSketch = builder.build();
    for (int i = 0; i < 2 * K; i++) {
      updatableSketch.update(i, 1);
    }
    updatableSketch.trim();
    Sketch<IntegerSummary> sketch2 = updatableSketch.compact();

    Assert.assertEquals(sketch1.getRetainedEntries(), sketch2.getRetainedEntries());
    Assert.assertEquals(sketch1.getThetaLong(), sketch2.getThetaLong());
    Assert.assertEquals(sketch1.isEmpty(), sketch2.isEmpty());
    Assert.assertEquals(sketch1.isEstimationMode(), sketch2.isEstimationMode());
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppIntegerSummary() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("tuple_int_n" + n + "_cpp.sk"));
      final Sketch<IntegerSummary> sketch =
          Sketches.heapifySketch(Memory.wrap(bytes), new IntegerSummaryDeserializer());
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      assertTrue(n > 1000 ? sketch.isEstimationMode() : !sketch.isEstimationMode());
      assertEquals(sketch.getEstimate(), n, n * 0.03);
      final TupleSketchIterator<IntegerSummary> it = sketch.iterator();
      while (it.next()) {
        assertTrue(it.getHash() < sketch.getThetaLong());
        assertTrue(it.getSummary().getValue() < n);
      }
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateForCppIntegerSummary() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final UpdatableSketch<Integer, IntegerSummary> sk =
          new UpdatableSketchBuilder<>(new IntegerSummaryFactory()).build();
      for (int i = 0; i < n; i++) sk.update(i, i);
      Files.newOutputStream(javaPath.resolve("tuple_int_n" + n + "_java.sk")).write(sk.compact().toByteArray());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class, groups = {CHECK_CPP_HISTORICAL_FILES})
  public void noSupportHeapifyV0_9_1() throws Exception {
    final byte[] byteArr = TestUtil.getResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk");
    ArrayOfDoublesUnion.heapify(Memory.wrap(byteArr));
  }

  @Test(expectedExceptions = SketchesArgumentException.class, groups = {CHECK_CPP_HISTORICAL_FILES})
  public void noSupportWrapV0_9_1() throws Exception {
    final byte[] byteArr = TestUtil.getResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk");
    ArrayOfDoublesUnion.wrap(WritableMemory.writableWrap(byteArr));
  }

}
