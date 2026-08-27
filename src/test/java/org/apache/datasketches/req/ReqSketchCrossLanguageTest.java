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

package org.apache.datasketches.req;

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class ReqSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final ReqSketch sk = ReqSketch.builder().build();
      for (int i = 1; i <= n; i++) {
        sk.update(i);
      }
      putBytesToJavaPath("req_float_n" + n + "_java.sk",  sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateNegativeBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {1, 10};
    for (final int n: nArr) {
      final ReqSketch sk = ReqSketch.builder().build();
      for (int i = 1; i <= n; i++) {
        sk.update(-i);
      }
      putBytesToJavaPath("req_float_negative_n" + n + "_java.sk", sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateMixedBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {1, 10};
    for (final int n: nArr) {
      final ReqSketch sk = ReqSketch.builder().build();
      for (int i = -n; i <= n; i++) { //produces sets of {-1,0,1}, {-10...,0,...10}
        sk.update(i);
      }
      putBytesToJavaPath("req_float_mixed_n" + n + "_java.sk", sk.toByteArray());
    }
  }

  @Test(groups = {CHECK_JAVA_FILES})
  public void checkJava() {
    checkReqPositiveValues(GroupLanguage.JAVA);
    checkReqNegativeValues(GroupLanguage.JAVA);
    checkReqMixedValues(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    checkReqPositiveValues(GroupLanguage.CPP);
    checkReqNegativeValues(GroupLanguage.CPP);
    checkReqMixedValues(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    checkReqPositiveValues(GroupLanguage.GO);
    checkReqNegativeValues(GroupLanguage.GO);
    checkReqMixedValues(GroupLanguage.GO);
  }

  private static void checkReqPositiveValues(final GroupLanguage lang) {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final int n: nArr) {
      final String fileName = "req_float_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReqSketch sk = ReqSketch.heapify(MemorySegment.ofArray(bytes));
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      assertTrue(n > 10 ? sk.isEstimationMode() : !sk.isEstimationMode());
      assertEquals(sk.getN(), n);
      if (n > 0) {
        assertEquals(sk.getMinItem(), 1);
        assertEquals(sk.getMaxItem(), n);
        final QuantilesFloatsSketchIterator it = sk.iterator();
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

  private static void checkReqNegativeValues(final GroupLanguage lang) {
    final int[] nArr = {1, 10};
    for (final int n: nArr) {
      final String fileName = "req_float_negative_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReqSketch sk = ReqSketch.heapify(MemorySegment.ofArray(bytes));
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      assertTrue(n > 10 ? sk.isEstimationMode() : !sk.isEstimationMode());
      assertEquals(sk.getN(), n);
      if (n > 0) {
        assertEquals(sk.getMinItem(), -n);
        assertEquals(sk.getMaxItem(), -1);
        final QuantilesFloatsSketchIterator it = sk.iterator();
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

  private static void checkReqMixedValues(final GroupLanguage lang) {
    final int[] nArr = {1, 10};
    for (final int n: nArr) {
      final String fileName = "req_float_mixed_n" + n + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReqSketch sk = ReqSketch.heapify(MemorySegment.ofArray(bytes));
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      if (n == 1)  { assertEquals(sk.getN(), 3); }
      if (n == 10) { assertEquals(sk.getN(), 21); }
      if (n > 0) {
        assertEquals(sk.getMinItem(), -n);
        assertEquals(sk.getMaxItem(), n);
        final QuantilesFloatsSketchIterator it = sk.iterator();
        long weight = 0;
        while(it.next()) {
          assertTrue(it.getQuantile() >= sk.getMinItem());
          assertTrue(it.getQuantile() <= sk.getMaxItem());
          weight += it.getWeight();
        }
        assertEquals(weight, 2 * n + 1);
      }
    }
  }
}
