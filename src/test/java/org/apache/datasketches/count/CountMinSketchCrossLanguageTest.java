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

package org.apache.datasketches.count;

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_RUST_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.testng.annotations.Test;

/**
 * Generates and checks the Count-Min version 2 cross-language files defined in section 8 of the
 * TCK spec docs/count-min-format.md.
 */
public class CountMinSketchCrossLanguageTest {
  private static final byte NUM_HASHES = 3;
  private static final int NUM_BUCKETS = 1000;

  /** One case from the table in section 8.1 of the spec. */
  private record Case(String name, long seed, int n) { }

  private static final Case[] CASES = {
    new Case("n0", 9001L, 0),
    new Case("n1", 9001L, 1),
    new Case("n10", 9001L, 10),
    new Case("n100", 9001L, 100),
    new Case("n1000", 9001L, 1000),
    new Case("n10000", 9001L, 10000),
    new Case("seed42_n1000", 42L, 1000) };

  private static String fileName(final Case testCase, final GroupLanguage lang) {
    return "count_min_v2_" + testCase.name() + lang.sfx + ".sk";
  }

  static CountMinSketch build(final Case testCase) {
    final int n = testCase.n();
    final CountMinSketch cms = new CountMinSketch(NUM_HASHES, NUM_BUCKETS, testCase.seed());
    for (int i = 0; i < n; i++) {
      cms.update(i, i + 1L);
      cms.update(Integer.toString(i), 1);
    }
    return cms;
  }

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void generateCountMinBinaries() {
    for (final Case testCase : CASES) {
      putBytesToJavaPath(fileName(testCase, GroupLanguage.JAVA), build(testCase).toByteArray());
    }
  }

  @Test(groups = {CHECK_JAVA_FILES}, priority = 1)
  public void checkJava() {
    checkCountMinBinaries(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    checkCountMinBinaries(GroupLanguage.CPP);
    checkCppVersion1Binaries();
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    checkCountMinBinaries(GroupLanguage.GO);
  }

  @Test(groups = {CHECK_RUST_FILES})
  public void checkRust() {
    checkCountMinBinaries(GroupLanguage.RUST);
  }

  private static void checkCountMinBinaries(final GroupLanguage lang) {
    for (final Case testCase : CASES) {
      final byte[] bytes = getFileBytes(lang.pth, fileName(testCase, lang));
      if (bytes.length == 0) { continue; }
      final long seed = testCase.seed();
      final int n = testCase.n();
      final CountMinSketch cms = CountMinSketch.deserialize(bytes, seed);

      assertEquals(cms.getSerialVersion(), CountMinSketch.SERIAL_VERSION_2, testCase.name());
      assertEquals(cms.getNumHashes_(), NUM_HASHES, testCase.name());
      assertEquals(cms.getNumBuckets_(), NUM_BUCKETS, testCase.name());
      assertEquals(cms.isEmpty(), n == 0, testCase.name());
      assertEquals(cms.getTotalWeight_(), ((long) n * (n + 1)) / 2 + n, testCase.name());
      for (int i = 0; i < n; i++) {
        final String s = Integer.toString(i);
        assertTrue(cms.getEstimate(i) >= (i + 1L), testCase.name() + " integer " + i);
        assertTrue(cms.getEstimate(i) <= cms.getUpperBound(i), testCase.name() + " integer " + i);
        assertTrue(cms.getEstimate(s) >= 1, testCase.name() + " string " + s);
        assertTrue(cms.getEstimate(s) <= cms.getUpperBound(s), testCase.name() + " string " + s);
      }
      // Version 2 is deterministic, so the image must equal the one Java builds for the same case
      assertEquals(bytes, build(testCase).toByteArray(), testCase.name());
    }
  }

  // Section 8.3: the C++ version 1 files deserialize as version 1. Their estimates aren't checked,
  // because version 1 hashing differs between languages.
  private static void checkCppVersion1Binaries() {
    for (final String name : new String[] { "count_min_empty_cpp.sk", "count_min_non_empty_cpp.sk" }) {
      final byte[] bytes = getFileBytes(GroupLanguage.CPP.pth, name);
      if (bytes.length == 0) { continue; }
      final CountMinSketch cms = CountMinSketch.deserialize(bytes, org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED);
      assertEquals(cms.getSerialVersion(), CountMinSketch.SERIAL_VERSION_1, name);
      assertEquals(cms.isEmpty(), name.startsWith("count_min_empty"), name);
    }
  }
}
