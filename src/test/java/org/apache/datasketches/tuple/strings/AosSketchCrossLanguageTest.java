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

package org.apache.datasketches.tuple.strings;

import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.common.ResizeFactor;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by other language code.
 * Test deserialization of binary sketches serialized by other language code.
 */
public class AosSketchCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingOneString() throws IOException {
    int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();
      for (int i = 0; i < n; i++) {
        sk.update(new String[] {String.valueOf(i)}, new String[] {"value" + i});
      }
      Files.newOutputStream(javaPath.resolve("aos_1_n" + n + "_java.sk")).write(sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingThreeStrings() throws IOException {
    int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();
      for (int i = 0; i < n; i++) {
        sk.update(new String[] {String.valueOf(i)}, new String[] {"a" + i, "b" + i, "c" + i});
      }
      Files.newOutputStream(javaPath.resolve("aos_3_n" + n + "_java.sk")).write(sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingNonEmptyNoEntries() throws IOException {
    ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch(12,
        ResizeFactor.X8, 0.01f);
    sk.update(new String[] {"key1"}, new String[] {"value1"});
    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 0);
    Files.newOutputStream(javaPath.resolve("aos_1_non_empty_no_entries_java.sk")).write(sk.compact().toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingMultiKeyStrings() throws IOException {
    int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n : nArr) {
      ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();
      for (int i = 0; i < n; i++) {
        sk.update(new String[] {"key" + i, "subkey" + (i % 10)}, new String[] {"value" + i});
      }
      Files.newOutputStream(javaPath.resolve("aos_multikey_n" + n + "_java.sk")).write(sk.compact().toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingUnicodeStrings() throws IOException {
    ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();

    sk.update(new String[]{"키", "열쇠"}, new String[]{"밸류", "값"});
    sk.update(new String[]{"🔑", "🗝️"}, new String[]{"📦", "🎁"});
    sk.update(new String[]{"ключ1", "ключ2"}, new String[]{"ценить1", "ценить2"});

    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 3);

    Files.newOutputStream(javaPath.resolve("aos_unicode_java.sk")).write(sk.compact().toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingEmptyStrings() throws IOException {
    ArrayOfStringsTupleSketch sk = new ArrayOfStringsTupleSketch();

    sk.update(new String[]{""}, new String[]{"empty_key_value"});
    sk.update(new String[]{"empty_value_key"}, new String[]{""});
    sk.update(new String[]{"", ""}, new String[]{"", ""});

    assertFalse(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 3);

    Files.newOutputStream(javaPath.resolve("aos_empty_strings_java.sk")).write(sk.compact().toByteArray());
  }
}
