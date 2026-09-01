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

package org.apache.datasketches.tdigest;

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_HISTORICAL_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.UtilityIO;
import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.testng.annotations.Test;

public class TDigestCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void generateForCppDouble() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n : nArr) {
      final TDigestDouble td = new TDigestDouble((short) 100);
      for (int i = 1; i <= n; i++) {
        td.update(i);
      }
      putBytesToJavaPath("tdigest_double_n" + n + "_java.sk", td.toByteArray());
    }
  }

  @Test(groups = {CHECK_JAVA_FILES}, priority = 1)
  public void checkJava() {
    deserializeTDigest(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    deserializeTDigest(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    deserializeTDigest(GroupLanguage.GO);
  }

  private static void deserializeTDigest(final GroupLanguage lang) {
    final String[] dfArr = {"double_", "float_"};
    final String[] bufArr = {"buf_", ""};
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final String df: dfArr) {
      for (final String buf: bufArr) {
        for (final int n : nArr) {
          final String fileName = "tdigest_" + df + buf + "n" + n + lang.sfx + ".sk";
          final byte[] bytes = getFileBytes(lang.pth, fileName);
          if (bytes.length == 0) { continue;}
          //System.out.println(fileName);
          final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes), df == "float_");
          assertTrue(n == 0 ? td.isEmpty() : !td.isEmpty());
          assertEquals(td.getTotalWeight(), n);
          if (n > 0) {
            assertEquals(td.getMinValue(), 1);
            assertEquals(td.getMaxValue(), n);
            assertEquals(td.getRank(0), 0);
            assertEquals(td.getRank(n + 1), 1);
            if (n == 1) {
              assertEquals(td.getRank(n), 0.5);
            } else {
              assertEquals(td.getRank(n / 2), 0.5, 0.05);
            }
          }
        }
      }
    }
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  public void deserializeFromReferenceImplementationDouble() {
    final byte[] bytes = UtilityIO.getTestResourceBytes("tdigest_ref_k100_n10000_double.sk");
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    final int n = 10000;
    assertEquals(td.getK(), 100);
    assertEquals(td.getTotalWeight(), n);
    assertEquals(td.getMinValue(), 0);
    assertEquals(td.getMaxValue(), n - 1);
    assertEquals(td.getRank(0), 0, 0.0001);
    assertEquals(td.getRank(n / 4), 0.25, 0.0001);
    assertEquals(td.getRank(n / 2), 0.5, 0.0001);
    assertEquals(td.getRank((n * 3) / 4), 0.75, 0.0001);
    assertEquals(td.getRank(n), 1);
  }

  @Test(groups = {CHECK_CPP_HISTORICAL_FILES})
  public void deserializeFromReferenceImplementationFloat() {
    final byte[] bytes = UtilityIO.getTestResourceBytes("tdigest_ref_k100_n10000_float.sk");
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    final int n = 10000;
    assertEquals(td.getK(), 100);
    assertEquals(td.getTotalWeight(), n);
    assertEquals(td.getMinValue(), 0);
    assertEquals(td.getMaxValue(), n - 1);
    assertEquals(td.getRank(0), 0, 0.0001);
    assertEquals(td.getRank(n / 4), 0.25, 0.0001);
    assertEquals(td.getRank(n / 2), 0.5, 0.0001);
    assertEquals(td.getRank((n * 3) / 4), 0.75, 0.0001);
    assertEquals(td.getRank(n), 1);
  }

}
