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

import static org.apache.datasketches.common.TestUtil.CHECK_CPP_FILES;
import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.cppPath;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class TDigestCrossLanguageTest {

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppDouble() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("tdigest_double_n" + n + "_cpp.sk"));
      final TDigestDouble td = TDigestDouble.heapify(Memory.wrap(bytes));
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
          assertEquals(td.getRank(n / 2), 0.5, n * 0.01);
        }
      }
    }
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void deserializeFromCppFloat() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final byte[] bytes = Files.readAllBytes(cppPath.resolve("tdigest_float_n" + n + "_cpp.sk"));
      final TDigestDouble td = TDigestDouble.heapify(Memory.wrap(bytes), true);
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
          assertEquals(td.getRank(n / 2), 0.5, n * 0.01);
        }
      }
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateForCppDouble() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final TDigestDouble td = new TDigestDouble((short) 100);
      for (int i = 1; i <= n; i++) td.update(i);
      Files.newOutputStream(javaPath.resolve("tdigest_double_n" + n + "_java.sk")).write(td.toByteArray());
    }
  }

}
