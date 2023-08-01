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

package org.apache.datasketches.tuple.arrayofdoubles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.FileOutputStream;

import org.testng.annotations.Test;

public class ArrayOfDoublesSketchSerDeTest {

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingOneValue() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      for (int i = 0; i < n; i++) sketch.update(i, new double[] {i});
      try (final FileOutputStream file = new FileOutputStream("aod_1_n" + n + ".sk")) {
        file.write(sketch.compact().toByteArray());
      }
    }
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingThreeValues() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(3).build();
      for (int i = 0; i < n; i++) sketch.update(i, new double[] {i, i, i});
      try (final FileOutputStream file = new FileOutputStream("aod_3_n" + n + ".sk")) {
        file.write(sketch.compact().toByteArray());
      }
    }
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingNonEmptyNoEntries() throws Exception {
    final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(0.01f).build();
    sketch.update(1, new double[] {1});
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
    try (final FileOutputStream file = new FileOutputStream("aod_1_non_empty_no_entries.sk")) {
      file.write(sketch.compact().toByteArray());
    }
  }

}
