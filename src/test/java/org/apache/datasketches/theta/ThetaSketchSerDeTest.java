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

package org.apache.datasketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.FileOutputStream;

import org.testng.annotations.Test;

public class ThetaSketchSerDeTest {

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final UpdateSketch sketch = UpdateSketch.builder().build();
      for (int i = 0; i < n; i++) sketch.update(i);
      try (final FileOutputStream file = new FileOutputStream("theta_n" + n + "_java.sk")) {
        file.write(sketch.compact().toByteArray());
      }
    }
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingNonEmptyNoEntries() throws Exception {
    final UpdateSketch sketch = UpdateSketch.builder().setP(0.01f).build();
    sketch.update(1);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getRetainedEntries(), 0);
    try (final FileOutputStream file = new FileOutputStream("theta_non_empty_no_entries_java.sk")) {
      file.write(sketch.compact().toByteArray());
    }
  }

}
