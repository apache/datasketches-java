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

import static org.apache.datasketches.common.TestUtil.javaPath;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.testng.annotations.Test;

public class VarOptSketchSerDeTest {

  @Test(groups = {"generate_java_files"})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final VarOptItemsSketch<Long> sk = VarOptItemsSketch.newInstance(32);
      for (int i = 1; i <= n; i++) sk.update(Long.valueOf(i), 1.0);
      Files.newOutputStream(javaPath.resolve("varopt_long_n" + n + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingStringExact() throws Exception {
    final VarOptItemsSketch<String> sketch = VarOptItemsSketch.newInstance(1024);
    for (int i = 1; i <= 200; ++i) {
      sketch.update(Integer.toString(i), 1000.0 / i);
    }
    try (final FileOutputStream file = new FileOutputStream("varopt_sketch_string_exact_java.sk")) {
      file.write(sketch.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingLongSampling() throws Exception {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(1024);
    for (long i = 0; i < 2000; ++i) {
      sketch.update(i, 1.0);
    }
    // heavy items have negative weights to allow a simple predicate to filter
    sketch.update(-1L, 100000.0);
    sketch.update(-2L, 110000.0);
    sketch.update(-3L, 120000.0);
    try (final FileOutputStream file = new FileOutputStream("varopt_sketch_long_sampling_java.sk")) {
      file.write(sketch.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

}
