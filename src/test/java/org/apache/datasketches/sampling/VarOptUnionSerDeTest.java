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

import java.io.FileOutputStream;

import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.testng.annotations.Test;

public class VarOptUnionSerDeTest {

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws Exception {
    final int kSmall = 16;
    final int n1 = 32;
    final int n2 = 64;
    final int kMax = 128;

    // small k sketch, but sampling
    VarOptItemsSketch<Double> sketch = VarOptItemsSketch.newInstance(kSmall);
    for (int i = 0; i < n1; ++i) {
      sketch.update(1.0 * i, 1.0);
    }
    sketch.update(-1.0, n1 * n1); // heavy items have negative weights to allow a simple predicate to filter


    final VarOptItemsUnion<Double> union = VarOptItemsUnion.newInstance(kMax);
    union.update(sketch);

    // another one, but different n to get a different per-item weight
    sketch = VarOptItemsSketch.newInstance(kSmall);
    for (int i = 0; i < n2; ++i) {
      sketch.update(1.0 * i, 1.0);
    }
    union.update(sketch);
    try (final FileOutputStream file = new FileOutputStream("varopt_union_double_sampling_java.sk")) {
      file.write(union.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

}
