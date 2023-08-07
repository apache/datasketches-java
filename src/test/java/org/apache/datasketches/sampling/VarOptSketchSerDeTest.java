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

import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.testng.annotations.Test;

public class VarOptSketchSerDeTest {

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(32);
      for (int i = 1; i <= n; i++) sketch.update(Long.valueOf(i), 1.0);
      try (final FileOutputStream file = new FileOutputStream("varopt_long_n" + n + ".sk")) {
        file.write(sketch.toByteArray(new ArrayOfLongsSerDe()));
      }
    }
  }

}
