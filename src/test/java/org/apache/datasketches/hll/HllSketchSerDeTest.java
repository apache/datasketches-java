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

package org.apache.datasketches.hll;

import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;

import java.io.IOException;
import java.nio.file.Files;

import org.testng.annotations.Test;

public class HllSketchSerDeTest {

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final HllSketch hll4 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_4);
      final HllSketch hll6 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_6);
      final HllSketch hll8 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_8);
      for (int i = 0; i < n; i++) hll4.update(i);
      for (int i = 0; i < n; i++) hll6.update(i);
      for (int i = 0; i < n; i++) hll8.update(i);
      Files.newOutputStream(javaPath.resolve("hll4_n" + n + "_java.sk")).write(hll4.toCompactByteArray());
      Files.newOutputStream(javaPath.resolve("hll6_n" + n + "_java.sk")).write(hll6.toCompactByteArray());
      Files.newOutputStream(javaPath.resolve("hll8_n" + n + "_java.sk")).write(hll8.toCompactByteArray());
    }
  }

}
