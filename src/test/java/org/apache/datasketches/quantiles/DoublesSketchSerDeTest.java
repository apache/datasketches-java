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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.common.TestUtil.javaPath;

import java.io.IOException;
import java.nio.file.Files;

import org.testng.annotations.Test;

public class DoublesSketchSerDeTest {

  @Test(groups = {"generate_java_files"})
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final UpdateDoublesSketch sk = DoublesSketch.builder().build();
      for (int i = 1; i <= n; i++) sk.update(i);
      Files.newOutputStream(javaPath.resolve("quantiles_double_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

}
