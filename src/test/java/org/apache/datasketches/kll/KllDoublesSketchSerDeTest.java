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

package org.apache.datasketches.kll;

import static org.apache.datasketches.common.Util.getResourceBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class KllDoublesSketchSerDeTest {

  @Test
  public void serializeDeserializeEmpty() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    final byte[] bytes = sketch1.toByteArray();
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    try { sketch2.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch2.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    assertEquals(sketch2.getSerializedSizeBytes(), sketch1.getSerializedSizeBytes());
  }

  @Test
  public void serializeDeserializeOneValue() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    sketch1.update(1);
    final byte[] bytes = sketch1.toByteArray();
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), 1.0);
    assertEquals(sketch2.getMaxItem(), 1.0);
    assertEquals(sketch2.getSerializedSizeBytes(), 8 + Double.BYTES);
  }

  @Test
  public void serializeDeserialize() {
    final KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }
    final byte[] bytes = sketch1.toByteArray();
    final KllDoublesSketch sketch2 = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), sketch1.getMinItem());
    assertEquals(sketch2.getMaxItem(), sketch1.getMaxItem());
    assertEquals(sketch2.getSerializedSizeBytes(), sketch1.getSerializedSizeBytes());
  }

  @Test
  public void compatibilityWithCppEstimationMode() throws Exception {
    final File file = Util.getResourceFile("kll_double_estimation_cpp.sk");
    try (MapHandle mh = Memory.map(file)) {
      final KllDoublesSketch sketch = KllDoublesSketch.heapify(mh.get());
      assertEquals(sketch.getMinItem(), 0);
      assertEquals(sketch.getMaxItem(), 999);
      assertEquals(sketch.getN(), 1000);
    }
  }

  @Test
  public void deserializeOneValueVersion1() throws Exception {
    final byte[] bytes = getResourceBytes("kll_sketch_double_one_item_v1.sk");
    final KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(bytes));
    assertFalse(sketch.isEmpty());
    assertFalse(sketch.isEstimationMode());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getMinItem(), 1.0);
    assertEquals(sketch.getMaxItem(), 1.0);
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
      for (int i = 0; i < n; i++) sketch.update(i);
      try (final FileOutputStream file = new FileOutputStream("kll_double_n" + n + ".sk")) {
        file.write(sketch.toByteArray());
      }
    }
  }

}
