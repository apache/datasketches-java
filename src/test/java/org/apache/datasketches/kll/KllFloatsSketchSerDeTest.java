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

public class KllFloatsSketchSerDeTest {

  @Test
  public void serializeDeserializeEmpty() {
    final KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance();
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllFloatsSketch sk2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertTrue(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    try { sk2.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sk2.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllFloatsSketch sk3 = KllFloatsSketch.wrap(Memory.wrap(bytes));
    assertTrue(sk3.isEmpty());
    assertEquals(sk3.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk3.getN(), sk1.getN());
    assertEquals(sk3.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    try { sk3.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sk3.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    assertEquals(sk3.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap -> byte[] -> compare byte[]
    final byte[] bytes2 = sk3.toByteArray();
    assertEquals(bytes, bytes2);
  }

  @Test
  public void serializeDeserializeOneValue() {
    final KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance();
    sk1.update(1);
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllFloatsSketch sk2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertFalse(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), 1);
    assertEquals(sk2.getN(), 1);
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk2.getMinItem(), 1.0F);
    assertEquals(sk2.getMaxItem(), 1.0F);
    assertEquals(sk2.getSerializedSizeBytes(), Long.BYTES + Float.BYTES);
    //from heap -> byte[] -> off heap
    final KllFloatsSketch sk3 = KllFloatsSketch.wrap(Memory.wrap(bytes));
    assertFalse(sk3.isEmpty());
    assertEquals(sk3.getNumRetained(), 1);
    assertEquals(sk3.getN(), 1);
    assertEquals(sk3.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk3.getMinItem(), 1.0f);
    assertEquals(sk3.getMaxItem(), 1.0f);
    assertEquals(sk3.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap -> byte[] -> compare byte[]
    final byte[] bytes2 = sk3.toByteArray();
    assertEquals(bytes, bytes2);
  }

  @Test
  public void serializeDeserializeMultipleValues() {
    final KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance();
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sk1.update(i);
    }
    assertEquals(sk1.getMinItem(), 0.0f);
    assertEquals(sk1.getMaxItem(), 999.0f);
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllFloatsSketch sk2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertFalse(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk2.getMinItem(), sk1.getMinItem());
    assertEquals(sk2.getMaxItem(), sk1.getMaxItem());
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllFloatsSketch sk3 = KllFloatsSketch.wrap(Memory.wrap(bytes));
    assertFalse(sk3.isEmpty());
    assertEquals(sk3.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk3.getN(), sk1.getN());
    assertEquals(sk3.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk3.getMinItem(), sk1.getMinItem());
    assertEquals(sk3.getMaxItem(), sk1.getMaxItem());
    assertEquals(sk3.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap -> byte[] -> compare byte[]
    final byte[] bytes2 = sk3.toByteArray();
    assertEquals(bytes, bytes2);
  }

  @Test
  public void compatibilityWithCppEstimationMode() throws Exception {
    final File file = Util.getResourceFile("kll_float_estimation_cpp.sk");
    try (MapHandle mh = Memory.map(file)) {
      final KllFloatsSketch sk = KllFloatsSketch.heapify(mh.get());
      assertEquals(sk.getMinItem(), 0);
      assertEquals(sk.getMaxItem(), 999);
      assertEquals(sk.getN(), 1000);
    }
  }

  @Test
  public void deserializeOneValueVersion1() throws Exception {
    final byte[] bytes = getResourceBytes("kll_sketch_float_one_item_v1.sk");
    final KllFloatsSketch sk = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertFalse(sk.isEmpty());
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getNumRetained(), 1);
    assertEquals(sk.getMinItem(), 1.0F);
    assertEquals(sk.getMaxItem(), 1.0F);
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
      for (int i = 1; i <= n; i++) sk.update(i);
      try (final FileOutputStream file = new FileOutputStream("kll_float_n" + n + ".sk")) {
        file.write(sk.toByteArray());
      }
    }
  }

}
