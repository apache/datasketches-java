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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.FileOutputStream;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class KllItemsSketchSerDeTest {
  public ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test
  public void serializeDeserializeEmpty() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertTrue(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    try { sk2.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sk2.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllItemsSketch<String> sk3 = KllItemsSketch.wrap(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
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
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sk1.update(" 1");
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertFalse(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), 1);
    assertEquals(sk2.getN(), 1);
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk2.getMinItem(), " 1");
    assertEquals(sk2.getMaxItem(), " 1");
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllItemsSketch<String> sk3 = KllItemsSketch.wrap(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
    assertFalse(sk3.isEmpty());
    assertEquals(sk3.getNumRetained(), 1);
    assertEquals(sk3.getN(), 1);
    assertEquals(sk3.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk3.getMinItem(), " 1");
    assertEquals(sk3.getMaxItem(), " 1");
    assertEquals(sk3.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap -> byte[] -> compare byte[]
    final byte[] bytes2 = sk3.toByteArray();
    assertEquals(bytes, bytes2);
  }

  @Test
  public void serializeDeserializeMultipleValues() {
    final KllItemsSketch<String> sk1 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sk1.update(Util.intToFixedLengthString(i, 4));
    }
    assertEquals(sk1.getMinItem(), "   0");
    assertEquals(sk1.getMaxItem(), " 999");
    //from heap -> byte[] -> heap
    final byte[] bytes = sk1.toByteArray();
    final KllItemsSketch<String> sk2 = KllItemsSketch.heapify(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sk1.getSerializedSizeBytes());
    assertFalse(sk2.isEmpty());
    assertEquals(sk2.getNumRetained(), sk1.getNumRetained());
    assertEquals(sk2.getN(), sk1.getN());
    assertEquals(sk2.getNormalizedRankError(false), sk1.getNormalizedRankError(false));
    assertEquals(sk2.getMinItem(), sk1.getMinItem());
    assertEquals(sk2.getMaxItem(), sk1.getMaxItem());
    assertEquals(sk2.getSerializedSizeBytes(), sk1.getSerializedSizeBytes());
    //from heap -> byte[] -> off heap
    final KllItemsSketch<String> sk3 = KllItemsSketch.wrap(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
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

  //no cross language tests yet

  //@Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final int digits = Util.numDigits(n);
      final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
      for (int i = 0; i < n; i++) sketch.update(Util.intToFixedLengthString(i, digits));
      try (final FileOutputStream file = new FileOutputStream("kll_string_n" + n + ".sk")) {
        file.write(sketch.toByteArray());
      }
    }
  }

}
