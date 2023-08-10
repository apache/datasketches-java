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

import static org.apache.datasketches.kll.KllItemsHelper.intToFixedLengthString;
import static org.apache.datasketches.kll.KllItemsHelper.numDigits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.FileOutputStream;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class KllItemsSketchSerDeTest {
  public ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test
  public void serializeDeserializeEmpty() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    final byte[] bytes = sketch1.toByteArray();
    final KllItemsSketch<String> sketch2 = KllItemsSketch.heapify(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
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
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sketch1.update(" 1");
    final byte[] bytes = sketch1.toByteArray();
    final KllItemsSketch<String> sketch2 = KllItemsSketch.heapify(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), 1);
    assertEquals(sketch2.getN(), 1);
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), " 1");
    assertEquals(sketch2.getMaxItem(), " 1");
    assertEquals(sketch2.getSerializedSizeBytes(), sketch2.getSerializedSizeBytes());
  }

  @Test
  public void serializeDeserialize() {
    final KllItemsSketch<String> sketch1 = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sketch1.update(intToFixedLengthString(i, 4));
    }
    final byte[] bytes = sketch1.toByteArray();
    final KllItemsSketch<String> sketch2 = KllItemsSketch.heapify(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
    assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinItem(), sketch1.getMinItem());
    assertEquals(sketch2.getMaxItem(), sketch1.getMaxItem());
    assertEquals(sketch2.getSerializedSizeBytes(), sketch1.getSerializedSizeBytes());
  }

  //no cross language tests yet

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTesting() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      final int digits = numDigits(n);
      final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
      for (int i = 0; i < n; i++) sketch.update(intToFixedLengthString(i, digits));
      try (final FileOutputStream file = new FileOutputStream("kll_items_n" + n + ".sk")) {
        file.write(sketch.toByteArray());
      }
    }
  }

}
