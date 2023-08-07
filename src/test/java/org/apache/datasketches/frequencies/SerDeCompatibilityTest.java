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

package org.apache.datasketches.frequencies;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.FileOutputStream;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerDeCompatibilityTest {

  static final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();

  @Test
  public void itemsToLongs() {
    final ItemsSketch<Long> sketch1 = new ItemsSketch<>(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    final byte[] bytes = sketch1.toByteArray(serDe);
    final LongsSketch sketch2 = LongsSketch.getInstance(WritableMemory.writableWrap(bytes));
    sketch2.update(2L);
    sketch2.update(3L);
    sketch2.update(2L);

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate(1L), 1);
    Assert.assertEquals(sketch2.getEstimate(2L), 3);
    Assert.assertEquals(sketch2.getEstimate(3L), 2);
    Assert.assertEquals(sketch2.getEstimate(4L), 1);
  }

  @Test
  public void longsToItems() {
    final LongsSketch sketch1 = new LongsSketch(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    final byte[] bytes = sketch1.toByteArray();
    final ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(WritableMemory.writableWrap(bytes), serDe);
    sketch2.update(2L);
    sketch2.update(3L);
    sketch2.update(2L);

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate(1L), 1);
    Assert.assertEquals(sketch2.getEstimate(2L), 3);
    Assert.assertEquals(sketch2.getEstimate(3L), 2);
    Assert.assertEquals(sketch2.getEstimate(4L), 1);
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingLongsSketch() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      LongsSketch sketch = new LongsSketch(64);
      for (int i = 1; i <= n; i++) sketch.update(i);
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      if (n > 10) {
        assertTrue(sketch.getMaximumError() > 0);
      } else {
        assertEquals(sketch.getMaximumError(), 0);
      }
      try (final FileOutputStream file = new FileOutputStream("frequent_long_n" + n + ".sk")) {
        file.write(sketch.toByteArray());
      }
    }
  }

  @Test(groups = {"generate"})
  public void generateBinariesForCompatibilityTestingStringsSketch() throws Exception {
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (int n: nArr) {
      ItemsSketch<String> sketch = new ItemsSketch<>(64);
      for (int i = 1; i <= n; i++) sketch.update(Integer.toString(i));
      assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
      if (n > 10) {
        assertTrue(sketch.getMaximumError() > 0);
      } else {
        assertEquals(sketch.getMaximumError(), 0);
      }
      try (final FileOutputStream file = new FileOutputStream("frequent_string_n" + n + ".sk")) {
        file.write(sketch.toByteArray(new ArrayOfStringsSerDe()));
      }
    }
  }

}
