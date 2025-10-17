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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.frequencies.FrequentItemsSketch;
import org.apache.datasketches.frequencies.FrequentLongsSketch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerDeCompatibilityTest {

  static final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();

  @Test
  public void itemsToLongs() {
    final FrequentItemsSketch<Long> sketch1 = new FrequentItemsSketch<>(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    final byte[] bytes = sketch1.toByteArray(serDe);
    final FrequentLongsSketch sketch2 = FrequentLongsSketch.getInstance(MemorySegment.ofArray(bytes));
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
    final FrequentLongsSketch sketch1 = new FrequentLongsSketch(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    final byte[] bytes = sketch1.toByteArray();
    final FrequentItemsSketch<Long> sketch2 = FrequentItemsSketch.getInstance(MemorySegment.ofArray(bytes), serDe);
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

}
