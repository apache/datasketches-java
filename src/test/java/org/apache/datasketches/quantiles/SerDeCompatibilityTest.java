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

import java.util.Comparator;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.ArrayOfDoublesSerDe;
import org.apache.datasketches.ArrayOfItemsSerDe;

@SuppressWarnings("javadoc")
public class SerDeCompatibilityTest {

  private static final ArrayOfItemsSerDe<Double> serDe = new ArrayOfDoublesSerDe();

  @Test
  public void itemsToDoubles() {
    final ItemsSketch<Double> sketch1 = ItemsSketch.getInstance(Comparator.naturalOrder());
    for (int i = 1; i <= 500; i++) { sketch1.update((double) i); }

    final byte[] bytes = sketch1.toByteArray(serDe);
    final UpdateDoublesSketch sketch2;
    sketch2 = UpdateDoublesSketch.heapify(Memory.wrap(bytes));

    for (int i = 501; i <= 1000; i++) { sketch2.update(i); }
    Assert.assertEquals(sketch2.getN(), 1000);
    Assert.assertTrue(sketch2.getRetainedItems() < 1000);
    Assert.assertEquals(sketch2.getMinValue(), 1.0);
    Assert.assertEquals(sketch2.getMaxValue(), 1000.0);
    // based on ~1.7% normalized rank error for this particular case
    Assert.assertEquals(sketch2.getQuantile(0.5), 500.0, 17);
  }

  @Test
  public void doublesToItems() {
    final UpdateDoublesSketch sketch1 = DoublesSketch.builder().build(); //SerVer = 3
    for (int i = 1; i <= 500; i++) { sketch1.update(i); }

    final CompactDoublesSketch cs = sketch1.compact();
    DoublesSketchTest.testSketchEquality(sketch1, cs);
    //final byte[] bytes = sketch1.compact().toByteArray(); // must be compact
    final byte[] bytes = cs.toByteArray(); // must be compact

    //reconstruct with ItemsSketch
    final ItemsSketch<Double> sketch2 = ItemsSketch.getInstance(Memory.wrap(bytes),
        Comparator.naturalOrder(), serDe);

    for (int i = 501; i <= 1000; i++) { sketch2.update((double) i); }
    Assert.assertEquals(sketch2.getN(), 1000);
    Assert.assertTrue(sketch2.getRetainedItems() < 1000);
    Assert.assertEquals(sketch2.getMinValue(), 1.0);
    Assert.assertEquals(sketch2.getMaxValue(), 1000.0);
    // based on ~1.7% normalized rank error for this particular case
    Assert.assertEquals(sketch2.getQuantile(0.5), 500.0, 17);
  }

}
