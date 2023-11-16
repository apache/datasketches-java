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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.quantilescommon.DoublesSortedViewIterator;
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KllDoublesSketchIteratorTest {

  @Test
  public void emptySketch() {
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    QuantilesDoublesSketchIterator it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void oneItemSketch() {
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1);
    QuantilesDoublesSketchIterator it = sketch.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getQuantile(), 1.0);
    Assert.assertEquals(it.getWeight(), 1);
    Assert.assertFalse(it.next());
  }

  @Test
  public void twoItemSketchForIterator() {
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);
    QuantilesDoublesSketchIterator itr = sketch.iterator();
    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 2.0);
    assertEquals(itr.getWeight(), 1);

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 1.0);
    assertEquals(itr.getWeight(), 1);
  }

  @Test
  public void twoItemSketchForSortedViewIterator() {
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);
    DoublesSortedViewIterator itr = sketch.getSortedView().iterator();

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 1.0);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 0);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 1);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 0.5);

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 2.0);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 1);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 2);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0.5);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 1.0);
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 100000; n += 2000) {
      KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
      for (int i = 0; i < n; i++) {
        sketch.update(i);
      }
      QuantilesDoublesSketchIterator it = sketch.iterator();
      int count = 0;
      int weight = 0;
      while (it.next()) {
        count++;
        weight += (int)it.getWeight();
      }
      Assert.assertEquals(count, sketch.getNumRetained());
      Assert.assertEquals(weight, n);
    }
  }

}
