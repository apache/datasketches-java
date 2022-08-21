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

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.FloatsSortedViewIterator;
import org.apache.datasketches.QuantilesFloatsSketchIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KllFloatsSketchIteratorTest {

  @Test
  public void emptySketch() {
    KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    QuantilesFloatsSketchIterator it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void twoItemSketchForIterator() {
    KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);;
    QuantilesFloatsSketchIterator itr = sketch.iterator();
    assertTrue(itr.next());

    assertEquals(itr.getValue(), 2f);
    assertEquals(itr.getWeight(), 1);

    assertTrue(itr.next());

    assertEquals(itr.getValue(), 1f);
    assertEquals(itr.getWeight(), 1);
  }

  @Test
  public void twoItemSketchForSortedViewIterator() {
    KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);;
    FloatsSortedViewIterator itr = sketch.getSortedView().iterator();

    assertTrue(itr.next());

    assertEquals(itr.getValue(), 1f);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 0);
    assertEquals(itr.getCumulativeWeight(INCLUSIVE), 1);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 0.5);

    assertTrue(itr.next());

    assertEquals(itr.getValue(), 2f);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getCumulativeWeight(EXCLUSIVE), 1);
    assertEquals(itr.getCumulativeWeight(INCLUSIVE), 2);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0.5);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 1.0);
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 100000; n += 2000) {
      KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
      for (int i = 0; i < n; i++) {
        sketch.update(i);
      }
      QuantilesFloatsSketchIterator it = sketch.iterator();
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
