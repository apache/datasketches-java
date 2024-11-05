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

import org.apache.datasketches.quantilescommon.LongsSortedViewIterator;
import org.apache.datasketches.quantilescommon.QuantilesLongsSketchIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class KllLongsSketchIteratorTest {

  @Test
  public void emptySketch() {
    KllLongsSketch sketch = KllLongsSketch.newHeapInstance();
    QuantilesLongsSketchIterator it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void oneItemSketch() {
    KllLongsSketch sketch = KllLongsSketch.newHeapInstance();
    sketch.update(1);
    QuantilesLongsSketchIterator it = sketch.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getQuantile(), 1L);
    Assert.assertEquals(it.getWeight(), 1);
    Assert.assertFalse(it.next());
  }

  @Test
  public void twoItemSketchForIterator() {
    KllLongsSketch sketch = KllLongsSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);
    QuantilesLongsSketchIterator itr = sketch.iterator();
    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 2L);
    assertEquals(itr.getWeight(), 1);

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 1L);
    assertEquals(itr.getWeight(), 1);
  }

  @Test
  public void twoItemSketchForSortedViewIterator() {
    KllLongsSketch sketch = KllLongsSketch.newHeapInstance();
    sketch.update(1);
    sketch.update(2);
    LongsSortedViewIterator itr = sketch.getSortedView().iterator();

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 1L);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 0);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 1);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 0.5);

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), 2L);
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 1);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 2);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0.5);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 1.0);
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 100000; n += 2000) {
      KllLongsSketch sketch = KllLongsSketch.newHeapInstance();
      for (int i = 0; i < n; i++) {
        sketch.update(i);
      }
      QuantilesLongsSketchIterator it = sketch.iterator();
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
