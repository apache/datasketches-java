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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllLongsSketch;
import org.apache.datasketches.quantilescommon.QuantilesLongsSketchIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KllDirectLongsSketchIteratorTest {

  @Test
  public void emptySketch() {
    final KllLongsSketch sketch = getDLSketch(200, 0);
    final QuantilesLongsSketchIterator it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void oneItemSketch() {
    final KllLongsSketch sketch = getDLSketch(200, 0);
    sketch.update(0);
    final QuantilesLongsSketchIterator it = sketch.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getQuantile(), 0);
    Assert.assertEquals(it.getWeight(), 1);
    Assert.assertFalse(it.next());
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 11000; n += 2000) {
      final KllLongsSketch sketch = getDLSketch(200, 0);
      for (int i = 0; i < n; i++) {
        sketch.update(i);
      }
      final QuantilesLongsSketchIterator it = sketch.iterator();
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

  private static KllLongsSketch getDLSketch(final int k, final int n) {
    final KllLongsSketch sk = KllLongsSketch.newHeapInstance(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    final byte[] byteArr = KllHelper.toByteArray(sk, true);
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);

    return KllLongsSketch.wrap(wseg);
  }

}
