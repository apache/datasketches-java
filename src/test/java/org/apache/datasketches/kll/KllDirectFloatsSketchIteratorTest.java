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

import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KllDirectFloatsSketchIteratorTest {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();

  @Test
  public void emptySketch() {
    final KllDirectFloatsSketch sketch = getDFSketch(200, 0);
    KllFloatsSketchIterator it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void oneItemSketch() {
    final KllDirectFloatsSketch sketch = getDFSketch(200, 0);
    sketch.update(0);
    KllFloatsSketchIterator it = sketch.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getValue(), 0f);
    Assert.assertEquals(it.getWeight(), 1);
    Assert.assertFalse(it.next());
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 100000; n += 2000) {
      final KllDirectFloatsSketch sketch = getDFSketch(200, 0);
      for (int i = 0; i < n; i++) {
        sketch.update(i);
      }
      KllFloatsSketchIterator it = sketch.iterator();
      int count = 0;
      int weight = 0;
      while (it.next()) {
        count++;
        weight += it.getWeight();
      }
      Assert.assertEquals(count, sketch.getNumRetained());
      Assert.assertEquals(weight, n);
    }
  }

  private static KllDirectFloatsSketch getDFSketch(final int k, final int n) {
    KllFloatsSketch sk = new KllFloatsSketch(k);
    for (int i = 1; i <= n; i++) { sk.update(i); }
    byte[] byteArr = sk.toUpdatableByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);

    KllDirectFloatsSketch dfsk = KllDirectFloatsSketch.writableWrap(wmem, memReqSvr);
    return dfsk;
  }

}
