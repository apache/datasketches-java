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

package org.apache.datasketches.tuple;

import static org.apache.datasketches.Util.getResourceBytes;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class CompactSketchWithDoubleSummaryTest {
  private final DoubleSummary.Mode mode = Mode.Sum;

  @Test
  public void emptyFromNonPublicConstructorNullArray() {
    CompactSketch<DoubleSummary> sketch =
        new CompactSketch<>(null, null, Long.MAX_VALUE, true);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
    sketch.toString();
  }

  @Test
  public void emptyFromNonPublicConstructor() {
    long[] keys = new long[0];
    DoubleSummary[] summaries =
        (DoubleSummary[]) java.lang.reflect.Array.newInstance(DoubleSummary.class, 0);
    CompactSketch<DoubleSummary> sketch =
        new CompactSketch<>(keys, summaries, Long.MAX_VALUE, true);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void emptyFromQuickSelectSketch() {
    UpdatableSketch<Double, DoubleSummary> us =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    CompactSketch<DoubleSummary> sketch = us.compact();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void exactModeFromQuickSelectSketch() {
    UpdatableSketch<Double, DoubleSummary> us =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    us.update(1, 1.0);
    us.update(2, 1.0);
    us.update(3, 1.0);
    us.update(1, 1.0);
    us.update(2, 1.0);
    us.update(3, 1.0);
    CompactSketch<DoubleSummary> sketch = us.compact();
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 3.0);
    Assert.assertEquals(sketch.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 3);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    int count = 0;
    while (it.next()) {
     Assert.assertEquals(it.getSummary().getValue(), 2.0);
     count++;
    }
    Assert.assertEquals(count, 3);
  }

  @Test
  public void serializeDeserializeSmallExact() {
    UpdatableSketch<Double, DoubleSummary> us =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    us.update("a", 1.0);
    us.update("b", 1.0);
    us.update("c", 1.0);
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    Sketch<DoubleSummary> sketch2 =
        Sketches.heapifySketch(Memory.wrap(sketch1.toByteArray()),
            new DoubleSummaryDeserializer());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 3.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch2.getRetainedEntries(), 3);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    SketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
     Assert.assertEquals(it.getSummary().getValue(), 1.0);
     count++;
    }
    Assert.assertEquals(count, 3);
  }

  @Test
  public void serializeDeserializeEstimation() throws Exception {
    UpdatableSketch<Double, DoubleSummary> us =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      us.update(i, 1.0);
    }
    us.trim();
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    byte[] bytes = sketch1.toByteArray();

    // for binary testing
    //TestUtil.writeBytesToFile(bytes, "CompactSketchWithDoubleSummary4K.sk");

    Sketch<DoubleSummary> sketch2 =
        Sketches.heapifySketch(Memory.wrap(bytes), new DoubleSummaryDeserializer());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), sketch1.getEstimate());
    Assert.assertEquals(sketch2.getThetaLong(), sketch1.getThetaLong());
    SketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeWrongType() {
    UpdatableSketch<Double, DoubleSummary> us =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      us.update(i, 1.0);
    }
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    Sketches.heapifyUpdatableSketch(Memory.wrap(sketch1.toByteArray()),
        new DoubleSummaryDeserializer(),
        new DoubleSummaryFactory(mode));
  }

  @Test
  public void serialVersion1Compatibility() throws Exception {
    byte[] bytes = getResourceBytes("CompactSketchWithDoubleSummary4K_serialVersion1.sk");
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes),
        new DoubleSummaryDeserializer());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch.getRetainedEntries(), 4096);
    int count = 0;
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

}
