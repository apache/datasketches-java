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

package org.apache.datasketches.tuple.arrayofdoubles;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesCompactSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DirectArrayOfDoublesCompactSketchTest {

  @Test
  public void emptyFromQuickSelectSketch() {
    ArrayOfDoublesUpdatableSketch us =
        new ArrayOfDoublesUpdatableSketchBuilder().build(MemorySegment.ofArray(new byte[1000000]));
    ArrayOfDoublesCompactSketch sketch = us.compact(MemorySegment.ofArray(new byte[1000000]));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertNotNull(sketch.getValues());
    Assert.assertEquals(sketch.getValues().length, 0);
    ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
  }

  @Test
  public void exactModeFromQuickSelectSketch() {
    ArrayOfDoublesUpdatableSketch us =
        new ArrayOfDoublesUpdatableSketchBuilder().build(MemorySegment.ofArray(new byte[1000000]));
    us.update(1, new double[] {1.0});
    us.update(2, new double[] {1.0});
    us.update(3, new double[] {1.0});
    us.update(1, new double[] {1.0});
    us.update(2, new double[] {1.0});
    us.update(3, new double[] {1.0});
    ArrayOfDoublesCompactSketch sketch = us.compact(MemorySegment.ofArray(new byte[1000000]));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 3.0);
    Assert.assertEquals(sketch.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 3);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getSeedHash(), Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED));
    double[][] values = sketch.getValues();
    Assert.assertEquals(values.length, 3);
    for (double[] array: values) {
      Assert.assertEquals(array[0], 2.0);
    }
  }

  @Test
  public void serializeDeserializeSmallExact() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(MemorySegment.ofArray(new byte[1000000]));
    us.update("a", new double[] {1.0});
    us.update("b", new double[] {1.0});
    us.update("c", new double[] {1.0});
    ArrayOfDoublesCompactSketch sketch1 = us.compact(MemorySegment.ofArray(new byte[1000000]));
    ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketches.wrapSketch(MemorySegment.ofArray(sketch1.toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 3.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch2.getRetainedEntries(), 3);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    double[][] values = sketch2.getValues();
    Assert.assertEquals(values.length, 3);
    for (double[] array: values) {
      Assert.assertEquals(array[0], 1.0);
    }
  }

  @Test
  public void serializeDeserializeEstimation() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(MemorySegment.ofArray(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      us.update(i, new double[] {1.0});
    }
    ArrayOfDoublesCompactSketch sketch1 = us.compact(MemorySegment.ofArray(new byte[1000000]));
    ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketches.wrapSketch(MemorySegment.ofArray(sketch1.toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), sketch1.getEstimate());
    Assert.assertEquals(sketch2.getThetaLong(), sketch1.getThetaLong());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeWithWrongSeed() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(MemorySegment.ofArray(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      us.update(i, new double[] {1.0});
    }
    ArrayOfDoublesCompactSketch sketch1 = us.compact(MemorySegment.ofArray(new byte[1000000]));
    ArrayOfDoublesSketches.wrapSketch(MemorySegment.ofArray(sketch1.toByteArray()), 123);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void fromQuickSelectSketchNotEnoughMemorySegment() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(MemorySegment.ofArray(new byte[1000000]));
    us.update(1, new double[] {1.0});
    us.compact(MemorySegment.ofArray(new byte[39]));
  }

}
