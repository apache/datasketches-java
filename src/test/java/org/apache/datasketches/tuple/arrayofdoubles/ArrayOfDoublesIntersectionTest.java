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

import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayOfDoublesIntersectionTest {

  private static ArrayOfDoublesCombiner combiner = new ArrayOfDoublesCombiner() {

    @Override
    public double[] combine(final double[] a, final double[] b) {
      for (int i = 0; i < a.length; i++) {
        a[i] += b[i];
      }
      return a;
    }
  };

  @Test
  public void nullInput() {
    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    try {
      intersection.intersect(null, null);
      fail();
    } catch (SketchesArgumentException e) {}

  }

  @Test
  public void empty() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.intersect(sketch1, null);
    final ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void degenerateWithExact() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(0.01f).build();
    sketch1.update("a", new double[] {1}); // this happens to get rejected because of sampling with low probability
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(1, new double[] {1});

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.intersect(sketch1, null);
    intersection.intersect(sketch2, null);
    final ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty()); //Degenerate
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 184.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void heapExactWithEmpty() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(3, new double[] {1});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.intersect(sketch1, null);
    intersection.intersect(sketch2, null);
    final ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void directExactWithEmpty() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder()
        .build(WritableMemory.writableWrap(new byte[1000000]));
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(3, new double[] {1});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder()
        .build(WritableMemory.writableWrap(new byte[1000000]));

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().
        buildIntersection(WritableMemory.writableWrap(new byte[1000000]));
    intersection.intersect(sketch1, null);
    intersection.intersect(sketch2, null);
    final ArrayOfDoublesCompactSketch result = intersection.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void heapExactMode() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(2, new double[] {1});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1});
    sketch2.update(2, new double[] {1});
    sketch2.update(3, new double[] {1});
    sketch2.update(3, new double[] {1});

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.intersect(sketch1, combiner);
    intersection.intersect(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    final double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 4.0);
    }

    intersection.reset();
    try {
      intersection.intersect(null, null);
      fail();
    } catch (SketchesArgumentException e) { }
  }

  @Test
  public void heapDisjointEstimationMode() {
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.intersect(sketch1, combiner);
    intersection.intersect(sketch2, combiner);
    final ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());  //Degenerate case
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 3.0);
    Assert.assertEquals(result.getValues().length, 0);
    Assert.assertTrue(result.thetaLong_ < Long.MAX_VALUE);
  }

  @Test
  public void directDisjointEstimationMode() {
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().
        build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().
        build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().
        buildIntersection(WritableMemory.writableWrap(new byte[1000000]));
    intersection.intersect(sketch1, combiner);
    intersection.intersect(sketch2, combiner);
    final ArrayOfDoublesCompactSketch result = intersection.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 3.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void heapEstimationMode() {
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.intersect(sketch1, combiner);
    intersection.intersect(sketch2, combiner);
    final ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    final double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 2.0);
    }
  }

  @Test
  public void directEstimationMode() {
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection(WritableMemory.writableWrap(new byte[1000000]));
    intersection.intersect(sketch1, combiner);
    intersection.intersect(sketch2, combiner);
    final ArrayOfDoublesCompactSketch result = intersection.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    final double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 2.0);
    }
  }

  @Test
  public void heapExactModeCustomSeed() {
    final long seed = 1234567890;

    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketch1.update(1, new double[] {1});
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(2, new double[] {1});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketch2.update(2, new double[] {1});
    sketch2.update(2, new double[] {1});
    sketch2.update(3, new double[] {1});
    sketch2.update(3, new double[] {1});

    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildIntersection();
    intersection.intersect(sketch1, combiner);
    intersection.intersect(sketch2, combiner);
    final ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    final double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 4.0);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeeds() {
    final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().setSeed(2).buildIntersection();
    intersection.intersect(sketch, combiner);
  }
}
