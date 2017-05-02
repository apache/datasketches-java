/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class ArrayOfDoublesIntersectionTest {
  private static ArrayOfDoublesCombiner combiner = new ArrayOfDoublesCombiner() {
    @Override
    public double[] combine(double[] a, double[] b) {
      for (int i = 0; i < a.length; i++) a[i] += b[i];
      return a;
    }
  };

  @Test
  public void nullInput() {
    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.update(null, null);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void empty() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.update(sketch1, null);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void notEmptyNoEntries() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(0.01f).build();
    sketch1.update("a", new double[] {1}); // this happens to get rejected because of sampling with low probability
    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.update(sketch1, null);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void heapExactWithEmpty() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(3, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.update(sketch1, null);
    intersection.update(sketch2, null);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void directExactWithEmpty() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder()
        .build(WritableMemory.wrap(new byte[1000000]));
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(3, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder()
        .build(WritableMemory.wrap(new byte[1000000]));

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().
        buildIntersection(WritableMemory.wrap(new byte[1000000]));
    intersection.update(sketch1, null);
    intersection.update(sketch2, null);
    ArrayOfDoublesCompactSketch result = intersection.getResult(WritableMemory.wrap(new byte[1000000]));
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void heapExactMode() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(2, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1});
    sketch2.update(2, new double[] {1});
    sketch2.update(3, new double[] {1});
    sketch2.update(3, new double[] {1});

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.update(sketch1, combiner);
    intersection.update(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) Assert.assertEquals(values[i][0], 4.0);

    intersection.reset();
    intersection.update(null, null);
    result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void heapDisjointEstimationMode() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.update(sketch1, combiner);
    intersection.update(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void directDisjointEstimationMode() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().
        build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().
        build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().
        buildIntersection(WritableMemory.wrap(new byte[1000000]));
    intersection.update(sketch1, combiner);
    intersection.update(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult(WritableMemory.wrap(new byte[1000000]));
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getValues().length, 0);
  }

  @Test
  public void heapEstimationMode() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection();
    intersection.update(sketch1, combiner);
    intersection.update(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) Assert.assertEquals(values[i][0], 2.0);
  }

  @Test
  public void directEstimationMode() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().buildIntersection(WritableMemory.wrap(new byte[1000000]));
    intersection.update(sketch1, combiner);
    intersection.update(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult(WritableMemory.wrap(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) Assert.assertEquals(values[i][0], 2.0);
  }

  @Test
  public void heapExactModeCustomSeed() {
    long seed = 1234567890;

    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketch1.update(1, new double[] {1});
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(2, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketch2.update(2, new double[] {1});
    sketch2.update(2, new double[] {1});
    sketch2.update(3, new double[] {1});
    sketch2.update(3, new double[] {1});

    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildIntersection();
    intersection.update(sketch1, combiner);
    intersection.update(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) Assert.assertEquals(values[i][0], 4.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeeds() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().setSeed(2).buildIntersection();
    intersection.update(sketch, combiner);
  }
}
