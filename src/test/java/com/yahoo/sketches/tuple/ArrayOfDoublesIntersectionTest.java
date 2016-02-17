/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.yahoo.sketches.memory.NativeMemory;

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
    ArrayOfDoublesIntersection intersection = new HeapArrayOfDoublesIntersection(1);
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
    UpdatableArrayOfDoublesSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    ArrayOfDoublesIntersection intersection = new HeapArrayOfDoublesIntersection(1);
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
    UpdatableArrayOfDoublesSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(32, 0.01f, 1);
    sketch1.update("a", new double[] {1}); // this happens to get rejected because of sampling with low probability
    ArrayOfDoublesIntersection intersection = new HeapArrayOfDoublesIntersection(1);
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
  public void exactWithEmpty() {
    UpdatableArrayOfDoublesSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(3, new double[] {1});

    UpdatableArrayOfDoublesSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);

    ArrayOfDoublesIntersection intersection = new HeapArrayOfDoublesIntersection(1);
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
  public void heapExact() {
    UpdatableArrayOfDoublesSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    sketch1.update(1, new double[] {1});
    sketch1.update(1, new double[] {1});
    sketch1.update(2, new double[] {1});
    sketch1.update(2, new double[] {1});

    UpdatableArrayOfDoublesSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    sketch2.update(2, new double[] {1});
    sketch2.update(2, new double[] {1});
    sketch2.update(3, new double[] {1});
    sketch2.update(3, new double[] {1});

    ArrayOfDoublesIntersection intersection = new HeapArrayOfDoublesIntersection(1);
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
  public void disjointEstimationMode() {
    int key = 0;
    UpdatableArrayOfDoublesSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    UpdatableArrayOfDoublesSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesIntersection intersection = new HeapArrayOfDoublesIntersection(1);
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
  public void heapEstimationMode() {
    int key = 0;
    UpdatableArrayOfDoublesSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    key -= 4096; // overlap half of the entries
    UpdatableArrayOfDoublesSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesIntersection intersection = new HeapArrayOfDoublesIntersection(1);
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
    UpdatableArrayOfDoublesSketch sketch1 = new DirectArrayOfDoublesQuickSelectSketch(4096, 1, new NativeMemory(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    key -= 4096; // overlap half of the entries
    UpdatableArrayOfDoublesSketch sketch2 = new DirectArrayOfDoublesQuickSelectSketch(4096, 1, new NativeMemory(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesIntersection intersection = new DirectArrayOfDoublesIntersection(1, new NativeMemory(new byte[1000000]));
    intersection.update(sketch1, combiner);
    intersection.update(sketch2, combiner);
    ArrayOfDoublesCompactSketch result = intersection.getResult(new NativeMemory(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) Assert.assertEquals(values[i][0], 2.0);
  }
}
