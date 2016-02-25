/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;

public class ArrayOfDoublesAnotBTest {

  @Test
  public void nullOrEmptyInput() {
    // calling getResult() before calling update() should yield an empty set
    ArrayOfDoublesAnotB aNotB = new HeapArrayOfDoublesAnotB(1);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    aNotB.update(null, null);
    result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    ArrayOfDoublesSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    aNotB.update(sketch, null);
    result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    aNotB.update(null, sketch);
    result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    aNotB.update(sketch, sketch);
    result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void aSameAsB() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {1.0});
    sketch.update(2, new double[] {1.0});
    sketch.update(3, new double[] {1.0});
    sketch.update(4, new double[] {1.0});
    sketch.update(5, new double[] {1.0});
    ArrayOfDoublesAnotB aNotB = new HeapArrayOfDoublesAnotB(1);
    aNotB.update(sketch, sketch);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void exactMode() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {1});
    sketchA.update(2, new double[] {1});
    sketchA.update(3, new double[] {1});
    sketchA.update(4, new double[] {1});
    sketchA.update(5, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(3, new double[] {1});
    sketchB.update(4, new double[] {1});
    sketchB.update(5, new double[] {1});
    sketchB.update(6, new double[] {1});
    sketchB.update(7, new double[] {1});

    ArrayOfDoublesAnotB aNotB = new HeapArrayOfDoublesAnotB(1);
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 2);
    Assert.assertEquals(result.getEstimate(), 2.0);
    Assert.assertEquals(result.getLowerBound(1), 2.0);
    Assert.assertEquals(result.getUpperBound(1), 2.0);
    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test
  public void estimationMode() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) sketchA.update(key++, new double[] {1});

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) sketchB.update(key++, new double[] {1});

    ArrayOfDoublesAnotB aNotB = new HeapArrayOfDoublesAnotB(1);
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }

    // same thing, but compact sketches
    aNotB.update(sketchA.compact(), sketchB.compact());
    result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

}
