/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class ArrayOfDoublesAnotBTest {

  @Test
  public void nullOrEmptyInput() {
    // calling getResult() before calling update() should yield an empty set
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
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
  public void nullOrEmptyA() {
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(1, new double[] {1.0});
    sketchB.update(2, new double[] {1.0});
    sketchB.update(3, new double[] {1.0});
    sketchB.update(4, new double[] {1.0});
    sketchB.update(5, new double[] {1.0});
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();

    aNotB.update(null, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    ArrayOfDoublesSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    aNotB.update(sketchA, sketchB);
    result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void nullOrEmptyB() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {1.0});
    sketchA.update(2, new double[] {1.0});
    sketchA.update(3, new double[] {1.0});
    sketchA.update(4, new double[] {1.0});
    sketchA.update(5, new double[] {1.0});
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();

    aNotB.update(sketchA, null);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 5);
    Assert.assertEquals(result.getEstimate(), 5.0);
    Assert.assertEquals(result.getLowerBound(1), 5.0);
    Assert.assertEquals(result.getUpperBound(1), 5.0);
    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }

    ArrayOfDoublesSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    aNotB.update(sketchA, sketchB);
    result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 5);
    Assert.assertEquals(result.getEstimate(), 5.0);
    Assert.assertEquals(result.getLowerBound(1), 5.0);
    Assert.assertEquals(result.getUpperBound(1), 5.0);
    it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test
  public void aSameAsB() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {1.0});
    sketch.update(2, new double[] {1.0});
    sketch.update(3, new double[] {1.0});
    sketch.update(4, new double[] {1.0});
    sketch.update(5, new double[] {1.0});
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
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

    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
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
  public void exactModeCustomSeed() {
    long seed = 1234567890;
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketchA.update(1, new double[] {1});
    sketchA.update(2, new double[] {1});
    sketchA.update(3, new double[] {1});
    sketchA.update(4, new double[] {1});
    sketchA.update(5, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketchB.update(3, new double[] {1});
    sketchB.update(4, new double[] {1});
    sketchB.update(5, new double[] {1});
    sketchB.update(6, new double[] {1});
    sketchB.update(7, new double[] {1});

    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildAnotB();
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

    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
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

    // same operation, but compact sketches and off-heap result
    aNotB.update(sketchA.compact(), sketchB.compact());
    result = aNotB.getResult(WritableMemory.wrap(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeedA() {
    ArrayOfDoublesSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketch, null);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeedB() {
    ArrayOfDoublesSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(null, sketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeeds() {
    ArrayOfDoublesSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(2).build();
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().setSeed(3).buildAnotB();
    aNotB.update(sketchA, sketchB);
  }

}
