/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class DirectArrayOfDoublesCompactSketchTest {

  @Test
  public void emptyFromQuickSelectSketch() {
    ArrayOfDoublesUpdatableSketch us =
        new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    ArrayOfDoublesCompactSketch sketch = us.compact(WritableMemory.wrap(new byte[1000000]));
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
        new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    us.update(1, new double[] {1.0});
    us.update(2, new double[] {1.0});
    us.update(3, new double[] {1.0});
    us.update(1, new double[] {1.0});
    us.update(2, new double[] {1.0});
    us.update(3, new double[] {1.0});
    ArrayOfDoublesCompactSketch sketch = us.compact(WritableMemory.wrap(new byte[1000000]));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 3.0);
    Assert.assertEquals(sketch.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 3);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getSeedHash(), Util.computeSeedHash(DEFAULT_UPDATE_SEED));
    double[][] values = sketch.getValues();
    Assert.assertEquals(values.length, 3);
    for (double[] array: values) {
      Assert.assertEquals(array[0], 2.0);
    }
  }

  @Test
  public void serializeDeserializeSmallExact() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    us.update("a", new double[] {1.0});
    us.update("b", new double[] {1.0});
    us.update("c", new double[] {1.0});
    ArrayOfDoublesCompactSketch sketch1 = us.compact(WritableMemory.wrap(new byte[1000000]));
    ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketches.wrapSketch(WritableMemory.wrap(sketch1.toByteArray()));
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
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      us.update(i, new double[] {1.0});
    }
    ArrayOfDoublesCompactSketch sketch1 = us.compact(WritableMemory.wrap(new byte[1000000]));
    ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketches.wrapSketch(WritableMemory.wrap(sketch1.toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), sketch1.getEstimate());
    Assert.assertEquals(sketch2.getThetaLong(), sketch1.getThetaLong());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeWithWrongSeed() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      us.update(i, new double[] {1.0});
    }
    ArrayOfDoublesCompactSketch sketch1 = us.compact(WritableMemory.wrap(new byte[1000000]));
    ArrayOfDoublesSketches.wrapSketch(WritableMemory.wrap(sketch1.toByteArray()), 123);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void fromQuickSelectSketchNotEnoughMemory() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    us.update(1, new double[] {1.0});
    us.compact(WritableMemory.wrap(new byte[39]));
  }

}
