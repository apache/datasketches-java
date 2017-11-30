/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;

public class HeapArrayOfDoublesQuickSelectSketchTest {
  @Test
  public void isEmpty() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
    Assert.assertNotNull(sketch.toString());
  }

  @Test
  public void isEmptyWithSampling() {
    float samplingProbability = 0.1f;
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(samplingProbability).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong() / (double) Long.MAX_VALUE, (double) samplingProbability);
    Assert.assertEquals(sketch.getTheta(), (double) samplingProbability);
  }

  @Test
  public void sampling() {
    float samplingProbability = 0.001f;
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(samplingProbability).build();
    sketch.update("a", new double[] {1.0});
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertTrue(sketch.getUpperBound(1) > 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0, 0.0000001);
    Assert.assertEquals(sketch.getThetaLong() / (double) Long.MAX_VALUE, (double) samplingProbability);
    Assert.assertEquals(sketch.getTheta(), (double) samplingProbability);
  }

  @Test
  public void exactMode() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 4096; i++) sketch.update(i, new double[] {1.0});
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 4096.0);
    Assert.assertEquals(sketch.getUpperBound(1), 4096.0);
    Assert.assertEquals(sketch.getLowerBound(1), 4096.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);

    double[][] values = sketch.getValues();
    Assert.assertEquals(values.length, 4096);
    int count = 0;
    for (int i = 0; i < values.length; i++) if (values[i] != null) count++;
    Assert.assertEquals(count, 4096);
    for (int i = 0; i < 4096; i++) Assert.assertEquals(values[i][0], 1.0);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
  }

  @Test
  // The moment of going into the estimation mode is, to some extent, an implementation detail
  // Here we assume that presenting as many unique values as twice the nominal size of the sketch will result in estimation mode
  public void estimationMode() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 8192; i++) sketch.update(i, new double[] {1.0});
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.01);
    Assert.assertTrue(sketch.getEstimate() >= sketch.getLowerBound(1));
    Assert.assertTrue(sketch.getEstimate() < sketch.getUpperBound(1));
    Assert.assertTrue(sketch.getRetainedEntries() > 4096);
    sketch.trim();
    Assert.assertEquals(sketch.getRetainedEntries(), 4096);

    double[][] values = sketch.getValues();
    int count = 0;
    for (double[] array: values) {
      if (array != null) {
        count++;
        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], 1.0);
      }
    }
    Assert.assertEquals(count, values.length);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
  }

  @Test
  public void updatesOfAllKeyTypes() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1L, new double[] {1.0});
    sketch.update(2.0, new double[] {1.0});
    sketch.update(new byte[] {3}, new double[] {1.0});
    sketch.update(new int[] {4}, new double[] {1.0});
    sketch.update(new long[] {5L}, new double[] {1.0});
    sketch.update("a", new double[] {1.0});
    Assert.assertEquals(sketch.getEstimate(), 6.0);
  }

  @Test
  public void doubleSum() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {1.0});
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getValues()[0][0], 1.0);
    sketch.update(1, new double[] {0.7});
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getValues()[0][0], 1.7);
    sketch.update(1, new double[] {0.8});
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getValues()[0][0], 2.5);
  }

  @Test
  public void serializeDeserializeExact() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1.0});

    ArrayOfDoublesUpdatableSketch sketch2 = ArrayOfDoublesUpdatableSketch.heapify(WritableMemory.wrap(sketch1.toByteArray()));

    Assert.assertEquals(sketch2.getEstimate(), 1.0);
    double[][] values = sketch2.getValues();
    Assert.assertEquals(values.length, 1);
    Assert.assertEquals(values[0][0], 1.0);

    // the same key, so still one unique
    sketch2.update(1, new double[] {1.0});
    Assert.assertEquals(sketch2.getEstimate(), 1.0);

    sketch2.update(2, new double[] {1.0});
    Assert.assertEquals(sketch2.getEstimate(), 2.0);
  }

  @Test
  public void serializeDeserializeEstimationNoResize() throws Exception {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().
        setResizeFactor(ResizeFactor.X1).build();
    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 8192; i++) sketch1.update(i, new double[] {1.0});
    }
    byte[] byteArray = sketch1.toByteArray();
    
    //for visual testing
    //TestUtil.writeBytesToFile(byteArray, "ArrayOfDoublesQuickSelectSketch4K.data");

    ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketch.heapify(Memory.wrap(byteArray));
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
    double[][] values = sketch2.getValues();
    Assert.assertTrue(values.length >= 4096);
    for (double[] array: values) Assert.assertEquals(array[0], 10.0);
  }

  @Test
  public void serializeDeserializeSampling() {
    int sketchSize = 16384;
    int numberOfUniques = sketchSize;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(sketchSize).setSamplingProbability(0.5f).build();
    for (int i = 0; i < numberOfUniques; i++) sketch1.update(i, new double[] {1.0});
    ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketch.heapify(Memory.wrap(sketch1.toByteArray()));
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate() / numberOfUniques, 1.0, 0.01);
    Assert.assertEquals(sketch2.getRetainedEntries() / (double) numberOfUniques, 0.5, 0.01);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
  }

}
