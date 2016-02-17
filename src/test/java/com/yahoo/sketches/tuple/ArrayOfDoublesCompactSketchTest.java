/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.yahoo.sketches.memory.NativeMemory;

public class ArrayOfDoublesCompactSketchTest {
  @Test
  public void heapToDirectExactTwoDoubles() {
    UpdatableArrayOfDoublesSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(32, 2);
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    ArrayOfDoublesSketch sketch2 = new DirectArrayOfDoublesCompactSketch(new NativeMemory(sketch1.compact().toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 4.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 4.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 4.0);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    double[][] values = sketch2.getValues();
    Assert.assertEquals(values.length, 4);
    for (double[] array: values) {
      Assert.assertEquals(array.length, 2);
      Assert.assertEquals(array[0], 2.0);
      Assert.assertEquals(array[1], 4.0);
    }
  }

  @Test
  public void directToHeapExactTwoDoubles() {
    UpdatableArrayOfDoublesSketch sketch1 = new DirectArrayOfDoublesQuickSelectSketch(32, 2, new NativeMemory(new byte[1000000]));
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    ArrayOfDoublesSketch sketch2 = new HeapArrayOfDoublesCompactSketch(new NativeMemory(sketch1.compact(new NativeMemory(new byte[1000000])).toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 4.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 4.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 4.0);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    double[][] values = sketch2.getValues();
    Assert.assertEquals(values.length, 4);
    for (double[] array: values) {
      Assert.assertEquals(array.length, 2);
      Assert.assertEquals(array[0], 2.0);
      Assert.assertEquals(array[1], 4.0);
    }
  }
}
