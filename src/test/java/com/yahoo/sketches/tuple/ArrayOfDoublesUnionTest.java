/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.yahoo.sketches.memory.NativeMemory;

public class ArrayOfDoublesUnionTest {

  @Test
  public void heapExactMode() {
    ArrayOfDoublesQuickSelectSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(8, 1);
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesQuickSelectSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(8, 1);
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion union = new HeapArrayOfDoublesUnion(8, 1);
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);
    double[][] values = result.getValues();
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);
  
    union.reset();
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
  }

  @Test
  public void heapEstimationMode() {
    int key = 0;
    ArrayOfDoublesQuickSelectSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesQuickSelectSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesUnion union = new HeapArrayOfDoublesUnion(4096, 1);
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
  
    union.reset();
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
    double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) Assert.assertEquals(values[i][0], 2.0);
  }

  @Test
  public void heapSerializeDeserialize() {
    int key = 0;
    ArrayOfDoublesQuickSelectSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesQuickSelectSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(4096, 1);
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesUnion union1 = new HeapArrayOfDoublesUnion(4096, 1);
    union1.update(sketch1);
    union1.update(sketch2);

    ArrayOfDoublesUnion union2 = new HeapArrayOfDoublesUnion(new NativeMemory(union1.toByteArray()));
    ArrayOfDoublesCompactSketch result = union2.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
  
    union2.reset();
    result = union2.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
    double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) Assert.assertEquals(values[i][0], 2.0);
  }

  @Test
  public void directExactMode() {
    ArrayOfDoublesQuickSelectSketch sketch1 = new DirectArrayOfDoublesQuickSelectSketch(8, 1, new NativeMemory(new byte[1000000]));
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesQuickSelectSketch sketch2 = new DirectArrayOfDoublesQuickSelectSketch(8, 1, new NativeMemory(new byte[1000000]));
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion union = new DirectArrayOfDoublesUnion(8, 1, new NativeMemory(new byte[1000000]));
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult(new NativeMemory(new byte[1000000]));
    Assert.assertEquals(result.getEstimate(), 3.0);
    double[][] values = result.getValues();
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);
  
    union.reset();
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
  }

  @Test
  public void directEstimationMode() {
    int key = 0;
    ArrayOfDoublesQuickSelectSketch sketch1 = new DirectArrayOfDoublesQuickSelectSketch(4096, 1, new NativeMemory(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch1.update(key++, new double[] {1.0});

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesQuickSelectSketch sketch2 = new DirectArrayOfDoublesQuickSelectSketch(4096, 1, new NativeMemory(new byte[1000000]));
    for (int i = 0; i < 8192; i++) sketch2.update(key++, new double[] {1.0});

    ArrayOfDoublesUnion union = new DirectArrayOfDoublesUnion(4096, 1, new NativeMemory(new byte[1000000]));
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult(new NativeMemory(new byte[1000000]));
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
  
    union.reset();
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
  }

  @Test
  public void heapToDirect() {
    ArrayOfDoublesQuickSelectSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesQuickSelectSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion union1 = new HeapArrayOfDoublesUnion(32, 1);
    union1.update(sketch1);

    ArrayOfDoublesUnion union2 = new DirectArrayOfDoublesUnion(new NativeMemory(union1.toByteArray()));
    union2.update(sketch2);

    ArrayOfDoublesCompactSketch result = union2.getResult(new NativeMemory(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 3.0);
    double[][] values = result.getValues();
    Assert.assertEquals(values.length, 3);
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);
  }

  @Test
  public void directToHeap() {
    ArrayOfDoublesQuickSelectSketch sketch1 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesQuickSelectSketch sketch2 = new HeapArrayOfDoublesQuickSelectSketch(32, 1);
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion union1 = new DirectArrayOfDoublesUnion(32, 1, new NativeMemory(new byte[1000000]));
    union1.update(sketch1);

    ArrayOfDoublesUnion union2 = new HeapArrayOfDoublesUnion(new NativeMemory(union1.toByteArray()));
    union2.update(sketch2);

    ArrayOfDoublesCompactSketch result = union2.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 3.0);
    double[][] values = result.getValues();
    Assert.assertEquals(values.length, 3);
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);
  }

}
