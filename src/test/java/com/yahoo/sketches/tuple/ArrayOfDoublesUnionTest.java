/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class ArrayOfDoublesUnionTest {

  @Test
  public void heapExactMode() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.update(sketch1);
    union.update(sketch2);
    int maxBytes = ArrayOfDoublesUnion.getMaxBytes(union.nomEntries_, union.numValues_);
    Assert.assertEquals(maxBytes, 131104);
    ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);
    double[][] values = result.getValues();
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);

    WritableMemory wmem = WritableMemory.wrap(union.toByteArray());
    ArrayOfDoublesUnion wrappedUnion = ArrayOfDoublesSketches.wrapUnion(wmem);
    result = wrappedUnion.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);
    values = result.getValues();
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
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertTrue(result.isEstimationMode());
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
  public void heapEstimationModeFullOverlapTwoValuesAndDownsizing() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0, 2.0});
    }

    key = 0; // full overlap
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0, 2.0});
    }

    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setNumberOfValues(2).setNominalEntries(1024).buildUnion();
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertTrue(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 8192.0, 8192 * 0.01);
    Assert.assertEquals(result.getRetainedEntries(), 1024); // union was downsampled

    ArrayOfDoublesSketchIterator it = result.iterator();
    double[] expected = {2, 4};
    while (it.next()) {
      Assert.assertEquals(it.getValues(), expected, Arrays.toString(it.getValues()) + " != " + Arrays.toString(expected));
    }
  }

  @Test
  public void heapMixedMode() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 1000; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 500; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(0.2f).build();
    for (int i = 0; i < 20000; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertTrue(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 20500.0, 20500 * 0.01);
  }

  @Test
  public void heapSerializeDeserialize() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union1.update(sketch1);
    union1.update(sketch2);

    ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.heapify(Memory.wrap(union1.toByteArray()));
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
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 2.0);
    }
  }

  @Test
  public void heapDeserializeV0_9_1() throws Exception {
    byte[] bytes = TestUtil.readBytesFromFile(getClass().getClassLoader().getResource("ArrayOfDoublesUnion_v0.9.1.bin").getFile());
    ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.heapify(Memory.wrap(bytes));
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
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 2.0);
    }
  }

  @Test
  public void heapSerializeDeserializeWithSeed() {
    long seed = 1;
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildUnion();
    union1.update(sketch1);
    union1.update(sketch2);

    ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.heapify(Memory.wrap(union1.toByteArray()), seed);
    ArrayOfDoublesCompactSketch result = union2.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
  }

  @Test
  public void directSerializeDeserialize() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(
        WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(
        WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion(
        WritableMemory.wrap(new byte[1000000]));
    union1.update(sketch1);
    union1.update(sketch2);

    ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.wrap(WritableMemory.wrap(union1.toByteArray()));
    ArrayOfDoublesCompactSketch result = union2.getResult(WritableMemory.wrap(new byte[1000000]));
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
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 2.0);
    }
  }

  @Test
  public void directSerializeDeserializeWithSeed() {
    long seed = 1;
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed)
        .build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed)
        .build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().setSeed(seed)
        .buildUnion(WritableMemory.wrap(new byte[1000000]));
    union1.update(sketch1);
    union1.update(sketch2);

    ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.wrap(WritableMemory.wrap(union1.toByteArray()), seed);
    ArrayOfDoublesCompactSketch result = union2.getResult(WritableMemory.wrap(new byte[1000000]));
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
  }

  @Test
  public void directExactMode() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion(WritableMemory.wrap(new byte[1000000]));
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult(WritableMemory.wrap(new byte[1000000]));
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
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.wrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion(WritableMemory.wrap(new byte[1000000]));
    union.update(sketch1);
    union.update(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult(WritableMemory.wrap(new byte[1000000]));
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
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion heapUnion = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    heapUnion.update(sketch1);

    ArrayOfDoublesUnion directUnion = ArrayOfDoublesUnion.wrap(WritableMemory.wrap(heapUnion.toByteArray()));
    directUnion.update(sketch2);

    ArrayOfDoublesCompactSketch result = directUnion.getResult(WritableMemory.wrap(new byte[1000000]));
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
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    ArrayOfDoublesUnion directUnion = new ArrayOfDoublesSetOperationBuilder().buildUnion(WritableMemory.wrap(new byte[1000000]));
    directUnion.update(sketch1);

    ArrayOfDoublesUnion heapUnion = ArrayOfDoublesUnion.heapify(Memory.wrap(directUnion.toByteArray()));
    heapUnion.update(sketch2);

    ArrayOfDoublesCompactSketch result = heapUnion.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 3.0);
    double[][] values = result.getValues();
    Assert.assertEquals(values.length, 3);
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeeds() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setSeed(2).buildUnion();
    union.update(sketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleInputSketchFewerValues() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setNumberOfValues(2).buildUnion();
    union.update(sketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleInputSketchMoreValues() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.update(sketch);
  }

}
