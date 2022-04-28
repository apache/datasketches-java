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

import static org.apache.datasketches.Util.getResourceBytes;

import java.util.Arrays;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayOfDoublesUnionTest {

  @Test
  public void heapExactMode() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.union(sketch1);
    union.union(sketch2);
    final int maxBytes = ArrayOfDoublesUnion.getMaxBytes(
        ArrayOfDoublesSetOperationBuilder.DEFAULT_NOMINAL_ENTRIES,
        ArrayOfDoublesSetOperationBuilder.DEFAULT_NUMBER_OF_VALUES);
    Assert.assertEquals(maxBytes, 131120); // 48 bytes preamble + 2 * nominal entries * (key size + value size)
    ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);
    double[][] values = result.getValues();
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);

    final WritableMemory wmem = WritableMemory.writableWrap(union.toByteArray());
    final ArrayOfDoublesUnion wrappedUnion = ArrayOfDoublesSketches.wrapUnion(wmem);
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
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.union(sketch1);
    union.union(sketch2);
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
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0, 2.0});
    }

    key = 0; // full overlap
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0, 2.0});
    }

    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setNumberOfValues(2).setNominalEntries(1024).buildUnion();
    union.union(sketch1);
    union.union(sketch2);
    final ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertTrue(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 8192.0, 8192 * 0.01);
    Assert.assertEquals(result.getRetainedEntries(), 1024); // union was downsampled

    final ArrayOfDoublesSketchIterator it = result.iterator();
    final double[] expected = {2, 4};
    while (it.next()) {
      Assert.assertEquals(it.getValues(), expected, Arrays.toString(it.getValues()) + " != " + Arrays.toString(expected));
    }
  }

  @Test
  public void heapMixedMode() {
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 1000; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 500; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(0.2f).build();
    for (int i = 0; i < 20000; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.union(sketch1);
    union.union(sketch2);
    final ArrayOfDoublesCompactSketch result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertTrue(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 20500.0, 20500 * 0.01);
  }

  @Test
  public void heapSerializeDeserialize() {
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

    final ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union1.union(sketch1);
    union1.union(sketch2);

    final ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.heapify(Memory.wrap(union1.toByteArray()));
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
    final double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 2.0);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void noSupportHeapifyV0_9_1() throws Exception {
    final byte[] bytes = getResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk");
    ArrayOfDoublesUnion.heapify(Memory.wrap(bytes));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void noSupportWrapV0_9_1() throws Exception {
    final byte[] bytes = getResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk");
    ArrayOfDoublesUnion.wrap(WritableMemory.writableWrap(bytes));
  }

  @Test
  public void heapSerializeDeserializeWithSeed() {
    final long seed = 1;
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildUnion();
    union1.union(sketch1);
    union1.union(sketch2);

    final ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.heapify(Memory.wrap(union1.toByteArray()), seed);
    final ArrayOfDoublesCompactSketch result = union2.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
  }

  @Test
  public void directSerializeDeserialize() {
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(
        WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(
        WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion(
        WritableMemory.writableWrap(new byte[1000000]));
    union1.union(sketch1);
    union1.union(sketch2);

    final ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.wrap(WritableMemory.writableWrap(union1.toByteArray()));
    ArrayOfDoublesCompactSketch result = union2.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);

    union2.reset();
    result = union2.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
    final double[][] values = result.getValues();
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i][0], 2.0);
    }
  }

  @Test
  public void directSerializeDeserializeWithSeed() {
    final long seed = 1;
    int key = 0;
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed)
        .build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed)
        .build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().setSeed(seed)
        .buildUnion(WritableMemory.writableWrap(new byte[1000000]));
    union1.union(sketch1);
    union1.union(sketch2);

    final ArrayOfDoublesUnion union2 = ArrayOfDoublesUnion.wrap(WritableMemory.writableWrap(union1.toByteArray()), seed);
    final ArrayOfDoublesCompactSketch result = union2.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
  }

  @Test
  public void directExactMode() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.writableWrap(new byte[1000000]));
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.writableWrap(new byte[1000000]));
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion(WritableMemory.writableWrap(new byte[1000000]));
    union.union(sketch1);
    union.union(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertEquals(result.getEstimate(), 3.0);
    final double[][] values = result.getValues();
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
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, new double[] {1.0});
    }

    key -= 4096; // overlap half of the entries
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build(WritableMemory.writableWrap(new byte[1000000]));
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, new double[] {1.0});
    }

    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion(WritableMemory.writableWrap(new byte[1000000]));
    union.union(sketch1);
    union.union(sketch2);
    ArrayOfDoublesCompactSketch result = union.getResult(WritableMemory.writableWrap(new byte[1000000]));
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
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    final ArrayOfDoublesUnion heapUnion = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    heapUnion.union(sketch1);

    final ArrayOfDoublesUnion directUnion = ArrayOfDoublesUnion.wrap(WritableMemory.writableWrap(heapUnion.toByteArray()));
    directUnion.union(sketch2);

    final ArrayOfDoublesCompactSketch result = directUnion.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 3.0);
    final double[][] values = result.getValues();
    Assert.assertEquals(values.length, 3);
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);
  }

  @Test
  public void directToHeap() {
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(1, new double[] {1.0});
    sketch1.update(2, new double[] {1.0});

    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update(2, new double[] {1.0});
    sketch2.update(2, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});
    sketch2.update(3, new double[] {1.0});

    final ArrayOfDoublesUnion directUnion = new ArrayOfDoublesSetOperationBuilder().buildUnion(WritableMemory.writableWrap(new byte[1000000]));
    directUnion.union(sketch1);

    final ArrayOfDoublesUnion heapUnion = ArrayOfDoublesUnion.heapify(Memory.wrap(directUnion.toByteArray()));
    heapUnion.union(sketch2);

    final ArrayOfDoublesCompactSketch result = heapUnion.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 3.0);
    final double[][] values = result.getValues();
    Assert.assertEquals(values.length, 3);
    Assert.assertEquals(values[0][0], 3.0);
    Assert.assertEquals(values[1][0], 3.0);
    Assert.assertEquals(values[2][0], 3.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeeds() {
    final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setSeed(2).buildUnion();
    union.union(sketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleInputSketchFewerValues() {
    final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setNumberOfValues(2).buildUnion();
    union.union(sketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleInputSketchMoreValues() {
    final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union.union(sketch);
  }

  @Test
  public void directDruidUsageOneSketch() {
    final WritableMemory mem = WritableMemory.writableWrap(new byte[1_000_000]);
    new ArrayOfDoublesSetOperationBuilder().buildUnion(mem); // just set up memory to wrap later

    final int n = 100_000; // estimation mode
    final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n; i++) {
      sketch.update(i, new double[] {1.0});
    }
    sketch.trim(); // pretend this is a result from a union

    // as Druid wraps memory
    WritableMemory mem2 = WritableMemory.writableWrap(new byte[1_000_000]);
    ArrayOfDoublesCompactSketch dcsk = sketch.compact(mem2);
    ArrayOfDoublesUnion union = ArrayOfDoublesSketches.wrapUnion(mem); //empty union
    union.union(dcsk); //TODO est error
    //ArrayOfDoublesSketches.wrapUnion(mem).union(sketch.compact(WritableMemory.writableWrap(new byte[1_000_000])));

    final ArrayOfDoublesSketch result = ArrayOfDoublesUnion.wrap(mem).getResult();
    Assert.assertEquals(result.getEstimate(), sketch.getEstimate());//expected [98045.91060164096] but found [4096.0]
    Assert.assertEquals(result.isEstimationMode(), sketch.isEstimationMode());
  }

  @Test
  public void directDruidUsageTwoSketches() {
    final WritableMemory mem = WritableMemory.writableWrap(new byte[1000000]);
    new ArrayOfDoublesSetOperationBuilder().buildUnion(mem); // just set up memory to wrap later

    int key = 0;

    final int n1 = 100000; // estimation mode
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n1; i++) {
      sketch1.update(key++, new double[] {1.0});
    }
    // as Druid wraps memory
    ArrayOfDoublesSketches.wrapUnion(mem).union(sketch1.compact(WritableMemory.writableWrap(new byte[1000000])));

    final int n2 = 1000000; // estimation mode
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n2; i++) {
      sketch2.update(key++, new double[] {1.0});
    }
    // as Druid wraps memory
    ArrayOfDoublesSketches.wrapUnion(mem).union(sketch2.compact(WritableMemory.writableWrap(new byte[1000000])));

    // build one sketch that must be the same as union
    key = 0; // reset to have the same keys
    final int n = n1 + n2;
    final ArrayOfDoublesUpdatableSketch expected = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n; i++) {
      expected.update(key++, new double[] {1.0});
    }
    expected.trim(); // union result is trimmed, so we need to trim this sketch for valid comparison

    final ArrayOfDoublesSketch result = ArrayOfDoublesUnion.wrap(mem).getResult();
    Assert.assertEquals(result.getEstimate(), expected.getEstimate());
    Assert.assertEquals(result.isEstimationMode(), expected.isEstimationMode());
    Assert.assertEquals(result.getUpperBound(1), expected.getUpperBound(1));
    Assert.assertEquals(result.getLowerBound(1), expected.getLowerBound(1));
    Assert.assertEquals(result.getRetainedEntries(), expected.getRetainedEntries());
    Assert.assertEquals(result.getNumValues(), expected.getNumValues());
  }

}
