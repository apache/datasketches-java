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

@SuppressWarnings("javadoc")
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
    int maxBytes = ArrayOfDoublesUnion.getMaxBytes(
        ArrayOfDoublesSetOperationBuilder.DEFAULT_NOMINAL_ENTRIES,
        ArrayOfDoublesSetOperationBuilder.DEFAULT_NUMBER_OF_VALUES);
    Assert.assertEquals(maxBytes, 131120); // 48 bytes preamble + 2 * nominal entries * (key size + value size)
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

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void noSupportHeapifyV0_9_1() throws Exception {
    final byte[] bytes = getResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk");
    ArrayOfDoublesUnion.heapify(Memory.wrap(bytes));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void noSupportWrapV0_9_1() throws Exception {
    final byte[] bytes = getResourceBytes("ArrayOfDoublesUnion_v0.9.1.sk");
    ArrayOfDoublesUnion.wrap(WritableMemory.wrap(bytes));
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

  @Test
  public void directDruidUsageOneSketch() {
    final WritableMemory mem = WritableMemory.wrap(new byte[1000000]);
    new ArrayOfDoublesSetOperationBuilder().buildUnion(mem); // just set up memory to wrap later

    final int n = 100000; // estimation mode
    final ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n; i++) {
      sketch.update(i, new double[] {1.0});
    }
    sketch.trim(); // pretend this is a result from a union

    // as Druid wraps memory
    ArrayOfDoublesSketches.wrapUnion(mem).update(sketch.compact(WritableMemory.wrap(new byte[1000000])));

    ArrayOfDoublesSketch result = ArrayOfDoublesUnion.wrap(mem).getResult();
    Assert.assertEquals(result.getEstimate(), sketch.getEstimate());
    Assert.assertEquals(result.isEstimationMode(), sketch.isEstimationMode());
  }

  @Test
  public void directDruidUsageTwoSketches() {
    final WritableMemory mem = WritableMemory.wrap(new byte[1000000]);
    new ArrayOfDoublesSetOperationBuilder().buildUnion(mem); // just set up memory to wrap later

    int key = 0;

    final int n1 = 100000; // estimation mode
    final ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n1; i++) {
      sketch1.update(key++, new double[] {1.0});
    }
    // as Druid wraps memory
    ArrayOfDoublesSketches.wrapUnion(mem).update(sketch1.compact(WritableMemory.wrap(new byte[1000000])));

    final int n2 = 1000000; // estimation mode
    final ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n2; i++) {
      sketch2.update(key++, new double[] {1.0});
    }
    // as Druid wraps memory
    ArrayOfDoublesSketches.wrapUnion(mem).update(sketch2.compact(WritableMemory.wrap(new byte[1000000])));

    // build one sketch that must be the same as union
    key = 0; // reset to have the same keys
    final int n = n1 + n2;
    final ArrayOfDoublesUpdatableSketch expected = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < n; i++) {
      expected.update(key++, new double[] {1.0});
    }
    expected.trim(); // union result is trimmed, so we need to trim this sketch for valid comparison

    ArrayOfDoublesSketch result = ArrayOfDoublesUnion.wrap(mem).getResult();
    Assert.assertEquals(result.getEstimate(), expected.getEstimate());
    Assert.assertEquals(result.isEstimationMode(), expected.isEstimationMode());
    Assert.assertEquals(result.getUpperBound(1), expected.getUpperBound(1));
    Assert.assertEquals(result.getLowerBound(1), expected.getLowerBound(1));
    Assert.assertEquals(result.getRetainedEntries(), expected.getRetainedEntries());
    Assert.assertEquals(result.getNumValues(), expected.getNumValues());
  }

}
