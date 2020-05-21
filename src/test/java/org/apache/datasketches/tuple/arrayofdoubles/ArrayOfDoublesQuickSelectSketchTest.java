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

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesQuickSelectSketchTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidSamplingProbability() {
    new ArrayOfDoublesUpdatableSketchBuilder().setSamplingProbability(2f);
  }

  @Test
  public void heapToDirectExactTwoDoubles() {
    double[] valuesArr = {1.0, 2.0};
    ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch1.update("a", valuesArr);
    sketch1.update("b", valuesArr);
    sketch1.update("c", valuesArr);
    sketch1.update("d", valuesArr);
    sketch1.update("a", valuesArr);
    noopUpdates(sketch1, valuesArr);
    ArrayOfDoublesUpdatableSketch sketch2 = ArrayOfDoublesUpdatableSketch.wrap(WritableMemory.wrap(sketch1.toByteArray()));
    sketch2.update("b", valuesArr);
    sketch2.update("c", valuesArr);
    sketch2.update("d", valuesArr);
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
  public void heapToDirectWithSeed() {
    long seed = 1;
    double[] values = {1.0};

    ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketch1.update("a", values);
    sketch1.update("b", values);
    sketch1.update("c", values);

    ArrayOfDoublesUpdatableSketch sketch2 = ArrayOfDoublesUpdatableSketch.wrap(WritableMemory.wrap(sketch1.toByteArray()), seed);
    sketch2.update("b", values);
    sketch2.update("c", values);
    sketch2.update("d", values);

    Assert.assertEquals(sketch2.getEstimate(), 4.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInsertExceptions() {
    ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch1.update("a", new double[] {1.0});
  }

  @Test
  public void directToHeapExactTwoDoubles() {
    double[] valuesArr = {1.0, 2.0};
    ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().
        setNumberOfValues(2).build(WritableMemory.wrap(new byte[1000000]));
    sketch1.update("a", valuesArr);
    sketch1.update("b", valuesArr);
    sketch1.update("c", valuesArr);
    sketch1.update("d", valuesArr);
    sketch1.update("a", valuesArr);
    noopUpdates(sketch1, valuesArr);
    ArrayOfDoublesUpdatableSketch sketch2 = ArrayOfDoublesUpdatableSketch.heapify(Memory.wrap(sketch1.toByteArray()));
    sketch2.update("b", valuesArr);
    sketch2.update("c", valuesArr);
    sketch2.update("d", valuesArr);
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
  public void directToHeapWithSeed() {
    long seed = 1;
    double[] values = {1.0};

    ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build(
            WritableMemory.wrap(new byte[1000000]));
    sketch1.update("a", values);
    sketch1.update("b", values);
    sketch1.update("c", values);

    ArrayOfDoublesUpdatableSketch sketch2 = ArrayOfDoublesUpdatableSketch.heapify(Memory.wrap(sketch1.toByteArray()), seed);
    sketch2.update("b", values);
    sketch2.update("c", values);
    sketch2.update("d", values);

    Assert.assertEquals(sketch2.getEstimate(), 4.0);
  }

  @Test
  public void maxBytes() {
    Assert.assertEquals(ArrayOfDoublesQuickSelectSketch.getMaxBytes(1024, 2), 49184);
  }

  private static void noopUpdates(ArrayOfDoublesUpdatableSketch sketch, double[] valuesArr) {
    byte[] byteArr = null;
    sketch.update(byteArr, valuesArr);
    byteArr = new byte[0];
    sketch.update(byteArr, valuesArr);
    int[] intArr = null;
    sketch.update(intArr, valuesArr);
    intArr = new int[0];
    sketch.update(intArr, valuesArr);
    long[] longArr = null;
    sketch.update(longArr, valuesArr);
    longArr = new long[0];
    sketch.update(longArr, valuesArr);
  }

}
