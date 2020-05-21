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

package org.apache.datasketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUnion;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.datasketches.SketchesReadOnlyException;

@SuppressWarnings("javadoc")
public class ReadOnlyMemoryTest {

  @Test
  public void wrapAndTryUpdatingSketch() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    ArrayOfDoublesUpdatableSketch sketch2 = (ArrayOfDoublesUpdatableSketch)
        ArrayOfDoublesSketches.wrapSketch(Memory.wrap(sketch1.toByteArray()));
    Assert.assertEquals(sketch2.getEstimate(), 1.0);
    sketch2.toByteArray();
    boolean thrown = false;
    try {
      sketch2.update(2, new double[] {1});
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    try {
      sketch2.trim();
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void heapifyAndUpdateSketch() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update(1, new double[] {1});
    // downcasting is not recommended, for testing only
    ArrayOfDoublesUpdatableSketch sketch2 = (ArrayOfDoublesUpdatableSketch)
        ArrayOfDoublesSketches.heapifySketch(Memory.wrap(sketch1.toByteArray()));
    sketch2.update(2, new double[] {1});
    Assert.assertEquals(sketch2.getEstimate(), 2.0);
  }

  @Test
  public void wrapAndTryUpdatingUnionEstimationMode() {
    int numUniques = 10000;
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch1.update(key++, new double[] {1});
    }
    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union1.update(sketch1);
    ArrayOfDoublesUnion union2 = ArrayOfDoublesSketches.wrapUnion(Memory.wrap(union1.toByteArray()));
    ArrayOfDoublesSketch resultSketch = union2.getResult();
    Assert.assertTrue(resultSketch.isEstimationMode());
    Assert.assertEquals(resultSketch.getEstimate(), numUniques, numUniques * 0.04);

    // make sure union update actually needs to modify the union
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch2.update(key++, new double[] {1});
    }

    boolean thrown = false;
    try {
      union2.update(sketch2);
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void heapifyAndUpdateUnion() {
    int numUniques = 10000;
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch1.update(key++, new double[] {1});
    }
    ArrayOfDoublesUnion union1 = new ArrayOfDoublesSetOperationBuilder().buildUnion();
    union1.update(sketch1);
    ArrayOfDoublesUnion union2 = ArrayOfDoublesSketches.heapifyUnion(Memory.wrap(union1.toByteArray()));
    ArrayOfDoublesSketch resultSketch = union2.getResult();
    Assert.assertTrue(resultSketch.isEstimationMode());
    Assert.assertEquals(resultSketch.getEstimate(), numUniques, numUniques * 0.04);

    // make sure union update actually needs to modify the union
    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < numUniques; i++) {
      sketch2.update(key++, new double[] {1});
    }
    union2.update(sketch2);
  }

}
