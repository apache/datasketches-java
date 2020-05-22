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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesCompactSketchTest {
  @Test
  public void heapToDirectExactTwoDoubles() {
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    ArrayOfDoublesSketch sketch2 = new DirectArrayOfDoublesCompactSketch(Memory.wrap(sketch1.compact().toByteArray()));
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
    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build(WritableMemory.wrap(new byte[1000000]));
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    ArrayOfDoublesSketch sketch2 = new HeapArrayOfDoublesCompactSketch(Memory.wrap(sketch1.compact(WritableMemory.wrap(new byte[1000000])).toByteArray()));
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
