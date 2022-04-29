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

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

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
    ArrayOfDoublesCompactSketch csk = sketch1.compact();
    Memory mem = Memory.wrap(csk.toByteArray());
    ArrayOfDoublesSketch sketch2 = new DirectArrayOfDoublesCompactSketch(mem);
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
    ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build(WritableMemory.writableWrap(new byte[1000000]));
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    sketch1.update("a", new double[] {1, 2});
    sketch1.update("b", new double[] {1, 2});
    sketch1.update("c", new double[] {1, 2});
    sketch1.update("d", new double[] {1, 2});
    ArrayOfDoublesSketch sketch2 =
        new HeapArrayOfDoublesCompactSketch(
            Memory.wrap(sketch1.compact(WritableMemory.writableWrap(new byte[1000000])).toByteArray()));
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

  @SuppressWarnings("unused")
  @Test
  public void checkGetValuesAndKeysMethods() {
    ArrayOfDoublesUpdatableSketchBuilder bldr = new ArrayOfDoublesUpdatableSketchBuilder();
    bldr.setNominalEntries(16).setNumberOfValues(2);

    HeapArrayOfDoublesQuickSelectSketch hqssk = (HeapArrayOfDoublesQuickSelectSketch) bldr.build();
    hqssk.update("a", new double[] {1, 2});
    hqssk.update("b", new double[] {3, 4});
    hqssk.update("c", new double[] {5, 6});
    hqssk.update("d", new double[] {7, 8});
    final double[][] values = hqssk.getValues();
    final double[] values1d = hqssk.getValuesAsOneDimension();
    final long[] keys = hqssk.getKeys();

    HeapArrayOfDoublesCompactSketch hcsk = (HeapArrayOfDoublesCompactSketch)hqssk.compact();
    final double[][] values2 = hcsk.getValues();
    final double[] values1d2 = hcsk.getValuesAsOneDimension();
    final long[] keys2 = hcsk.getKeys();
    assertEquals(values2, values);
    assertEquals(values1d2, values1d);
    assertEquals(keys2, keys);

    Memory hqsskMem = Memory.wrap(hqssk.toByteArray());

    DirectArrayOfDoublesQuickSelectSketchR dqssk =
        (DirectArrayOfDoublesQuickSelectSketchR)ArrayOfDoublesSketch.wrap(hqsskMem, Util.DEFAULT_UPDATE_SEED);
    final double[][] values3 = dqssk.getValues();
    final double[] values1d3 = dqssk.getValuesAsOneDimension();
    final long[] keys3 = dqssk.getKeys();
    assertEquals(values3, values);
    assertEquals(values1d3, values1d);
    assertEquals(keys3, keys);

    Memory hcskMem = Memory.wrap(hcsk.toByteArray());

    DirectArrayOfDoublesCompactSketch dcsk2 =
        (DirectArrayOfDoublesCompactSketch)ArrayOfDoublesSketch.wrap(hcskMem, Util.DEFAULT_UPDATE_SEED);
    final double[][] values4 = dqssk.getValues();
    final double[] values1d4 = dqssk.getValuesAsOneDimension();
    final long[] keys4 = dqssk.getKeys();
    assertEquals(values4, values);
    assertEquals(values1d4, values1d);
    assertEquals(keys4, keys);
  }

}
