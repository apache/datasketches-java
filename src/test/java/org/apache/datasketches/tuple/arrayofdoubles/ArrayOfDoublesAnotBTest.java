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

import static org.testng.Assert.fail;

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayOfDoublesAnotBTest {

  @Test
  public void nullOrEmptyInput() {
    // calling getResult() before calling update() should yield an empty set
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    ArrayOfDoublesSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    try {
      aNotB.update(sketch, null);
      fail();
    } catch (SketchesArgumentException e) {}

    try {
      aNotB.update(null, sketch);
      fail();
    } catch (SketchesArgumentException e) {}

    aNotB.update(sketch, sketch);
    result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void emptyA() {
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(1, new double[] {1.0});
    sketchB.update(2, new double[] {1.0});
    sketchB.update(3, new double[] {1.0});
    sketchB.update(4, new double[] {1.0});
    sketchB.update(5, new double[] {1.0});
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();

    ArrayOfDoublesSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void emptyB() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {1.0});
    sketchA.update(2, new double[] {1.0});
    sketchA.update(3, new double[] {1.0});
    sketchA.update(4, new double[] {1.0});
    sketchA.update(5, new double[] {1.0});
    ArrayOfDoublesSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();

    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 5);
    Assert.assertEquals(result.getEstimate(), 5.0);
    Assert.assertEquals(result.getLowerBound(1), 5.0);
    Assert.assertEquals(result.getUpperBound(1), 5.0);

    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test
  public void aSameAsB() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {1.0});
    sketch.update(2, new double[] {1.0});
    sketch.update(3, new double[] {1.0});
    sketch.update(4, new double[] {1.0});
    sketch.update(5, new double[] {1.0});
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketch, sketch);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void exactMode() {
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchA.update(1, new double[] {1});
    sketchA.update(2, new double[] {1});
    sketchA.update(3, new double[] {1});
    sketchA.update(4, new double[] {1});
    sketchA.update(5, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketchB.update(3, new double[] {1});
    sketchB.update(4, new double[] {1});
    sketchB.update(5, new double[] {1});
    sketchB.update(6, new double[] {1});
    sketchB.update(7, new double[] {1});

    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 2);
    Assert.assertEquals(result.getEstimate(), 2.0);
    Assert.assertEquals(result.getLowerBound(1), 2.0);
    Assert.assertEquals(result.getUpperBound(1), 2.0);
    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test
  public void exactModeTwoDoubles() {
    ArrayOfDoublesUpdatableSketchBuilder bldr = new ArrayOfDoublesUpdatableSketchBuilder();
    bldr.setNominalEntries(16);
    bldr.setNumberOfValues(2);
    bldr.setResizeFactor(ResizeFactor.X1);

    double[] valuesArr1 = {1.0, 2.0};
    double[] valuesArr2 = {2.0, 4.0};
    ArrayOfDoublesUpdatableSketch sketch1 = bldr.build();
    sketch1.update("a", valuesArr1);
    sketch1.update("b", valuesArr2);
    sketch1.update("c", valuesArr1);
    sketch1.update("d", valuesArr1);
    ArrayOfDoublesUpdatableSketch sketch2 = bldr.build();
    sketch2.update("c", valuesArr2);
    sketch2.update("d", valuesArr2);
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketch1, sketch2);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 2);
    double[] resultArr = new double[] {2.0,4.0,1.0,2.0}; //order specific to this test
    Assert.assertEquals(result.getValuesAsOneDimension(), resultArr);
  }

  @Test
  public void exactModeCustomSeed() {
    long seed = 1234567890;
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketchA.update(1, new double[] {1});
    sketchA.update(2, new double[] {1});
    sketchA.update(3, new double[] {1});
    sketchA.update(4, new double[] {1});
    sketchA.update(5, new double[] {1});

    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    sketchB.update(3, new double[] {1});
    sketchB.update(4, new double[] {1});
    sketchB.update(5, new double[] {1});
    sketchB.update(6, new double[] {1});
    sketchB.update(7, new double[] {1});

    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildAnotB();
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getRetainedEntries(), 2);
    Assert.assertEquals(result.getEstimate(), 2.0);
    Assert.assertEquals(result.getLowerBound(1), 2.0);
    Assert.assertEquals(result.getUpperBound(1), 2.0);
    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test
  public void estimationMode() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketchA.update(key++, new double[] {1});
    }

    key -= 4096; // overlap half of the entries
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      sketchB.update(key++, new double[] {1});
    }

    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }

    // same operation, but compact sketches and off-heap result
    aNotB.update(sketchA.compact(), sketchB.compact());
    result = aNotB.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test
  public void estimationModeLargeB() {
    int key = 0;
    ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 10000; i++) {
      sketchA.update(key++, new double[] {1});
    }

    key -= 2000; // overlap
    ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 100000; i++) {
      sketchB.update(key++, new double[] {1});
    }

    final int expected = 10000 - 2000;
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketchA, sketchB);
    ArrayOfDoublesCompactSketch result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), expected, expected * 0.1); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    ArrayOfDoublesSketchIterator it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }

    // same operation, but compact sketches and off-heap result
    aNotB.update(sketchA.compact(), sketchB.compact());
    result = aNotB.getResult(WritableMemory.writableWrap(new byte[1000000]));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), expected, expected * 0.1); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getValues(), new double[] {1});
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeedA() {
    ArrayOfDoublesSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(sketch, null);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeedB() {
    ArrayOfDoublesSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().buildAnotB();
    aNotB.update(null, sketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleSeeds() {
    ArrayOfDoublesSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(1).build();
    ArrayOfDoublesSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(2).build();
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().setSeed(3).buildAnotB();
    aNotB.update(sketchA, sketchB);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void incompatibleNumValues() {
    ArrayOfDoublesSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(1).build();
    ArrayOfDoublesSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().setSeed(3).buildAnotB();
    aNotB.update(sketchA, sketchB);
  }

}
