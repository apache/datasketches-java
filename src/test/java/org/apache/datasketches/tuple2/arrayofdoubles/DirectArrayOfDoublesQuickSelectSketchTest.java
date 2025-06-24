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

package org.apache.datasketches.tuple2.arrayofdoubles;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DirectArrayOfDoublesQuickSelectSketchTest {
  @Test
  public void isEmpty() {
    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().
        build(MemorySegment.ofArray(new byte[1000000]));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
  }

  @Test
  public void isEmptyWithSampling() {
    final float samplingProbability = 0.1f;
    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().
        setSamplingProbability(samplingProbability).
        build(MemorySegment.ofArray(new byte[1000000]));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertTrue(((DirectArrayOfDoublesQuickSelectSketch)sketch).isInSamplingMode());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
  }

  @Test
  // very low probability of being sampled
  // once the an input value is chosen so that it is rejected, the test will continue to work
  //  unless the hash function and the seed are the same
  public void sampling() {
    final float samplingProbability = 0.001f;
    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().
        setSamplingProbability(samplingProbability).
        build(MemorySegment.ofArray(new byte[1000000]));
    sketch.update("a", new double[] {1.0});
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertTrue(sketch.getUpperBound(1) > 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0, 0.0000001);
    Assert.assertEquals(
        (float)(sketch.getThetaLong() / (double) Long.MAX_VALUE), samplingProbability);
    Assert.assertEquals((float)sketch.getTheta(), samplingProbability);
  }

  @Test
  public void exactMode() {
    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().
        build(MemorySegment.ofArray(new byte[1000000]));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 0; i < 4096; i++) {
      sketch.update(i, new double[] {1.0});
    }
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 4096.0);
    Assert.assertEquals(sketch.getUpperBound(1), 4096.0);
    Assert.assertEquals(sketch.getLowerBound(1), 4096.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);

    final double[][] values = sketch.getValues();
    Assert.assertEquals(values.length, 4096);
    int count = 0;
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        count++;
      }
    }
    Assert.assertEquals(count, 4096);
    for (int i = 0; i < 4096; i++) {
      Assert.assertEquals(values[i][0], 1.0);
    }

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
  }

  @Test
  // The moment of going into the estimation mode is, to some extent, an implementation detail
  // Here we assume that presenting as many unique values as twice the nominal size of the sketch
  //  will result in estimation mode
  public void estimationMode() {
    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().
        build(MemorySegment.ofArray(new byte[4096 * 2 * 16 + 32]));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 8192; i++) {
      sketch.update(i, new double[] {1.0});
    }
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.01);
    Assert.assertTrue(sketch.getEstimate() >= sketch.getLowerBound(1));
    Assert.assertTrue(sketch.getEstimate() < sketch.getUpperBound(1));

    final double[][] values = sketch.getValues();
    Assert.assertTrue(values.length >= 4096);
    int count = 0;
    for (final double[] array: values) {
      if (array != null) {
        count++;
        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], 1.0);
      }
    }
    Assert.assertEquals(count, values.length);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getSamplingProbability(), 1.0F);
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
  }

  @Test
  public void updatesOfAllKeyTypes() {
    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().
        build(MemorySegment.ofArray(new byte[1000000]));
    sketch.update(1L, new double[] {1.0});
    sketch.update(2.0, new double[] {1.0});
    final byte[] bytes = new byte[] {3, 4};
    sketch.update(bytes, new double[] {1.0});
    sketch.update(ByteBuffer.wrap(bytes), new double[] {1.0}); // same as previous
    sketch.update(ByteBuffer.wrap(bytes, 0, 1), new double[] {1.0}); // slice
    sketch.update(new int[] {4}, new double[] {1.0});
    sketch.update(new long[] {5L}, new double[] {1.0});
    sketch.update("a", new double[] {1.0});
    Assert.assertEquals(sketch.getEstimate(), 7.0);
  }

  @Test
  public void doubleSum() {
    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().
        build(MemorySegment.ofArray(new byte[1000000]));
    sketch.update(1, new double[] {1.0});
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getValues()[0][0], 1.0);
    sketch.update(1, new double[] {0.7});
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getValues()[0][0], 1.7);
    sketch.update(1, new double[] {0.8});
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getValues()[0][0], 2.5);
  }

  @Test
  public void serializeDeserializeExact() throws Exception {
    final ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().
        build(MemorySegment.ofArray(new byte[1000000]));
    sketch1.update(1, new double[] {1.0});

    final ArrayOfDoublesUpdatableSketch sketch2 = ArrayOfDoublesUpdatableSketch.wrap(MemorySegment.ofArray(sketch1.toByteArray()));

    Assert.assertEquals(sketch2.getEstimate(), 1.0);
    final double[][] values = sketch2.getValues();
    Assert.assertEquals(values.length, 1);
    Assert.assertEquals(values[0][0], 1.0);

    // the same key, so still one unique
    sketch2.update(1, new double[] {1.0});
    Assert.assertEquals(sketch2.getEstimate(), 1.0);

    sketch2.update(2, new double[] {1.0});
    Assert.assertEquals(sketch2.getEstimate(), 2.0);
  }

  @Test
  public void serializeDeserializeEstimationNoResize() throws Exception {
    final ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().setResizeFactor(ResizeFactor.X1).
        build(MemorySegment.ofArray(new byte[1000000]));
    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 8192; i++) {
        sketch1.update(i, new double[] {1.0});
      }
    }
    final byte[] byteArray = sketch1.toByteArray();

    //for visual testing
    //TestUtil.writeBytesToFile(byteArray, "ArrayOfDoublesQuickSelectSketch4K.data");

    final ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketch.wrap(MemorySegment.ofArray(byteArray));
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
    final double[][] values = sketch2.getValues();
    Assert.assertTrue(values.length >= 4096);
    for (final double[] array: values) {
      Assert.assertEquals(array[0], 10.0);
    }
  }

  @Test
  public void serializeDeserializeSampling() {
    final int sketchSize = 16384;
    final int numberOfUniques = sketchSize;
    final ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder().
        setNominalEntries(sketchSize).setSamplingProbability(0.5f).
        build(MemorySegment.ofArray(new byte[1000000]));
    for (int i = 0; i < numberOfUniques; i++) {
      sketch1.update(i, new double[] {1.0});
    }
    final ArrayOfDoublesSketch sketch2 =
        ArrayOfDoublesSketch.wrap(MemorySegment.ofArray(sketch1.toByteArray()));
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate() / numberOfUniques, 1.0, 0.01);
    Assert.assertEquals(sketch2.getRetainedEntries() / (double) numberOfUniques, 0.5, 0.01);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void notEnoughMemory() {
    new ArrayOfDoublesUpdatableSketchBuilder().
    setNominalEntries(32).build(MemorySegment.ofArray(new byte[1055]));
  }
}
