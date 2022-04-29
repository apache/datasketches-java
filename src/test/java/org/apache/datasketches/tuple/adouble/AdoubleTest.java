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

package org.apache.datasketches.tuple.adouble;

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AdoubleTest {
  private final DoubleSummary.Mode mode = Mode.Sum;

  @Test
  public void isEmpty() {
    final int lgK = 12;
    final DoubleSketch sketch = new DoubleSketch(lgK, mode);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertNotNull(sketch.toString());
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void checkLowK() {
    final UpdatableSketchBuilder<Double, DoubleSummary> bldr = new UpdatableSketchBuilder<>(
        new DoubleSummaryFactory(Mode.Sum));
    bldr.setNominalEntries(16);
    final UpdatableSketch<Double,DoubleSummary> sk = bldr.build();
    assertEquals(sk.getLgK(), 4);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void serDeTest() {
    final int lgK = 12;
    final int K = 1 << lgK;
    final DoubleSketch a1Sk = new DoubleSketch(lgK, Mode.AlwaysOne);
    final int m = 2 * K;
    for (int key = 0; key < m; key++) {
      a1Sk.update(key, 1.0);
    }
    final double est1 = a1Sk.getEstimate();
    final Memory mem = Memory.wrap(a1Sk.toByteArray());
    final DoubleSketch a1Sk2 = new DoubleSketch(mem, Mode.AlwaysOne);
    final double est2 = a1Sk2.getEstimate();
    assertEquals(est1, est2);
  }

  @Test
  public void checkStringKey() {
    final int lgK = 12;
    final int K = 1 << lgK;
    final DoubleSketch a1Sk1 = new DoubleSketch(lgK, Mode.AlwaysOne);
    final int m = K / 2;
    for (int key = 0; key < m; key++) {
      a1Sk1.update(Integer.toHexString(key), 1.0);
    }
    assertEquals(a1Sk1.getEstimate(), K / 2.0);
  }


  @Test
  public void isEmptyWithSampling() {
    final float samplingProbability = 0.1f;
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode))
          .setSamplingProbability(samplingProbability).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
  }

  @Test
  public void sampling() {
    final float samplingProbability = 0.001f;
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(mode)).setSamplingProbability(samplingProbability).build();
    sketch.update("a", 1.0);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertTrue(sketch.getUpperBound(1) > 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0, 0.0000001);
    Assert.assertEquals((float)sketch.getTheta(),  samplingProbability);
    Assert.assertEquals((float)sketch.getTheta(), samplingProbability);
  }

  @Test
  public void exactMode() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(mode)).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 4096; i++) {
      sketch.update(i, 1.0);
    }
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 4096.0);
    Assert.assertEquals(sketch.getUpperBound(1), 4096.0);
    Assert.assertEquals(sketch.getLowerBound(1), 4096.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);

    int count = 0;
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertEquals(count, 4096);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
  }

  @Test
  // The moment of going into the estimation mode is, to some extent, an implementation detail
  // Here we assume that presenting as many unique values as twice the nominal
  //   size of the sketch will result in estimation mode
  public void estimationMode() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(mode)).build();
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 8192; i++) {
      sketch.update(i, 1.0);
    }
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.01);
    Assert.assertTrue(sketch.getEstimate() >= sketch.getLowerBound(1));
    Assert.assertTrue(sketch.getEstimate() < sketch.getUpperBound(1));

    int count = 0;
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertTrue(count >= 4096);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
}

  @Test
  public void estimationModeWithSamplingNoResizing() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(mode))
              .setSamplingProbability(0.5f)
              .setResizeFactor(ResizeFactor.X1).build();
    for (int i = 0; i < 16384; i++) {
      sketch.update(i, 1.0);
    }
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 16384, 16384 * 0.01);
    Assert.assertTrue(sketch.getEstimate() >= sketch.getLowerBound(1));
    Assert.assertTrue(sketch.getEstimate() < sketch.getUpperBound(1));
  }

  @Test
  public void updatesOfAllKeyTypes() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch.update(1L, 1.0);
    sketch.update(2.0, 1.0);
    final byte[] bytes = { 3 };
    sketch.update(bytes, 1.0);
    final int[] ints = { 4 };
    sketch.update(ints, 1.0);
    final long[] longs = { 5L };
    sketch.update(longs, 1.0);
    sketch.update("a", 1.0);
    Assert.assertEquals(sketch.getEstimate(), 6.0);
  }

  @Test
  public void doubleSummaryDefaultSumMode() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(mode)).build();
    {
      sketch.update(1, 1.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.7);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.7);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.8);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 2.5);
      Assert.assertFalse(it.next());
    }
  }

  @Test
  public void doubleSummaryMinMode() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(DoubleSummary.Mode.Min)).build();
    {
      sketch.update(1, 1.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.7);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 0.7);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.8);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 0.7);
      Assert.assertFalse(it.next());
    }
  }
  @Test

  public void doubleSummaryMaxMode() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(DoubleSummary.Mode.Max)).build();
    {
      sketch.update(1, 1.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.7);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 2.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      final SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 2.0);
      Assert.assertFalse(it.next());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void serializeDeserializeExact() throws Exception {
    final UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch1.update(1, 1.0);

    final UpdatableSketch<Double, DoubleSummary> sketch2 = Sketches.heapifyUpdatableSketch(
        Memory.wrap(sketch1.toByteArray()),
        new DoubleSummaryDeserializer(), new DoubleSummaryFactory(mode));

    Assert.assertEquals(sketch2.getEstimate(), 1.0);
    final SketchIterator<DoubleSummary> it = sketch2.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 1.0);
    Assert.assertFalse(it.next());

    // the same key, so still one unique
    sketch2.update(1, 1.0);
    Assert.assertEquals(sketch2.getEstimate(), 1.0);

    sketch2.update(2, 1.0);
    Assert.assertEquals(sketch2.getEstimate(), 2.0);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void serializeDeserializeEstimationNoResizing() throws Exception {
    final UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(mode)).setResizeFactor(ResizeFactor.X1).build();
    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 8192; i++) {
        sketch1.update(i, 1.0);
      }
    }
    sketch1.trim();
    final byte[] bytes = sketch1.toByteArray();

    //for binary testing
    //TestUtil.writeBytesToFile(bytes, "UpdatableSketchWithDoubleSummary4K.sk");

    final Sketch<DoubleSummary> sketch2 =
        Sketches.heapifySketch(Memory.wrap(bytes), new DoubleSummaryDeserializer());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
    final SketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 10.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void serializeDeserializeSampling() throws Exception {
    final int sketchSize = 16384;
    final int numberOfUniques = sketchSize;
    final UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode))
        .setNominalEntries(sketchSize).setSamplingProbability(0.5f).build();
    for (int i = 0; i < numberOfUniques; i++) {
      sketch1.update(i, 1.0);
    }
    final Sketch<DoubleSummary> sketch2 = Sketches.heapifySketch(
        Memory.wrap(sketch1.toByteArray()), new DoubleSummaryDeserializer());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate() / numberOfUniques, 1.0, 0.01);
    Assert.assertEquals(sketch2.getRetainedEntries() / (double) numberOfUniques, 0.5, 0.01);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
  }

  @Test
  public void checkUpdatableSketch() {
    final DoubleSummaryFactory dsumFact = new DoubleSummaryFactory(mode);
    //DoubleSummary dsum = dsumFact.newSummary();
    final UpdatableSketchBuilder<Double, DoubleSummary> bldr = new UpdatableSketchBuilder<>(dsumFact);
    final UpdatableSketch<Double, DoubleSummary> usk = bldr.build();
    final byte[] byteArr = new byte[0];
    usk.update(byteArr, 0.0);
    final int[] intArr = new int[0];
    usk.update(intArr, 1.0);
    final long[] longArr = new long[0];
    usk.update(longArr, 2.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidSamplingProbability() {
    new UpdatableSketchBuilder<>
      (new DoubleSummaryFactory(mode)).setSamplingProbability(2f).build();
  }

}
