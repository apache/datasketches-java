/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple.adouble;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.AnotB;
import com.yahoo.sketches.tuple.CompactSketch;
import com.yahoo.sketches.tuple.Intersection;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.adouble.DoubleSummary.Mode;

public class UpdatableSketchWithDoubleSummaryTest {

  @Test
  public void isEmpty() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertNotNull(sketch.toString());
    SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void isEmptyWithSampling() {
    float samplingProbability = 0.1f;
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory())
          .setSamplingProbability(samplingProbability).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals((float)sketch.getTheta(), samplingProbability);
    Assert.assertEquals(sketch.getTheta(), (double) samplingProbability);
  }

  @Test
  public void sampling() {
    float samplingProbability = 0.001f;
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory()).setSamplingProbability(samplingProbability).build();
    sketch.update("a", 1.0);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertTrue(sketch.getUpperBound(1) > 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0, 0.0000001);
    Assert.assertEquals((float)sketch.getTheta(),  samplingProbability);
    Assert.assertEquals(sketch.getTheta(), (double) samplingProbability);
  }

  @Test
  public void exactMode() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory()).build();
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
    SketchIterator<DoubleSummary> it = sketch.iterator();
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
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory()).build();
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 8192; i++) {
      sketch.update(i, 1.0);
    }
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.01);
    Assert.assertTrue(sketch.getEstimate() >= sketch.getLowerBound(1));
    Assert.assertTrue(sketch.getEstimate() < sketch.getUpperBound(1));

    int count = 0;
    SketchIterator<DoubleSummary> it = sketch.iterator();
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
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory())
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
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch.update(1L, 1.0);
    sketch.update(2.0, 1.0);
    byte[] bytes = { 3 };
    sketch.update(bytes, 1.0);
    int[] ints = { 4 };
    sketch.update(ints, 1.0);
    long[] longs = { 5L };
    sketch.update(longs, 1.0);
    sketch.update("a", 1.0);
    Assert.assertEquals(sketch.getEstimate(), 6.0);
  }

  @Test
  public void updateDoubleSummary() {
    DoubleSummary ds = new DoubleSummary();
    ds.update(1.0);
    Assert.assertEquals(ds.getValue(), 1.0);
  }

  @Test
  public void doubleSummaryDefaultSumMode() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory()).build();
    {
      sketch.update(1, 1.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.7);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.7);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.8);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 2.5);
      Assert.assertFalse(it.next());
    }
  }

  @Test
  public void doubleSummaryMinMode() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(DoubleSummary.Mode.Min)).build();
    {
      sketch.update(1, 1.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.7);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 0.7);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.8);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 0.7);
      Assert.assertFalse(it.next());
    }
  }
  @Test

  public void doubleSummaryMaxMode() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory(DoubleSummary.Mode.Max)).build();
    {
      sketch.update(1, 1.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 0.7);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      Assert.assertFalse(it.next());
    }
    {
      sketch.update(1, 2.0);
      Assert.assertEquals(sketch.getRetainedEntries(), 1);
      SketchIterator<DoubleSummary> it = sketch.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(it.getSummary().getValue(), 2.0);
      Assert.assertFalse(it.next());
    }
  }

  @Test
  public void serializeDeserializeExact() throws Exception {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 = Sketches.heapifyUpdatableSketch(
        Memory.wrap(sketch1.toByteArray()),
        new DoubleSummaryDeserializer(), new DoubleSummaryFactory());

    Assert.assertEquals(sketch2.getEstimate(), 1.0);
    SketchIterator<DoubleSummary> it = sketch2.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 1.0);
    Assert.assertFalse(it.next());

    // the same key, so still one unique
    sketch2.update(1, 1.0);
    Assert.assertEquals(sketch2.getEstimate(), 1.0);

    sketch2.update(2, 1.0);
    Assert.assertEquals(sketch2.getEstimate(), 2.0);
  }

  @Test
  public void serializeDeserializeEstimationNoResizing() throws Exception {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(
            new DoubleSummaryFactory()).setResizeFactor(ResizeFactor.X1).build();
    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 8192; i++) {
        sketch1.update(i, 1.0);
      }
    }
    sketch1.trim();
    byte[] bytes = sketch1.toByteArray();

    //for visual testing
    //TestUtil.writeBytesToFile(bytes, "UpdatableSketchWithDoubleSummary4K.bin");

    Sketch<DoubleSummary> sketch2 =
        Sketches.heapifySketch(Memory.wrap(bytes), new DoubleSummaryDeserializer());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
    SketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 10.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

  @Test
  public void serializeDeserializeSampling() throws Exception {
    int sketchSize = 16384;
    int numberOfUniques = sketchSize;
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory())
        .setNominalEntries(sketchSize).setSamplingProbability(0.5f).build();
    for (int i = 0; i < numberOfUniques; i++) {
      sketch1.update(i, 1.0);
    }
    Sketch<DoubleSummary> sketch2 = Sketches.heapifySketch(
        Memory.wrap(sketch1.toByteArray()), new DoubleSummaryDeserializer());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate() / numberOfUniques, 1.0, 0.01);
    Assert.assertEquals(sketch2.getRetainedEntries() / (double) numberOfUniques, 0.5, 0.01);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
  }

  @Test
  public void unionExactMode() {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    sketch2.update(2, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);

    Union<DoubleSummary> union = new Union<>(new DoubleSummarySetOperations());
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);

    SketchIterator<DoubleSummary> it = result.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 3.0);
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 3.0);
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 3.0);
    Assert.assertFalse(it.next());

    union.reset();
    result = union.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
  }

  @Test
  public void unionEstimationMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, 1.0);
    }

    key -= 4096; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, 1.0);
    }

    Union<DoubleSummary> union = new Union<>(4096, new DoubleSummarySetOperations());
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
  }

  @Test
  public void unionMixedMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 1000; i++) {
      sketch1.update(key++, 1.0);
      //System.out.println("theta1=" + sketch1.getTheta() + " " + sketch1.getThetaLong());
    }

    key -= 500; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>
          (new DoubleSummaryFactory()).setSamplingProbability(0.2f).build();
    for (int i = 0; i < 20000; i++) {
      sketch2.update(key++, 1.0);
      //System.out.println("theta2=" + sketch2.getTheta() + " " + sketch2.getThetaLong());
    }

    Union<DoubleSummary> union = new Union<>(4096, new DoubleSummarySetOperations());
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 20500.0, 20500 * 0.01);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
  }

  @Test
  public void intersectionEmpty() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations());
    intersection.update(sketch);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void intersectionNotEmptyNoEntries() {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>
          (new DoubleSummaryFactory()).setSamplingProbability(0.01f).build();
    sketch1.update("a", 1.0); // this happens to get rejected because of sampling with low probability
    Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations());
    intersection.update(sketch1);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0, 0.0001);
    Assert.assertTrue(result.getUpperBound(1) > 0);
  }

  @Test
  public void intersectionExactWithNull() {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(3, 1.0);

    Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations());
    intersection.update(sketch1);
    intersection.update(null);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void intersectionExactWithEmpty() {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(3, 1.0);

    Sketch<DoubleSummary> sketch2 = Sketches.createEmptySketch();

    Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(Mode.Sum));
    intersection.update(sketch1);
    intersection.update(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void intersectionExactMode() {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    sketch2.update(2, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);

    Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations());
    intersection.update(sketch1);
    intersection.update(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    SketchIterator<DoubleSummary> it = result.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 4.0);
    Assert.assertFalse(it.next());

    intersection.reset();
    intersection.update(null);
    result = intersection.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
}

  @Test
  public void intersectionDisjointEstimationMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, 1.0);
    }

    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, 1.0);
    }

    Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations());
    intersection.update(sketch1);
    intersection.update(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertTrue(result.getUpperBound(1) > 0);

    // an intersection with no entries must survive more updates
    intersection.update(sketch1);
    result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertTrue(result.getUpperBound(1) > 0);
  }

  @Test
  public void intersectionEstimationMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, 1.0);
    }

    key -= 4096; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, 1.0);
    }

    Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations());
    intersection.update(sketch1);
    intersection.update(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
 // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    SketchIterator<DoubleSummary> it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 2.0);
    }
  }

  @Test
  public void aNotBEmpty() {
    AnotB<DoubleSummary> aNotB = new AnotB<>();
    // calling getResult() before calling update() should yield an empty set
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    aNotB.update(null, null);
    result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);

    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    aNotB.update(sketch, sketch);
    result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void aNotBEmptyA() {
    UpdatableSketch<Double, DoubleSummary> sketchA =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

    UpdatableSketch<Double, DoubleSummary> sketchB =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketchB.update(1, 1.0);
    sketchB.update(2, 1.0);

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    aNotB.update(sketchA, sketchB);
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void aNotBEmptyB() {
    UpdatableSketch<Double, DoubleSummary> sketchA =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketchB =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    aNotB.update(sketchA, sketchB);
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 2);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 2.0);
    Assert.assertEquals(result.getLowerBound(1), 2.0);
    Assert.assertEquals(result.getUpperBound(1), 2.0);

    // same thing, but compact sketches
    aNotB.update(sketchA.compact(), sketchB.compact());
    result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 2);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 2.0);
    Assert.assertEquals(result.getLowerBound(1), 2.0);
    Assert.assertEquals(result.getUpperBound(1), 2.0);
  }

  @Test
  public void aNotBExactMode() {
    UpdatableSketch<Double, DoubleSummary> sketchA =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketchA.update(1, 1.0);
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);
    sketchA.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketchB =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    sketchB.update(2, 1.0);
    sketchB.update(2, 1.0);
    sketchB.update(3, 1.0);
    sketchB.update(3, 1.0);

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    aNotB.update(sketchA, sketchB);
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    SketchIterator<DoubleSummary> it = result.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 2.0);
    Assert.assertFalse(it.next());
  }

  @Test
  public void aNotBEstimationMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketchA =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketchA.update(key++, 1.0);
    }

    key -= 4096; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketchB =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) {
      sketchB.update(key++, 1.0);
    }

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    aNotB.update(sketchA, sketchB);
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
 // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    SketchIterator<DoubleSummary> it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
    }

    // same thing, but compact sketches
    aNotB.update(sketchA.compact(), sketchB.compact());
    result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
 // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidSamplingProbability() {
    new UpdatableSketchBuilder<>
      (new DoubleSummaryFactory()).setSamplingProbability(2f).build();
  }

//@Test
//  @Deprecated
//  public void serialVersion1Compatibility() throws Exception {
//    byte[] bytes = TestUtil.readBytesFromFile(getClass().getClassLoader()
//        .getResource("UpdatableSketchWithDoubleSummary4K_serialVersion1.bin").getFile());
//    UpdatableSketch<Double, DoubleSummary> sketch =
//        Sketches.heapifyUpdatableSketch(
//            Memory.wrap(bytes), new DoubleSummaryDeserializer(), new DoubleSummaryFactory());
//    Assert.assertTrue(sketch.isEstimationMode());
//    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.99);
//    Assert.assertEquals(sketch.getRetainedEntries(), 4096);
//    int count = 0;
//    SketchIterator<DoubleSummary> it = sketch.iterator();
//    while (it.next()) {
//      Assert.assertEquals(it.getSummary().getValue(), 10.0);
//      count++;
//    }
//    Assert.assertEquals(count, 4096);
//  }

}
