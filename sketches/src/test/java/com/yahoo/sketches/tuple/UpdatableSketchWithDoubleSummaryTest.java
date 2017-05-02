/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

public class UpdatableSketchWithDoubleSummaryTest {
  @Test
  public void isEmpty() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
  }

  @Test
  public void isEmptyWithSampling() {
    float samplingProbability = 0.1f;
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setSamplingProbability(samplingProbability).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong() / (double) Long.MAX_VALUE, (double) samplingProbability);
    Assert.assertEquals(sketch.getTheta(), (double) samplingProbability);
  }

  @Test
  public void sampling() {
    float samplingProbability = 0.001f;
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setSamplingProbability(samplingProbability).build();
    sketch.update("a", 1.0);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertTrue(sketch.getUpperBound(1) > 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0, 0.0000001);
    Assert.assertEquals(sketch.getThetaLong() / (double) Long.MAX_VALUE, (double) samplingProbability);
    Assert.assertEquals(sketch.getTheta(), (double) samplingProbability);
  }

  @Test
  public void exactMode() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 4096; i++) sketch.update(i, 1.0);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 4096.0);
    Assert.assertEquals(sketch.getUpperBound(1), 4096.0);
    Assert.assertEquals(sketch.getLowerBound(1), 4096.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);

    DoubleSummary[] summaries = sketch.getSummaries();
    Assert.assertEquals(summaries.length, 4096);
    int count = 0;
    for (int i = 0; i < summaries.length; i++) if (summaries[i] != null) count++;
    Assert.assertEquals(count, 4096);
    Assert.assertEquals(summaries[0].getValue(), 1.0);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
  }

  @Test
  // The moment of going into the estimation mode is, to some extent, an implementation detail
  // Here we assume that presenting as many unique values as twice the nominal size of the sketch will result in estimation mode
  public void estimationMode() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    for (int i = 1; i <= 8192; i++) sketch.update(i, 1.0);
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.01);
    Assert.assertTrue(sketch.getEstimate() >= sketch.getLowerBound(1));
    Assert.assertTrue(sketch.getEstimate() < sketch.getUpperBound(1));

    DoubleSummary[] summaries = sketch.getSummaries();
    Assert.assertTrue(summaries.length >= 4096);
    int count = 0;
    for (DoubleSummary summary: summaries) {
      if (summary != null) {
        count++;
        Assert.assertEquals(summary.getValue(), 1.0);
      }
    }
    Assert.assertEquals(count, summaries.length);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
}

  @Test
  public void estimationModeWithSamplingNoResizing() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setSamplingProbability(0.5f).setResizeFactor(ResizeFactor.X1).build();
    for (int i = 0; i < 16384; i++) sketch.update(i, 1.0);
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 16384, 16384 * 0.01);
    Assert.assertTrue(sketch.getEstimate() >= sketch.getLowerBound(1));
    Assert.assertTrue(sketch.getEstimate() < sketch.getUpperBound(1));
  }

  @Test
  public void updatesOfAllKeyTypes() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
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
  public void doubleSummaryDefaultSumMode() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch.update(1, 1.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 1.0);
    sketch.update(1, 0.7);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 1.7);
    sketch.update(1, 0.8);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 2.5);
  }

  @Test
  public void doubleSummaryMinMode() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory(DoubleSummary.Mode.Min)).build();
    sketch.update(1, 1.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 1.0);
    sketch.update(1, 0.7);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 0.7);
    sketch.update(1, 0.8);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 0.7);
  }
  @Test

  public void doubleSummaryMaxMode() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory(DoubleSummary.Mode.Max)).build();
    sketch.update(1, 1.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 1.0);
    sketch.update(1, 0.7);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 1.0);
    sketch.update(1, 2.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 1);
    Assert.assertEquals(sketch.getSummaries()[0].getValue(), 2.0);
  }

  @Test
  public void serializeDeserializeExact() throws Exception {
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 = Sketches.heapifyUpdatableSketch(WritableMemory.wrap(sketch1.toByteArray()));

    Assert.assertEquals(sketch2.getEstimate(), 1.0);
    DoubleSummary[] summaries = sketch2.getSummaries();
    Assert.assertEquals(summaries.length, 1);
    Assert.assertEquals(summaries[0].getValue(), 1.0);

    // the same key, so still one unique
    sketch2.update(1, 1.0);
    Assert.assertEquals(sketch2.getEstimate(), 1.0);

    sketch2.update(2, 1.0);
    Assert.assertEquals(sketch2.getEstimate(), 2.0);
  }

  @Test
  public void serializeDeserializeEstimationNoResizing() throws Exception {
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setResizeFactor(ResizeFactor.X1).build();
    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 8192; i++) sketch1.update(i, 1.0);
    }
    sketch1.trim();
    byte[] bytes = sketch1.toByteArray();
    
    //for visual testing
    //TestUtil.writeBytesToFile(bytes, "TupleSketchWithDoubleSummary4K.data");

    Sketch<DoubleSummary> sketch2 = Sketches.heapifySketch(WritableMemory.wrap(bytes));
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
    DoubleSummary[] summaries = sketch2.getSummaries();
    Assert.assertEquals(summaries.length, 4096);
    for (DoubleSummary summary: summaries) Assert.assertEquals(summary.getValue(), 10.0);
  }

  @Test
  public void serializeDeserializeSampling() throws Exception {
    int sketchSize = 16384;
    int numberOfUniques = sketchSize;
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(sketchSize).setSamplingProbability(0.5f).build();
    for (int i = 0; i < numberOfUniques; i++) sketch1.update(i, 1.0);
    Sketch<DoubleSummary> sketch2 = Sketches.heapifySketch(WritableMemory.wrap(sketch1.toByteArray()));
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate() / numberOfUniques, 1.0, 0.01);
    Assert.assertEquals(sketch2.getRetainedEntries() / (double) numberOfUniques, 0.5, 0.01);
    Assert.assertEquals(sketch1.getTheta(), sketch2.getTheta());
  }

  @Test
  public void unionExactMode() {
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    sketch2.update(2, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);

    Union<DoubleSummary> union = new Union<DoubleSummary>(new DoubleSummaryFactory());
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);
    DoubleSummary[] summaries = result.getSummaries();
    Assert.assertEquals(summaries[0].getValue(), 3.0);
    Assert.assertEquals(summaries[1].getValue(), 3.0);
    Assert.assertEquals(summaries[2].getValue(), 3.0);
  
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
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketch1.update(key++, 1.0);

    key -= 4096; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketch2.update(key++, 1.0);

    Union<DoubleSummary> union = new Union<DoubleSummary>(4096, new DoubleSummaryFactory());
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
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 1000; i++) sketch1.update(key++, 1.0);
    //System.out.println("theta1=" + sketch1.getTheta() + " " + sketch1.getThetaLong());

    key -= 500; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setSamplingProbability(0.2f).build();
    for (int i = 0; i < 20000; i++) sketch2.update(key++, 1.0);
    //System.out.println("theta2=" + sketch2.getTheta() + " " + sketch2.getThetaLong());

    Union<DoubleSummary> union = new Union<DoubleSummary>(4096, new DoubleSummaryFactory());
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 20500.0, 20500 * 0.01);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
  }

  @Test
  public void intersectionEmpty() {
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    Intersection<DoubleSummary> intersection = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
    intersection.update(sketch);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertNull(result.getSummaries());
  }

  @Test
  public void intersectionNotEmptyNoEntries() {
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setSamplingProbability(0.01f).build();
    sketch1.update("a", 1.0); // this happens to get rejected because of sampling with low probability
    Intersection<DoubleSummary> intersection = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
    intersection.update(sketch1);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0, 0.0001);
    Assert.assertTrue(result.getUpperBound(1) > 0);
    Assert.assertNull(result.getSummaries());
  }

  @Test
  public void intersectionExactWithNull() {
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(3, 1.0);

    Intersection<DoubleSummary> intersection = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
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
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(3, 1.0);

    Sketch<DoubleSummary> sketch2 = Sketches.createEmptySketch();

    Intersection<DoubleSummary> intersection = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
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
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    sketch2.update(2, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);

    Intersection<DoubleSummary> intersection = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
    intersection.update(sketch1);
    intersection.update(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    DoubleSummary[] summaries = result.getSummaries();
    Assert.assertEquals(summaries[0].getValue(), 4.0);

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
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketch1.update(key++, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketch2.update(key++, 1.0);

    Intersection<DoubleSummary> intersection = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
    intersection.update(sketch1);
    intersection.update(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertTrue(result.getUpperBound(1) > 0);
    Assert.assertNull(result.getSummaries());
  }

  @Test
  public void intersectionEstimationMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketch1.update(key++, 1.0);

    key -= 4096; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketch2.update(key++, 1.0);

    Intersection<DoubleSummary> intersection = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
    intersection.update(sketch1);
    intersection.update(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    DoubleSummary[] summaries = sketch2.getSummaries();
    for (DoubleSummary summary: summaries) Assert.assertEquals(summary.getValue(), 1.0);
  }

  @Test
  public void aNotBEmpty() {
    AnotB<DoubleSummary> aNotB = new AnotB<DoubleSummary>();
    // calling getResult() before calling update() should yield an empty set
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertNull(result.getSummaries());

    aNotB.update(null, null);
    result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertNull(result.getSummaries());

    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    aNotB.update(sketch, sketch);
    result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertNull(result.getSummaries());
  }

  @Test
  public void aNotBEmptyA() {
    UpdatableSketch<Double, DoubleSummary> sketchA = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();

    UpdatableSketch<Double, DoubleSummary> sketchB = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketchB.update(1, 1.0);
    sketchB.update(2, 1.0);

    AnotB<DoubleSummary> aNotB = new AnotB<DoubleSummary>();
    aNotB.update(sketchA, sketchB);
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertNull(result.getSummaries());
  }

  @Test
  public void aNotBEmptyB() {
    UpdatableSketch<Double, DoubleSummary> sketchA = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketchB = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();

    AnotB<DoubleSummary> aNotB = new AnotB<DoubleSummary>();
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
    UpdatableSketch<Double, DoubleSummary> sketchA = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketchA.update(1, 1.0);
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);
    sketchA.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketchB = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketchB.update(2, 1.0);
    sketchB.update(2, 1.0);
    sketchB.update(3, 1.0);
    sketchB.update(3, 1.0);

    AnotB<DoubleSummary> aNotB = new AnotB<DoubleSummary>();
    aNotB.update(sketchA, sketchB);
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    DoubleSummary[] summaries = result.getSummaries();
    Assert.assertEquals(summaries[0].getValue(), 2.0);
  }

  @Test
  public void aNotBEstimationMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketchA = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketchA.update(key++, 1.0);

    key -= 4096; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketchB = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) sketchB.update(key++, 1.0);

    AnotB<DoubleSummary> aNotB = new AnotB<DoubleSummary>();
    aNotB.update(sketchA, sketchB);
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    DoubleSummary[] summaries = sketchB.getSummaries();
    for (DoubleSummary summary: summaries) Assert.assertEquals(summary.getValue(), 1.0);

    // same thing, but compact sketches
    aNotB.update(sketchA.compact(), sketchB.compact());
    result = aNotB.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03); // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    summaries = sketchB.getSummaries();
    for (DoubleSummary summary: summaries) Assert.assertEquals(summary.getValue(), 1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidSamplingProbability() {
    new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setSamplingProbability(2f).build();
  }

}
