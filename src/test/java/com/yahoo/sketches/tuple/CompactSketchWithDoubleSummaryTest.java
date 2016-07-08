/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.yahoo.sketches.memory.NativeMemory;

public class CompactSketchWithDoubleSummaryTest {
  @Test
  public void emptyFromNonPublicConstructorNullArray() {
    CompactSketch<DoubleSummary> sketch = new CompactSketch<DoubleSummary>(null, null, Long.MAX_VALUE, true);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertNull(sketch.getSummaries());
  }

  @Test
  public void emptyFromNonPublicConstructor() {
    long[] keys = new long[0];
    DoubleSummary[] summaries = (DoubleSummary[]) java.lang.reflect.Array.newInstance(DoubleSummary.class, 0);
    CompactSketch<DoubleSummary> sketch = new CompactSketch<DoubleSummary>(keys, summaries, Long.MAX_VALUE, true);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertNull(sketch.getSummaries());
  }

  @Test
  public void emptyFromQuickSelectSketch() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    CompactSketch<DoubleSummary> sketch = us.compact();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertNull(sketch.getSummaries());
  }

  @Test
  public void exactModeFromQuickSelectSketch() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    us.update(1, 1.0);
    us.update(2, 1.0);
    us.update(3, 1.0);
    us.update(1, 1.0);
    us.update(2, 1.0);
    us.update(3, 1.0);
    CompactSketch<DoubleSummary> sketch = us.compact();
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 3.0);
    Assert.assertEquals(sketch.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 3);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    DoubleSummary[] summaries = sketch.getSummaries();
    Assert.assertEquals(summaries.length, 3);
    for (DoubleSummary summary: summaries) Assert.assertEquals(summary.getValue(), 2.0);
  }

  @Test
  public void serializeDeserializeSmallExact() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    us.update("a", 1.0);
    us.update("b", 1.0);
    us.update("c", 1.0);
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    Sketch<DoubleSummary> sketch2 = Sketches.heapifySketch(new NativeMemory(sketch1.toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 3.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch2.getRetainedEntries(), 3);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    DoubleSummary[] summaries = sketch2.getSummaries();
    Assert.assertEquals(summaries.length, 3);
    for (DoubleSummary summary: summaries) Assert.assertEquals(summary.getValue(), 1.0);
  }

  @Test
  public void serializeDeserializeEstimation() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) us.update(i, 1.0);
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    Sketch<DoubleSummary> sketch2 = Sketches.heapifySketch(new NativeMemory(sketch1.toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), sketch1.getEstimate());
    Assert.assertEquals(sketch2.getThetaLong(), sketch1.getThetaLong());
    SketchIterator<DoubleSummary> it = sketch2.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void deserializeWrongType() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) us.update(i, 1.0);
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    Sketches.heapifyUpdatableSketch(new NativeMemory(sketch1.toByteArray()));
  }
}
