/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

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
    SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
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
    SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void emptyFromQuickSelectSketch() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    CompactSketch<DoubleSummary> sketch = us.compact();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void exactModeFromQuickSelectSketch() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
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
    SketchIterator<DoubleSummary> it = sketch.iterator();
    int count = 0;
    while (it.next()) {
     Assert.assertEquals(it.getSummary().getValue(), 2.0);
     count++;
    }
    Assert.assertEquals(count, 3);
  }

  @Test
  public void serializeDeserializeSmallExact() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    us.update("a", 1.0);
    us.update("b", 1.0);
    us.update("c", 1.0);
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    Sketch<DoubleSummary> sketch2 =
        Sketches.heapifySketch(Memory.wrap(sketch1.toByteArray()), new DoubleSummaryDeserializer());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 3.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch2.getRetainedEntries(), 3);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    SketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
     Assert.assertEquals(it.getSummary().getValue(), 1.0);
     count++;
    }
    Assert.assertEquals(count, 3);
  }

  @Test
  public void serializeDeserializeEstimation() throws Exception {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) us.update(i, 1.0);
    us.trim();
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    byte[] bytes = sketch1.toByteArray();

    // for visual testing
    //TestUtil.writeBytesToFile(bytes, "CompactSketchWithDoubleSummary4K.bin");

    Sketch<DoubleSummary> sketch2 =
        Sketches.heapifySketch(Memory.wrap(bytes), new DoubleSummaryDeserializer());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), sketch1.getEstimate());
    Assert.assertEquals(sketch2.getThetaLong(), sketch1.getThetaLong());
    SketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeWrongType() {
    UpdatableSketch<Double, DoubleSummary> us = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();
    for (int i = 0; i < 8192; i++) us.update(i, 1.0);
    CompactSketch<DoubleSummary> sketch1 = us.compact();
    Sketches.heapifyUpdatableSketch(Memory.wrap(sketch1.toByteArray()), new DoubleSummaryDeserializer(),
        new DoubleSummaryFactory());
  }

  @Test
  public void serialVersion1Compatibility() throws Exception {
    byte[] bytes = TestUtil.readBytesFromFile(getClass().getClassLoader()
        .getResource("CompactSketchWithDoubleSummary4K_serialVersion1.bin").getFile());
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes), new DoubleSummaryDeserializer());
    Assert.assertTrue(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 8192, 8192 * 0.99);
    Assert.assertEquals(sketch.getRetainedEntries(), 4096);
    int count = 0;
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

}
