/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.testng.annotations.Test;

/**
 * @author Jon Malkin
 */
public class VarOptItemsSamplesTest {
  @Test
  public void compareIteratorToArrays() {
    final int k = 64;
    final int n = 1024;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, n);
    // and a few heavy items
    sketch.update(n * 10L, n * 10.0);
    sketch.update(n * 11L, n * 11.0);
    sketch.update(n * 12L, n * 12.0);

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final Long[] items = samples.items();
    final double[] weights = samples.weights();
    int i = 0;
    for (VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      assertEquals(ws.getWeight(), weights[i]);
      assertEquals(ws.getItem(), items[i]);
      ++i;
    }
  }

  @Test(expectedExceptions = ConcurrentModificationException.class)
  public void checkConcurrentModification() {
    final int k = 128;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, k);

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();

    for (VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      if (ws.getItem() > (k / 2)) { // guaranteed to exist somewhere in the sketch
        sketch.update(-1L, 1.0);
      }
    }
    fail();
  }

  @Test(expectedExceptions = ConcurrentModificationException.class)
  public void checkWeightCorrectingConcurrentModification() {
    final int k = 128;
    // sketch needs to be in sampling mode
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, 2 * k);

    final Iterator<VarOptItemsSamples<Long>.WeightedSample> iter;
    iter = sketch.getSketchSamples().getWeightCorrRIter();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      if (++i > (k / 2)) {
        sketch.update(-1L, 1.0);
      }
    }
    fail();
  }

  @Test(expectedExceptions = NoSuchElementException.class)
  public void checkReadingPastEndOfIterator() {
    final int k = 128;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, k);

    final Iterator<VarOptItemsSamples<Long>.WeightedSample> iter;
    iter = sketch.getSketchSamples().iterator();

    while (iter.hasNext()) {
      iter.next();
    }
    iter.next(); // no more elements
    fail();
  }

  @Test(expectedExceptions = NoSuchElementException.class)
  public void checkWeightCorrectionReadingPastEndOfIterator() {
    final int k = 128;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, k);

    final Iterator<VarOptItemsSamples<Long>.WeightedSample> iter;
    iter = sketch.getSketchSamples().getWeightCorrRIter();

    while (iter.hasNext()) {
      iter.next();
    }
    iter.next(); // no more elements
    fail();
  }

  @Test
  public void checkPolymorphicBaseClass() {
    // use setClass()
    final VarOptItemsSketch<Number> sketch = VarOptItemsSketch.newInstance(12);
    sketch.update(1, 0.5);
    sketch.update(2L, 1.7);
    sketch.update(3.0f, 2.0);
    sketch.update(4.0, 3.1);

    try {
      final VarOptItemsSamples<Number> samples = sketch.getSketchSamples();
      // will try to create array based on type of first item in the array and fail
      samples.items();
      fail();
    } catch (final ArrayStoreException e) {
      // expected
    }

    final VarOptItemsSamples<Number> samples = sketch.getSketchSamples();
    samples.setClass(Number.class);
    assertEquals(samples.items(0), 1);
    assertEquals(samples.items(1), 2L);
    assertEquals(samples.items(2), 3.0f);
    assertEquals(samples.items(3), 4.0);
  }

  @Test
  public void checkEmptySketch() {
    // verify what happens when n_ == 0
    final VarOptItemsSketch<String> sketch = VarOptItemsSketch.newInstance(32);
    assertEquals(sketch.getNumSamples(), 0);

    final VarOptItemsSamples<String> samples = sketch.getSketchSamples();
    assertEquals(samples.getNumSamples(), 0);
    assertNull(samples.items());
    assertNull(samples.items(0));
    assertNull(samples.weights());
    assertTrue(Double.isNaN(samples.weights(0)));
  }

  @Test
  public void checkUnderFullSketchIterator() {
    // needed to fully cover iterator's next() and hasNext() conditions
    final int k = 128;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketchTest.getUnweightedLongsVIS(k, k / 2);

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();

    for (VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      assertTrue((ws.getItem() >= 0) && (ws.getItem() < (k / 2)));
    }
  }
}
