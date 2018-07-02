/*
 * Copyright 2018, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DoublesSketchIteratorTest {

  @Test
  public void emptySketch() {
    DoublesSketch sketch = DoublesSketch.builder().build();
    DoublesSketchIterator it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void oneItemSketch() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(0);
    DoublesSketchIterator it = sketch.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getValue(), 0.0);
    Assert.assertEquals(it.getWeight(), 1);
    Assert.assertFalse(it.next());
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 100000; n += 2000) {
      UpdateDoublesSketch sketch = DoublesSketch.builder().build();
      for (int i = 0; i < n; i++) { 
        sketch.update(i);
      }
      DoublesSketchIterator it = sketch.iterator();
      int count = 0;
      int weight = 0;
      while (it.next()) {
        count++;
        weight += it.getWeight();
      }
      Assert.assertEquals(count, sketch.getRetainedItems());
      Assert.assertEquals(weight, n);
    }
  }

}
