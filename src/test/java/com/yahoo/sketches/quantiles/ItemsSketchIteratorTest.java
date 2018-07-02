/*
 * Copyright 2018, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Comparator;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ItemsSketchIteratorTest {

  @Test
  public void emptySketch() {
    ItemsSketch<Integer> sketch = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    ItemsSketchIterator<Integer> it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void oneItemSketch() {
    ItemsSketch<Integer> sketch = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    sketch.update(0);
    ItemsSketchIterator<Integer> it = sketch.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getValue(), Integer.valueOf(0));
    Assert.assertEquals(it.getWeight(), 1);
    Assert.assertFalse(it.next());
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 100000; n += 2000) {
      ItemsSketch<Integer> sketch = ItemsSketch.getInstance(128, Comparator.naturalOrder());
      for (int i = 0; i < n; i++) { 
        sketch.update(i);
      }
      ItemsSketchIterator<Integer> it = sketch.iterator();
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
