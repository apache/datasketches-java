/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Comparator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.memory.NativeMemory;

public class SerDeCompatibilityTest {

  static final ArrayOfItemsSerDe<Double> serDe = new ArrayOfDoublesSerDe();

  @Test
  public void itemsToDoubles() {
    ItemsSketch<Double> sketch1 = ItemsSketch.getInstance(Comparator.naturalOrder());
    for (int i = 1; i <= 500; i++) sketch1.update((double) i);

    byte[] bytes = sketch1.toByteArray(serDe);
    DoublesSketch sketch2 = DoublesSketch.heapify(new NativeMemory(bytes));

    for (int i = 501; i <= 1000; i++) sketch2.update(i);
    Assert.assertEquals(sketch2.getN(), 1000);
    Assert.assertTrue(sketch2.getRetainedItems() < 1000);
    Assert.assertEquals(sketch2.getMinValue(), Double.valueOf(1));
    Assert.assertEquals(sketch2.getMaxValue(), Double.valueOf(1000));
    // based on ~1.7% normalized rank error for this particular case
    Assert.assertEquals(sketch2.getQuantile(0.5), Double.valueOf(500), 17);
  }

  @Test
  public void doublesToItems() {
    DoublesSketch sketch1 = DoublesSketch.builder().build();
    for (int i = 1; i <= 500; i++) sketch1.update(i);

    byte[] bytes = sketch1.toByteArray();
    ItemsSketch<Double> sketch2 = ItemsSketch.getInstance(new NativeMemory(bytes), Comparator.naturalOrder(), serDe);

    for (int i = 501; i <= 1000; i++) sketch2.update((double) i);
    Assert.assertEquals(sketch2.getN(), 1000);
    Assert.assertTrue(sketch2.getRetainedItems() < 1000);
    Assert.assertEquals(sketch2.getMinValue(), Double.valueOf(1));
    Assert.assertEquals(sketch2.getMaxValue(), Double.valueOf(1000));
    // based on ~1.7% normalized rank error for this particular case
    Assert.assertEquals(sketch2.getQuantile(0.5), Double.valueOf(500), 17);
  }

}
