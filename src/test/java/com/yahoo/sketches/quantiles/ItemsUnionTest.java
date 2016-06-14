/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import java.util.Comparator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.memory.NativeMemory;

public class ItemsUnionTest {

  @Test
  public void test() {
    ItemsUnion<Long> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    ItemsQuantilesSketch<Long> result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());

    for (int i = 1; i <= 1000; i++) union.update((long) i);
    result = union.getResult();
    Assert.assertEquals(result.getN(), 1000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(1000));
    Assert.assertEquals(result.getQuantile(0.5), 500, 17); // ~1.7% normalized rank error

    ItemsQuantilesSketch<Long> sketch1 = ItemsQuantilesSketch.getInstance(Comparator.naturalOrder());
    for (int i = 1001; i <= 2000; i++) sketch1.update((long) i);
    union.update(sketch1);
    result = union.getResult();
    Assert.assertEquals(result.getN(), 2000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(2000));
    Assert.assertEquals(result.getQuantile(0.5), 1000, 34); // ~1.7% normalized rank error

    ItemsQuantilesSketch<Long> sketch2 = ItemsQuantilesSketch.getInstance(Comparator.naturalOrder());
    for (int i = 2001; i <= 3000; i++) sketch2.update((long) i);
    ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    union.update(new NativeMemory(sketch2.toByteArray(serDe)), Comparator.naturalOrder(), serDe);
    result = union.getResultAndReset();
    Assert.assertEquals(result.getN(), 3000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(3000));
    Assert.assertEquals(result.getQuantile(0.5), 1500, 51); // ~1.7% normalized rank error

    result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());
  }

}
