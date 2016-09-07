/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Comparator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;

public class ItemsUnionTest {
  
  @Test
  public void nullAndEmpty() {
    ItemsUnion<Integer> union = ItemsUnion.getInstance(Comparator.naturalOrder());

    // union gadget sketch is null at this point
    union.update((Integer) null);
    ItemsSketch<Integer> result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());
    Assert.assertNull(union.getResultAndReset());
    
    ItemsSketch<Integer> emptySk = ItemsSketch.getInstance(Comparator.naturalOrder());
    ItemsSketch<Integer> validSk = ItemsSketch.getInstance(Comparator.naturalOrder());
    validSk.update(1);
    union.update(validSk);
    
    union = ItemsUnion.getInstance(result);
    // internal sketch is empty at this point
    union.update((ItemsSketch<Integer>) null);
    union.update(emptySk);
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());
    union.update(validSk);
    
    
    union.reset();
    // internal sketch is null again
    union.update((ItemsSketch<Integer>) null);
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());

    // internal sketch is not null again because getResult() instantiated it
    union.update(ItemsSketch.getInstance(Comparator.naturalOrder()));
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());

    union.reset();
    // internal sketch is null again
    union.update(ItemsSketch.getInstance(Comparator.naturalOrder()));
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());
  }

  @Test
  public void nullAndEmptyInputsToNonEmptyUnion() {
    ItemsUnion<Integer> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    union.update(1);
    ItemsSketch<Integer> result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getN(), 1);
    Assert.assertEquals(result.getMinValue(), Integer.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Integer.valueOf(1));

    union.update((ItemsSketch<Integer>) null);
    result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getN(), 1);
    Assert.assertEquals(result.getMinValue(), Integer.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Integer.valueOf(1));

    union.update(ItemsSketch.getInstance(Comparator.naturalOrder()));
    result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getN(), 1);
    Assert.assertEquals(result.getMinValue(), Integer.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Integer.valueOf(1));
  }

  @Test
  public void basedOnSketch() {
    Comparator<String> comp = Comparator.naturalOrder();
    ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comp);
    ItemsUnion<String> union = ItemsUnion.getInstance(sketch);
    union.reset();
    byte[] byteArr = sketch.toByteArray(serDe);
    Memory mem = new NativeMemory(byteArr);
    union = ItemsUnion.getInstance(mem, comp, serDe);
    Assert.assertEquals(byteArr.length, 8);
    union.reset();
  }
  
  @Test
  public void sameK() {
    ItemsUnion<Long> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    ItemsSketch<Long> result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());

    for (int i = 1; i <= 1000; i++) union.update((long) i);
    result = union.getResult();
    Assert.assertEquals(result.getN(), 1000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(1000));
    Assert.assertEquals(result.getQuantile(0.5), 500, 17); // ~1.7% normalized rank error

    ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(Comparator.naturalOrder());
    for (int i = 1001; i <= 2000; i++) sketch1.update((long) i);
    union.update(sketch1);
    result = union.getResult();
    Assert.assertEquals(result.getN(), 2000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(2000));
    Assert.assertEquals(result.getQuantile(0.5), 1000, 34); // ~1.7% normalized rank error

    ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(Comparator.naturalOrder());
    for (int i = 2001; i <= 3000; i++) sketch2.update((long) i);
    ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    union.update(new NativeMemory(sketch2.toByteArray(serDe)), serDe);
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

  @Test
  public void differentK() {
    ItemsUnion<Long> union = ItemsUnion.getInstance(512, Comparator.naturalOrder());
    ItemsSketch<Long> result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());

    for (int i = 1; i <= 10000; i++) union.update((long) i);
    result = union.getResult();
    Assert.assertEquals(result.getK(), 512);
    Assert.assertEquals(result.getN(), 10000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(10000));
    Assert.assertEquals(result.getQuantile(0.5), 5000, 50); // ~0.5% normalized rank error

    ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(256, Comparator.naturalOrder());
    for (int i = 10001; i <= 20000; i++) sketch1.update((long) i);
    union.update(sketch1);
    result = union.getResult();
    Assert.assertEquals(result.getK(), 256);
    Assert.assertEquals(result.getN(), 20000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(20000));
    Assert.assertEquals(result.getQuantile(0.5), 10000, 180); // ~0.9% normalized rank error

    ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    for (int i = 20001; i <= 30000; i++) sketch2.update((long) i);
    ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    union.update(new NativeMemory(sketch2.toByteArray(serDe)), serDe);
    result = union.getResultAndReset();
    Assert.assertEquals(result.getK(), 128);
    Assert.assertEquals(result.getN(), 30000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(30000));
    Assert.assertEquals(result.getQuantile(0.5), 15000, 510); // ~1.7% normalized rank error

    result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());
  }

  @Test
  public void differentLargerK() {
    ItemsUnion<Long> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(256, Comparator.naturalOrder());
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1L);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }
  
  @Test
  public void differentSmallerK() {
    ItemsUnion<Long> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(64, Comparator.naturalOrder());
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 64);
    sketch1.update(1L);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 64);
  }
  
  @Test
  public void toStringCrudeCheck() {
    ItemsUnion<String> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    union.update("a");
    String brief = union.toString();
    String full = union.toString(true, true);
    Assert.assertTrue(brief.length() < full.length());
  }

}
