/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.DEFAULT_K;

import java.util.Comparator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;

public class ItemsUnionTest {

  @Test
  public void nullAndEmpty() {
    ItemsUnion<Integer> union = ItemsUnion.getInstance(Comparator.naturalOrder());

    // union gadget sketch is null at this point
    Assert.assertTrue(union.isEmpty());
    Assert.assertFalse(union.isDirect());
    Assert.assertEquals(union.getMaxK(), DEFAULT_K);
    Assert.assertEquals(union.getEffectiveK(), DEFAULT_K);
    Assert.assertTrue(union.toString().length() > 0);

    union.update((Integer) null);
    ItemsSketch<Integer> result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());
    Assert.assertNull(union.getResultAndReset());

    final ItemsSketch<Integer> emptySk = ItemsSketch.getInstance(Comparator.naturalOrder());
    final ItemsSketch<Integer> validSk = ItemsSketch.getInstance(Comparator.naturalOrder());
    validSk.update(1);
    union.update(validSk);

    union = ItemsUnion.getInstance(result);
    // internal sketch is empty at this point
    union.update((ItemsSketch<Integer>) null);
    union.update(emptySk);
    Assert.assertTrue(union.isEmpty());
    Assert.assertFalse(union.isDirect());
    Assert.assertEquals(union.getMaxK(), DEFAULT_K);
    Assert.assertEquals(union.getEffectiveK(), DEFAULT_K);
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
    final ItemsUnion<Integer> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
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
    final Comparator<String> comp = Comparator.naturalOrder();
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(comp);
    ItemsUnion<String> union = ItemsUnion.getInstance(sketch);
    union.reset();
    final byte[] byteArr = sketch.toByteArray(serDe);
    final Memory mem = Memory.wrap(byteArr);
    union = ItemsUnion.getInstance(mem, comp, serDe);
    Assert.assertEquals(byteArr.length, 8);
    union.reset();
  }

  @Test
  public void sameK() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    ItemsSketch<Long> result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());

    for (int i = 1; i <= 1000; i++) { union.update((long) i); }
    result = union.getResult();
    Assert.assertEquals(result.getN(), 1000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(1000));
    Assert.assertEquals(result.getQuantile(0.5), 500, 17); // ~1.7% normalized rank error

    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(Comparator.naturalOrder());
    for (int i = 1001; i <= 2000; i++) { sketch1.update((long) i); }
    union.update(sketch1);
    result = union.getResult();
    Assert.assertEquals(result.getN(), 2000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(2000));
    Assert.assertEquals(result.getQuantile(0.5), 1000, 34); // ~1.7% normalized rank error

    final ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(Comparator.naturalOrder());
    for (int i = 2001; i <= 3000; i++) { sketch2.update((long) i); }
    final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    union.update(Memory.wrap(sketch2.toByteArray(serDe)), serDe);
    result = union.getResultAndReset();
    Assert.assertNotNull(result);
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
    final ItemsUnion<Long> union = ItemsUnion.getInstance(512, Comparator.naturalOrder());
    ItemsSketch<Long> result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    Assert.assertNull(result.getMinValue());
    Assert.assertNull(result.getMaxValue());

    for (int i = 1; i <= 10000; i++) { union.update((long) i); }
    result = union.getResult();
    Assert.assertEquals(result.getK(), 512);
    Assert.assertEquals(result.getN(), 10000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(10000));
    Assert.assertEquals(result.getQuantile(0.5), 5000, 50); // ~0.5% normalized rank error

    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(256, Comparator.naturalOrder());
    for (int i = 10001; i <= 20000; i++) { sketch1.update((long) i); }
    union.update(sketch1);
    result = union.getResult();
    Assert.assertEquals(result.getK(), 256);
    Assert.assertEquals(result.getN(), 20000);
    Assert.assertEquals(result.getMinValue(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxValue(), Long.valueOf(20000));
    Assert.assertEquals(result.getQuantile(0.5), 10000, 180); // ~0.9% normalized rank error

    final ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    for (int i = 20001; i <= 30000; i++) { sketch2.update((long) i); }
    final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    union.update(Memory.wrap(sketch2.toByteArray(serDe)), serDe);
    result = union.getResultAndReset();
    Assert.assertNotNull(result);
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
    final ItemsUnion<Long> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(256, Comparator.naturalOrder());
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1L);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentSmallerK() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(64, Comparator.naturalOrder());
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 64);
    sketch1.update(1L);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 64);
  }

  @Test
  public void toStringCrudeCheck() {
    final ItemsUnion<String> union = ItemsUnion.getInstance(128, Comparator.naturalOrder());
    union.update("a");
    final String brief = union.toString();
    final String full = union.toString(true, true);
    Assert.assertTrue(brief.length() < full.length());
  }

  @Test
  public void meNullOtherExactBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(16, Comparator.naturalOrder()); //me null
    ItemsSketch<Long> skExact = buildIS(32, 31); //other is bigger, exact
    union.update(skExact);
    println(skExact.toString(true, true));
    println(union.toString(true, true));
    Assert.assertEquals(skExact.getQuantile(0.5), union.getResult().getQuantile(0.5), 0.0);

    union.reset();
    skExact = buildIS(8, 15); //other is smaller exact,
    union.update(skExact);
    println(skExact.toString(true, true));
    println(union.toString(true, true));
    Assert.assertEquals(skExact.getQuantile(0.5), union.getResult().getQuantile(0.5), 0.0);
  }

  @Test
  public void meNullOtherEstBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(16, Comparator.naturalOrder()); //me null
    ItemsSketch<Long> skEst = buildIS(32, 64); //other is bigger, est
    union.update(skEst);
    Assert.assertEquals(union.getResult().getMinValue(), skEst.getMinValue(), 0.0);
    Assert.assertEquals(union.getResult().getMaxValue(), skEst.getMaxValue(), 0.0);

    union.reset();
    skEst = buildIS(8, 64); //other is smaller est,
    union.update(skEst);
    Assert.assertEquals(union.getResult().getMinValue(), skEst.getMinValue(), 0.0);
    Assert.assertEquals(union.getResult().getMaxValue(), skEst.getMaxValue(), 0.0);
  }

  @Test
  public void meEmptyOtherExactBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(16, Comparator.naturalOrder()); //me null
    final ItemsSketch<Long> skEmpty = ItemsSketch.getInstance(64, Comparator.naturalOrder());
    union.update(skEmpty); //empty at k = 16
    ItemsSketch<Long> skExact = buildIS(32, 63); //bigger, exact
    union.update(skExact);
    Assert.assertEquals(union.getResult().getMinValue(), skExact.getMinValue(), 0.0);
    Assert.assertEquals(union.getResult().getMaxValue(), skExact.getMaxValue(), 0.0);

    union.reset();
    union.update(skEmpty); //empty at k = 16
    skExact = buildIS(8, 15); //smaller, exact
    union.update(skExact);
    Assert.assertEquals(union.getResult().getMinValue(), skExact.getMinValue(), 0.0);
    Assert.assertEquals(union.getResult().getMaxValue(), skExact.getMaxValue(), 0.0);
  }

  @Test
  public void meEmptyOtherEstBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(16, Comparator.naturalOrder()); //me null
    final ItemsSketch<Long> skEmpty = ItemsSketch.getInstance(64, Comparator.naturalOrder());
    union.update(skEmpty); //empty at k = 16
    ItemsSketch<Long> skExact = buildIS(32, 64); //bigger, est
    union.update(skExact);
    Assert.assertEquals(union.getResult().getMinValue(), skExact.getMinValue(), 0.0);
    Assert.assertEquals(union.getResult().getMaxValue(), skExact.getMaxValue(), 0.0);

    union.reset();
    union.update(skEmpty); //empty at k = 16
    skExact = buildIS(8, 16); //smaller, est
    union.update(skExact);
    Assert.assertEquals(union.getResult().getMinValue(), skExact.getMinValue(), 0.0);
    Assert.assertEquals(union.getResult().getMaxValue(), skExact.getMaxValue(), 0.0);
  }

  @Test
  public void checkMergeIntoEqualKs() {
    final ItemsSketch<Long> skEmpty1 = buildIS(32, 0);
    final ItemsSketch<Long> skEmpty2 = buildIS(32, 0);
    ItemsMergeImpl.mergeInto(skEmpty1, skEmpty2);
    Assert.assertNull(skEmpty2.getMaxValue());
    Assert.assertNull(skEmpty2.getMaxValue());

    ItemsSketch<Long> skValid1, skValid2;
    int n = 64;
    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, 0, 0); //empty
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinValue(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxValue(), n - 1.0, 0.0);

    skValid1 = buildIS(32, 0, 0); //empty
    skValid2 = buildIS(32, n, 0);
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinValue(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxValue(), n - 1.0, 0.0);

    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, n, n);
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinValue(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxValue(), 2 * n - 1.0, 0.0);

    n = 512;
    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, n, n);
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinValue(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxValue(), 2 * n - 1.0, 0.0);

    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, n, n);
    ItemsMergeImpl.mergeInto(skValid2, skValid1);
    Assert.assertEquals(skValid1.getMinValue(), 0.0, 0.0);
    Assert.assertEquals(skValid1.getMaxValue(), 2 * n - 1.0, 0.0);
  }

  @Test
  public void checkDownSamplingMergeIntoUnequalKs() {
    ItemsSketch<Long> sk1, sk2;
    final int n = 128;
    sk1 = buildIS(64, n, 0);
    sk2 = buildIS(32, n, 128);
    ItemsMergeImpl.downSamplingMergeInto(sk1, sk2);

    sk1 = buildIS(64, n, 128);
    sk2 = buildIS(32, n, 0);
    ItemsMergeImpl.downSamplingMergeInto(sk1, sk2);
  }

  @Test
  public void checkToByteArray() {
    final int k = 32;
    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    ItemsUnion<Long> union = ItemsUnion.getInstance(k, Comparator.naturalOrder());
    byte[] bytesOut = union.toByteArray(serDe);
    Assert.assertEquals(bytesOut.length, 8);
    Assert.assertTrue(union.isEmpty());

    final byte[] byteArr = buildIS(k, 2 * k + 5).toByteArray(serDe);
    final Memory mem = Memory.wrap(byteArr);
    union = ItemsUnion.getInstance(mem, Comparator.naturalOrder(), serDe);
    bytesOut = union.toByteArray(serDe);
    Assert.assertEquals(bytesOut.length, byteArr.length);
    Assert.assertEquals(bytesOut, byteArr); // assumes consistent internal use of toByteArray()
  }

  private static ItemsSketch<Long> buildIS(final int k, final int n) {
    return buildIS(k, n, 0);
  }

  private static ItemsSketch<Long> buildIS(final int k, final int n, final int startV) {
    final ItemsSketch<Long> is = ItemsSketch.getInstance(k, Comparator.naturalOrder());
    for (long i = 0; i < n; i++) { is.update(i + startV); }
    return is;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final  String s) {
    //System.out.println(s); //disable here
  }

}
