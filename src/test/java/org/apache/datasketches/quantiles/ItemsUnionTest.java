/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.quantiles.PreambleUtil.DEFAULT_K;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ItemsUnionTest {

  @Test
  public void nullAndEmpty() {
    final ItemsSketch<Integer> emptySk = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    final ItemsSketch<Integer> validSk = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    ItemsSketch<Integer> result;
    validSk.update(1);
    ItemsUnion<Integer> union = ItemsUnion.getInstance(Integer.class, Comparator.naturalOrder());
    union.union(validSk);

    union = ItemsUnion.getInstance(emptySk);
    // internal sketch is empty at this point
    union.union((ItemsSketch<Integer>) null);
    union.union(emptySk);
    Assert.assertTrue(union.isEmpty());
    Assert.assertEquals(union.getMaxK(), DEFAULT_K);
    Assert.assertEquals(union.getEffectiveK(), DEFAULT_K);
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}
    union.union(validSk);

    union.reset();
    // internal sketch is null again
    union.union((ItemsSketch<Integer>) null);
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}

    // internal sketch is not null again because getResult() instantiated it
    union.union(ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder()));
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}

    union.reset();
    // internal sketch is null again
    union.union(ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder()));
    result = union.getResult();
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}
  }

  @Test
  public void nullAndEmptyInputsToNonEmptyUnion() {
    final ItemsUnion<Integer> union = ItemsUnion.getInstance(Integer.class, 128, Comparator.naturalOrder());
    union.update(1);
    ItemsSketch<Integer> result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getN(), 1);
    Assert.assertEquals(result.getMinItem(), Integer.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Integer.valueOf(1));

    union.union((ItemsSketch<Integer>) null);
    result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getN(), 1);
    Assert.assertEquals(result.getMinItem(), Integer.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Integer.valueOf(1));

    union.union(ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder()));
    result = union.getResult();
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getN(), 1);
    Assert.assertEquals(result.getMinItem(), Integer.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Integer.valueOf(1));
  }

  @Test
  public void basedOnSketch() {
    final Comparator<String> comp = Comparator.naturalOrder();
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, comp);
    ItemsUnion<String> union = ItemsUnion.getInstance(sketch);
    union.reset();
    final byte[] byteArr = sketch.toByteArray(serDe);
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    union = ItemsUnion.getInstance(String.class, seg, comp, serDe);
    Assert.assertEquals(byteArr.length, 8);
    union.reset();
  }

  @Test
  public void sameK() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 128, Comparator.naturalOrder());
    ItemsSketch<Long> result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) { }
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) { }

    for (int i = 1; i <= 1000; i++) { union.update((long) i); }
    result = union.getResult();
    Assert.assertEquals(result.getN(), 1000);
    Assert.assertEquals(result.getMinItem(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Long.valueOf(1000));
    Assert.assertEquals(result.getQuantile(0.5), 500, 17); // ~1.7% normalized rank error

    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(Long.class, Comparator.naturalOrder());
    for (int i = 1001; i <= 2000; i++) { sketch1.update((long) i); }
    union.union(sketch1);
    result = union.getResult();
    Assert.assertEquals(result.getN(), 2000);
    Assert.assertEquals(result.getMinItem(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Long.valueOf(2000));
    Assert.assertEquals(result.getQuantile(0.5), 1000, 34); // ~1.7% normalized rank error

    final ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(Long.class, Comparator.naturalOrder());
    for (int i = 2001; i <= 3000; i++) { sketch2.update((long) i); }
    final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    union.union(MemorySegment.ofArray(sketch2.toByteArray(serDe)), serDe);
    result = union.getResultAndReset();
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getN(), 3000);
    Assert.assertEquals(result.getMinItem(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Long.valueOf(3000));
    Assert.assertEquals(result.getQuantile(0.5), 1500, 51); // ~1.7% normalized rank error

    result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) { }
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) { }
  }

  @Test
  public void differentK() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 512, Comparator.naturalOrder());
    ItemsSketch<Long> result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}

    for (int i = 1; i <= 10000; i++) { union.update((long) i); }
    result = union.getResult();
    Assert.assertEquals(result.getK(), 512);
    Assert.assertEquals(result.getN(), 10000);
    Assert.assertEquals(result.getMinItem(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Long.valueOf(10000));
    Assert.assertEquals(result.getQuantile(0.5), 5000, 50); // ~0.5% normalized rank error

    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(Long.class, 256, Comparator.naturalOrder());
    for (int i = 10001; i <= 20000; i++) { sketch1.update((long) i); }
    union.union(sketch1);
    result = union.getResult();
    Assert.assertEquals(result.getK(), 256);
    Assert.assertEquals(result.getN(), 20000);
    Assert.assertEquals(result.getMinItem(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Long.valueOf(20000));
    Assert.assertEquals(result.getQuantile(0.5), 10000, 180); // ~0.9% normalized rank error

    final ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(Long.class,128,  Comparator.naturalOrder());
    for (int i = 20001; i <= 30000; i++) { sketch2.update((long) i); }
    final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    union.union(MemorySegment.ofArray(sketch2.toByteArray(serDe)), serDe);
    result = union.getResultAndReset();
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getK(), 128);
    Assert.assertEquals(result.getN(), 30000);
    Assert.assertEquals(result.getMinItem(), Long.valueOf(1));
    Assert.assertEquals(result.getMaxItem(), Long.valueOf(30000));
    Assert.assertEquals(result.getQuantile(0.5), 15000, 510); // ~1.7% normalized rank error

    result = union.getResult();
    Assert.assertEquals(result.getN(), 0);
    try { result.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { result.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}
  }

  @Test
  public void differentLargerK() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 128, Comparator.naturalOrder());
    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(Long.class, 256, Comparator.naturalOrder());
    union.union(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1L);
    union.union(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentSmallerK() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 128, Comparator.naturalOrder());
    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(Long.class, 64, Comparator.naturalOrder());
    union.union(sketch1); //union empty, sketch1: empty
    Assert.assertEquals(union.getResult().getK(), 128); //union: empty, k=128
    sketch1.update(1L); //union: empty, k=128; sketch: valid, k=64
    union.union(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void toStringCrudeCheck() {
    final ItemsUnion<String> union = ItemsUnion.getInstance(String.class, 128, Comparator.naturalOrder());
    union.update("a");
    final String brief = union.toString();
    final String full = union.toString(true, true);
    Assert.assertTrue(brief.length() < full.length());
  }

  @Test
  public void meNullOtherExactBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 16, Comparator.naturalOrder()); //me null
    ItemsSketch<Long> skExact = buildIS(32, 31); //other is bigger, exact
    union.union(skExact);
    println(skExact.toString(true, true));
    println(union.toString(true, true));
    Assert.assertEquals(skExact.getQuantile(0.5), union.getResult().getQuantile(0.5), 0.0);

    union.reset();
    skExact = buildIS(8, 15); //other is smaller exact,
    union.union(skExact);
    println(skExact.toString(true, true));
    println(union.toString(true, true));
    Assert.assertEquals(skExact.getQuantile(0.5), union.getResult().getQuantile(0.5), 0.0);
  }

  @Test
  public void meNullOtherEstBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 16, Comparator.naturalOrder()); //me null
    ItemsSketch<Long> skEst = buildIS(32, 64); //other is bigger, est
    union.union(skEst);
    Assert.assertEquals(union.getResult().getMinItem(), skEst.getMinItem(), 0.0);
    Assert.assertEquals(union.getResult().getMaxItem(), skEst.getMaxItem(), 0.0);

    union.reset();
    skEst = buildIS(8, 64); //other is smaller est,
    union.union(skEst);
    Assert.assertEquals(union.getResult().getMinItem(), skEst.getMinItem(), 0.0);
    Assert.assertEquals(union.getResult().getMaxItem(), skEst.getMaxItem(), 0.0);
  }

  @Test
  public void meEmptyOtherExactBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 16, Comparator.naturalOrder()); //me null
    final ItemsSketch<Long> skEmpty = ItemsSketch.getInstance(Long.class, 64, Comparator.naturalOrder());
    union.union(skEmpty); //empty at k = 16
    ItemsSketch<Long> skExact = buildIS(32, 63); //bigger, exact
    union.union(skExact);
    Assert.assertEquals(union.getResult().getMinItem(), skExact.getMinItem(), 0.0);
    Assert.assertEquals(union.getResult().getMaxItem(), skExact.getMaxItem(), 0.0);

    union.reset();
    union.union(skEmpty); //empty at k = 16
    skExact = buildIS(8, 15); //smaller, exact
    union.union(skExact);
    Assert.assertEquals(union.getResult().getMinItem(), skExact.getMinItem(), 0.0);
    Assert.assertEquals(union.getResult().getMaxItem(), skExact.getMaxItem(), 0.0);
  }

  @Test
  public void meEmptyOtherEstBiggerSmaller() {
    final ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, 16, Comparator.naturalOrder()); //me null
    final ItemsSketch<Long> skEmpty = ItemsSketch.getInstance(Long.class, 64, Comparator.naturalOrder());
    union.union(skEmpty); //empty at k = 16
    ItemsSketch<Long> skExact = buildIS(32, 64); //bigger, est
    union.union(skExact);
    Assert.assertEquals(union.getResult().getMinItem(), skExact.getMinItem(), 0.0);
    Assert.assertEquals(union.getResult().getMaxItem(), skExact.getMaxItem(), 0.0);

    union.reset();
    union.union(skEmpty); //empty at k = 16
    skExact = buildIS(8, 16); //smaller, est
    union.union(skExact);
    Assert.assertEquals(union.getResult().getMinItem(), skExact.getMinItem(), 0.0);
    Assert.assertEquals(union.getResult().getMaxItem(), skExact.getMaxItem(), 0.0);
  }

  @Test
  public void checkMergeIntoEqualKs() {
    final ItemsSketch<Long> skEmpty1 = buildIS(32, 0);
    final ItemsSketch<Long> skEmpty2 = buildIS(32, 0);
    ItemsMergeImpl.mergeInto(skEmpty1, skEmpty2);
    try { skEmpty2.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { skEmpty2.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}

    ItemsSketch<Long> skValid1, skValid2;
    int n = 64;
    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, 0, 0); //empty
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinItem(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxItem(), n - 1.0, 0.0);

    skValid1 = buildIS(32, 0, 0); //empty
    skValid2 = buildIS(32, n, 0);
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinItem(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxItem(), n - 1.0, 0.0);

    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, n, n);
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinItem(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxItem(), (2 * n) - 1.0, 0.0);

    n = 512;
    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, n, n);
    ItemsMergeImpl.mergeInto(skValid1, skValid2);
    Assert.assertEquals(skValid2.getMinItem(), 0.0, 0.0);
    Assert.assertEquals(skValid2.getMaxItem(), (2 * n) - 1.0, 0.0);

    skValid1 = buildIS(32, n, 0);
    skValid2 = buildIS(32, n, n);
    ItemsMergeImpl.mergeInto(skValid2, skValid1);
    Assert.assertEquals(skValid1.getMinItem(), 0.0, 0.0);
    Assert.assertEquals(skValid1.getMaxItem(), (2 * n) - 1.0, 0.0);
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
    ItemsUnion<Long> union = ItemsUnion.getInstance(Long.class, k, Comparator.naturalOrder());
    byte[] bytesOut = union.toByteArray(serDe);
    Assert.assertEquals(bytesOut.length, 8);
    Assert.assertTrue(union.isEmpty());

    final byte[] byteArr = buildIS(k, (2 * k) + 5).toByteArray(serDe);
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    union = ItemsUnion.getInstance(Long.class, seg, Comparator.naturalOrder(), serDe);
    bytesOut = union.toByteArray(serDe);
    Assert.assertEquals(bytesOut.length, byteArr.length);
    Assert.assertEquals(bytesOut, byteArr); // assumes consistent internal use of toByteArray()
  }

  private static ItemsSketch<Long> buildIS(final int k, final int n) {
    return buildIS(k, n, 0);
  }

  private static ItemsSketch<Long> buildIS(final int k, final int n, final int startV) {
    final ItemsSketch<Long> is = ItemsSketch.getInstance(Long.class, k, Comparator.naturalOrder());
    for (long i = 0; i < n; i++) { is.update(i + startV); }
    return is;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param o value to print
   */
  static void println(final  Object o) {
    //System.out.println(o.toString()); //disable here
  }

}
