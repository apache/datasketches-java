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

import static org.apache.datasketches.common.Util.clearBits;
import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.getString;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.GenericSortedView;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.ItemsSketchSortedView;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ItemsSketchTest {

  @BeforeMethod
  public void setUp() {
    ItemsSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void empty() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 128, Comparator.naturalOrder());
    assertNotNull(sketch);
    sketch.update(null);
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    try { sketch.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { sketch.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}

    try { sketch.getQuantile(0.5); fail(); } catch (final IllegalArgumentException e) {}
    try { sketch.getQuantiles(new double[] {0.0, 1.0}); fail(); } catch (final IllegalArgumentException e) {}

    final byte[] byteArr = sketch.toByteArray(new ArrayOfStringsSerDe());
    assertEquals(byteArr.length, 8);
    try { sketch.getPMF(new String[0]); fail(); } catch (final IllegalArgumentException e) {}
    try { sketch.getCDF(new String[0]); fail(); } catch (final IllegalArgumentException e) {}
    try { sketch.getRank("a"); fail(); } catch (final IllegalArgumentException e) {}
  }

  @Test
  public void oneItem() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 128, Comparator.naturalOrder());
    sketch.update("a");
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getMinItem(), "a");
    assertEquals(sketch.getMaxItem(), "a");
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), "a");
    assertEquals(sketch.getRank("a", EXCLUSIVE), 0.0);

    {
      final double[] pmf = sketch.getPMF(new String[0], EXCLUSIVE);
      assertEquals(pmf.length, 1);
      assertEquals(pmf[0], 1.0);
    }

    {
      final double[] pmf = sketch.getPMF(new String[] {"a"}, EXCLUSIVE);
      assertEquals(pmf.length, 2);
      assertEquals(pmf[0], 0.0);
      assertEquals(pmf[1], 1.0);
    }

    {
      final double[] cdf = sketch.getCDF(new String[0], EXCLUSIVE);
      assertEquals(cdf.length, 1);
      assertEquals(cdf[0], 1.0);
    }

    {
      final double[] cdf = sketch.getCDF(new String[] {"a"}, EXCLUSIVE);
      assertEquals(cdf.length, 2);
      assertEquals(cdf[0], 0.0);
      assertEquals(cdf[1], 1.0);
    }

    sketch.reset();
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    try { sketch.getMinItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { sketch.getMaxItem(); fail(); } catch (final IllegalArgumentException e) {}
    try { sketch.getQuantile(0.5); fail(); } catch (final IllegalArgumentException e) {}
  }

  @Test
  public void tenItems() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, 128, Comparator.naturalOrder());
    for (int i = 1; i <= 10; i++) { sketch.update(i); }
    assertFalse(sketch.isEmpty());
    assertFalse(sketch.isReadOnly());
    assertEquals(sketch.getN(), 10);
    assertEquals(sketch.getNumRetained(), 10);
    for (int i = 1; i <= 10; i++) {
      assertEquals(sketch.getRank(i), (i) / 10.0);
      assertEquals(sketch.getRank(i, EXCLUSIVE), (i - 1) / 10.0);
      assertEquals(sketch.getRank(i, INCLUSIVE), i / 10.0);
    }
    final Integer[] qArr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    double[] rOut = sketch.getRanks(qArr); //inclusive
    for (int i = 0; i < qArr.length; i++) {
      assertEquals(rOut[i], (i + 1) / 10.0);
    }
    rOut = sketch.getRanks(qArr, EXCLUSIVE); //exclusive
    for (int i = 0; i < qArr.length; i++) {
      assertEquals(rOut[i], i / 10.0);
    }
    // inclusive = (default)
    assertEquals(sketch.getQuantile(0, EXCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, EXCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.2, EXCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.3, EXCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.4, EXCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.6, EXCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.7, EXCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.8, EXCLUSIVE), 9);
    assertEquals(sketch.getQuantile(0.9, EXCLUSIVE), 10);
    assertEquals(sketch.getQuantile(1, EXCLUSIVE), 10);
    // inclusive = true
    assertEquals(sketch.getQuantile(0, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.2, INCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.3, INCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.4, INCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.5, INCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.6, INCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.7, INCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.8, INCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.9, INCLUSIVE), 9);
    assertEquals(sketch.getQuantile(1, INCLUSIVE), 10);

    // getQuantile() and getQuantiles() equivalence
    {
      // inclusive = (default)
      final Integer[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1});
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0), quantiles[i]);
      }
    }
    {
      // inclusive = true
      final Integer[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}, INCLUSIVE);
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, INCLUSIVE), quantiles[i]);
      }
    }
  }

  @Test
  public void estimation() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, 128, Comparator.naturalOrder());
    for (int i = 1; i <= 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getN(), 1000);
    assertTrue(sketch.getNumRetained() < 1000);
    assertEquals(sketch.getMinItem(), Integer.valueOf(1));
    assertEquals(sketch.getMaxItem(), Integer.valueOf(1000));
    // based on ~1.7% normalized rank error for this particular case
    assertEquals(sketch.getQuantile(0.5), 500, 17);

    final double[] normRanks = {0.0, 0.5, 1.0};
    final Integer[] quantiles = sketch.getQuantiles(normRanks);

    assertEquals(quantiles[1], 500, 17); // median

    final double[] normRanks2 = {.25, 0.5, 0.75};
    final Integer[] quantiles2 = sketch.getQuantiles(normRanks2);
    assertEquals(quantiles2[0], 250, 17);
    assertEquals(quantiles2[1], 500, 17);
    assertEquals(quantiles2[2], 750, 17);

    final double normErr = sketch.getNormalizedRankError(true);
    assertEquals(normErr, .0172, .001);
    println(""+normErr);

    {
      final double[] pmf = sketch.getPMF(new Integer[0]);
      assertEquals(pmf.length, 1);
      assertEquals(pmf[0], 1.0);
    }

    {
      final double[] pmf = sketch.getPMF(new Integer[] {500});
      assertEquals(pmf.length, 2);
      assertEquals(pmf[0], 0.5, 0.05);
      assertEquals(pmf[1], 0.5, 0.05);
    }

    {
      final Integer[] intArr = new Integer[50];
      for (int i= 0; i<50; i++) {
        intArr[i] = (20*i) +10;
      }
      final double[] pmf = sketch.getPMF(intArr);
      assertEquals(pmf.length, 51);
    }

    {
      final double[] cdf = sketch.getCDF(new Integer[0]);
      assertEquals(cdf.length, 1);
      assertEquals(cdf[0], 1.0);
    }

    {
      final double[] cdf = sketch.getCDF(new Integer[] {500});
      assertEquals(cdf.length, 2);
      assertEquals(cdf[0], 0.5, 0.05);
      assertEquals(cdf[1], 1.0, 0.05);
    }

    assertEquals(sketch.getRank(500), 0.5, 0.01);
  }

  @Test
  public void serializeDeserializeLong() {
    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(Long.class, 128, Comparator.naturalOrder());
    for (int i = 1; i <= 500; i++) {
      sketch1.update((long) i);
    }

    final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    final byte[] bytes = sketch1.toByteArray(serDe);
    final ItemsSketch<Long> sketch2 =
        ItemsSketch.getInstance(Long.class, MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);

    for (int i = 501; i <= 1000; i++) {
      sketch2.update((long) i);
    }
    assertEquals(sketch2.getN(), 1000);
    assertTrue(sketch2.getNumRetained() < 1000);
    assertEquals(sketch2.getMinItem(), Long.valueOf(1));
    assertEquals(sketch2.getMaxItem(), Long.valueOf(1000));
    // based on ~1.7% normalized rank error for this particular case
    assertEquals(sketch2.getQuantile(0.5), 500, 17);
  }

  @Test
  public void serializeDeserializeDouble() {
    final ItemsSketch<Double> sketch1 = ItemsSketch.getInstance(Double.class, 128, Comparator.naturalOrder());
    for (int i = 1; i <= 500; i++) {
      sketch1.update((double) i);
    }

    final ArrayOfItemsSerDe<Double> serDe = new ArrayOfDoublesSerDe();
    final byte[] bytes = sketch1.toByteArray(serDe);
    final ItemsSketch<Double> sketch2 =
        ItemsSketch.getInstance(Double.class, MemorySegment.ofArray(bytes), Comparator.naturalOrder(), serDe);

    for (int i = 501; i <= 1000; i++) {
      sketch2.update((double) i);
    }
    assertEquals(sketch2.getN(), 1000);
    assertTrue(sketch2.getNumRetained() < 1000);
    assertEquals(sketch2.getMinItem(), Double.valueOf(1));
    assertEquals(sketch2.getMaxItem(), Double.valueOf(1000));
    // based on ~1.7% normalized rank error for this particular case
    assertEquals(sketch2.getQuantile(0.5), 500, 17);
  }

  @Test
  public void serializeDeserializeString() {
    // numeric order to be able to make meaningful assertions
    final Comparator<String> numericOrder = new Comparator<String>() {
      @Override
      public int compare(final String s1, final String s2) {
        final Integer i1 = Integer.parseInt(s1, 2);
        final Integer i2 = Integer.parseInt(s2, 2);
        return i1.compareTo(i2);
      }
    };
    final ItemsSketch<String> sketch1 = ItemsSketch.getInstance(String.class, 128, numericOrder);
    for (int i = 1; i <= 500; i++)
     {
      sketch1.update(Integer.toBinaryString(i << 10)); // to make strings longer
    }

    final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();
    final byte[] bytes = sketch1.toByteArray(serDe);
    final ItemsSketch<String> sketch2 = ItemsSketch.getInstance(String.class, MemorySegment.ofArray(bytes), numericOrder, serDe);

    for (int i = 501; i <= 1000; i++) {
      sketch2.update(Integer.toBinaryString(i << 10));
    }
    assertEquals(sketch2.getN(), 1000);
    assertTrue(sketch2.getNumRetained() < 1000);
    assertEquals(sketch2.getMinItem(), Integer.toBinaryString(1 << 10));
    assertEquals(sketch2.getMaxItem(), Integer.toBinaryString(1000 << 10));
    // based on ~1.7% normalized rank error for this particular case
    assertEquals(Integer.parseInt(sketch2.getQuantile(0.5), 2) >> 10, 500, 17);
  }

  @Test
  public void toStringCrudeCheck() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
    String brief, full, part;
    brief = sketch.toString();
    full = sketch.toString(true, true);
    part = sketch.toString(false, true);
    sketch.update("a");
    brief = sketch.toString();
    full = sketch.toString(true, true);
    part = sketch.toString(false, true);
    //println(full);
    assertTrue(brief.length() < full.length());
    assertTrue(part.length() < full.length());
    final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();
    final byte[] bytes = sketch.toByteArray(serDe);
    ItemsSketch.toString(bytes);
    ItemsSketch.toString(MemorySegment.ofArray(bytes));
    //PreambleUtil.toString(bytes, true); // not a DoublesSketch so this will fail
    //ItemsSketch<String> sketch2 = ItemsSketch.getInstance(MemorySegment.wrap(bytes), Comparator.naturalOrder(), serDe);
  }

  @Test
  public void toStringBiggerCheck() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    final String bigger = sketch.toString();
    final String full = sketch.toString(true, true);
    //println(full);
    assertTrue(bigger.length() < full.length());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkDownsampleException() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    sketch.downSample(32);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void negativeQuantileMustThrow() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 16, Comparator.naturalOrder());
    sketch.update("ABC");
    sketch.getQuantile(-0.1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceExcep1() {
    final MemorySegment mem = MemorySegment.ofArray(new byte[4]);
    ItemsSketch.getInstance(String.class, mem, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceExcep2() {
    final MemorySegment mem = MemorySegment.ofArray(new byte[8]);
    ItemsSketch.getInstance(String.class, mem, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
  }

  @Test
  public void checkGoodSerDeId() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
    final byte[] byteArr = sketch.toByteArray(new ArrayOfStringsSerDe());
    final MemorySegment mem = MemorySegment.ofArray(byteArr);
    //println(PreambleUtil.toString(mem));
    ItemsSketch.getInstance(String.class, mem, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
  }

  @Test
  public void checkDownsample() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    final ItemsSketch<String> out = sketch.downSample(8);
    assertEquals(out.getK(), 8);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void unorderedSplitPoints() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    sketch.update(1);
    sketch.getPMF(new Integer[] {2, 1});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nonUniqueSplitPoints() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    sketch.update(1);
    sketch.getPMF(new Integer[] {1, 1});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nullInSplitPoints() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    sketch.update(1);
    sketch.getPMF(new Integer[] {1, null});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void compactNotSupported() {
    final ArrayOfDoublesSerDe serDe = new ArrayOfDoublesSerDe();
    final ItemsSketch<Double> sketch = ItemsSketch.getInstance(Double.class, Comparator.naturalOrder());
    final byte[] byteArr = sketch.toByteArray(serDe);
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    clearBits(seg, PreambleUtil.FLAGS_BYTE, (byte) PreambleUtil.COMPACT_FLAG_MASK);
    println(PreambleUtil.toString(seg, false));
    ItemsSketch.getInstance(Double.class, seg, Comparator.naturalOrder(), serDe);
  }

  @Test
  public void checkPutMemory() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    final byte[] byteArr = new byte[200];
    final MemorySegment mem = MemorySegment.ofArray(byteArr);
    sketch.putMemorySegment(mem, new ArrayOfStringsSerDe());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPutMemoryException() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, 16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    final byte[] byteArr = new byte[100];
    final MemorySegment mem = MemorySegment.ofArray(byteArr);
    sketch.putMemorySegment(mem, new ArrayOfStringsSerDe());
  }

  @Test
  public void checkPMFonEmpty() {
    final ItemsSketch<String> iss = buildStringIS(32, 32);
    final double[] ranks = {};
    final String[] qOut = iss.getQuantiles(ranks);
    println("qOut: "+qOut.length);
    assertEquals(qOut.length, 0);
    final double[] cdfOut = iss.getCDF(new String[0]);
    println("cdfOut: "+cdfOut.length);
    assertEquals(cdfOut[0], 1.0, 0.0);
  }

  @Test
  public void checkToFromByteArray() {
    checkToFromByteArray2(128, 1300); //generates a pattern of 5 -> 101
    checkToFromByteArray2(4, 7);
    checkToFromByteArray2(4, 8);
    checkToFromByteArray2(4, 9);
  }

  @Test
  public void getRankAndGetCdfConsistency() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    final int n = 1_000_000;
    final Integer[] values = new Integer[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }
    { // inclusive = false (default)
      final double[] ranks = sketch.getCDF(values);
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(values[i]), 0.00001, "CDF vs rank for value " + i);
      }
    }
    { // inclusive = true
      final double[] ranks = sketch.getCDF(values, INCLUSIVE);
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(values[i], INCLUSIVE), 0.00001, "CDF vs rank for value " + i);
      }
    }
  }

  @Test
  public void getRankAndGetCdfConsistencyReverseComparator() {
    final ItemsSketch<Integer> sketch =
        ItemsSketch.getInstance(Integer.class, Comparator.<Integer>naturalOrder().reversed());
    final int n = 1_000_000;
    final Integer[] values = new Integer[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }
    Arrays.sort(values, sketch.getComparator());
    final double[] ranks = sketch.getCDF(values);
    for (int i = 0; i < n; i++) {
      assertEquals(ranks[i], sketch.getRank(values[i]), 0.00001, "CDF vs rank for value " + i);
    }
  }

  @Test
  public void checkBounds() {
    final ItemsSketch<Double> sketch = ItemsSketch.getInstance(Double.class, Comparator.naturalOrder());
    for (int i = 0; i < 1000; i++) {
      sketch.update((double)i);
    }
    final double eps = sketch.getNormalizedRankError(false);
    final double est = sketch.getQuantile(0.5);
    final double ub = sketch.getQuantileUpperBound(0.5);
    final double lb = sketch.getQuantileLowerBound(0.5);
    assertEquals(ub, (double)sketch.getQuantile(.5 + eps));
    assertEquals(lb, (double)sketch.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
  }

  @Test
  public void checkGetKFromEqs() {
    final ItemsSketch<Double> sketch = ItemsSketch.getInstance(Double.class, Comparator.naturalOrder());
    final int k = sketch.getK();
    final double eps = ItemsSketch.getNormalizedRankError(k, false);
    final double epsPmf = ItemsSketch.getNormalizedRankError(k, true);
    final int kEps = ItemsSketch.getKFromEpsilon(eps, false);
    final int kEpsPmf = ItemsSketch.getKFromEpsilon(epsPmf, true);
    assertEquals(kEps, k);
    assertEquals(kEpsPmf, k);
  }

  private static void checkToFromByteArray2(final int k, final int n) {
    final ItemsSketch<String> is = buildStringIS(k, n);
    byte[] byteArr;
    MemorySegment mem;
    ItemsSketch<String> is2;
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    //ordered
    byteArr = is.toByteArray(true, serDe);
    mem = MemorySegment.ofArray(byteArr);
    is2 = ItemsSketch.getInstance(String.class, mem, Comparator.naturalOrder(), serDe);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(is.getQuantile(f), is2.getQuantile(f));
    }

    //Not-ordered
    byteArr = is.toByteArray(false, serDe);
    mem = MemorySegment.ofArray(byteArr);
    is2 = ItemsSketch.getInstance(String.class, mem, Comparator.naturalOrder(), serDe);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(is.getQuantile(f), is2.getQuantile(f));
    }
  }

  static ItemsSketch<String> buildStringIS(final int k, final int n) {
    return buildStringIS(k, n, 0);
  }

  static ItemsSketch<String> buildStringIS(final int k, final int n, final int start) {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(String.class, k, Comparator.naturalOrder());
    for (int i = 0; i < n; i++) {
      sketch.update(Integer.toString(i + start));
    }
    return sketch;
  }

  @Test
  public void sortedView() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    sketch.update(3);
    sketch.update(1);
    sketch.update(2);
    { // cumulative inclusive
      final GenericSortedView<Integer> view = sketch.getSortedView();
      final GenericSortedViewIterator<Integer> it = view.iterator();
      assertEquals(it.next(), true);
      assertEquals(it.getQuantile(), 1);
      assertEquals(it.getWeight(), 1);
      assertEquals(it.getNaturalRank(INCLUSIVE), 1);
      assertEquals(it.next(), true);
      assertEquals(it.getQuantile(), 2);
      assertEquals(it.getWeight(), 1);
      assertEquals(it.getNaturalRank(INCLUSIVE), 2);
      assertEquals(it.next(), true);
      assertEquals(it.getQuantile(), 3);
      assertEquals(it.getWeight(), 1);
      assertEquals(it.getNaturalRank(INCLUSIVE), 3);
      assertEquals(it.next(), false);
    }
  }

  @Test
  public void checkIssue484() {
    final Boolean[] items = { true,false,true,false,true,false,true,false,true,false };
    final ItemsSketch<Boolean> sketch = ItemsSketch.getInstance(Boolean.class, Boolean::compareTo);
    for (int i = 0; i < items.length; i++) { sketch.update(items[i]); }
    final byte[] serialized = sketch.toByteArray(new ArrayOfBooleansSerDe());
    final ItemsSketch<Boolean> deserialized =
        ItemsSketch.getInstance(Boolean.class, MemorySegment.ofArray(serialized), Boolean::compareTo, new ArrayOfBooleansSerDe());
    checkSketchesEqual(sketch, deserialized);
  }

  private static <T> void checkSketchesEqual(final ItemsSketch<T> expected, final ItemsSketch<T> actual) {
    final ItemsSketchSortedView<T> expSV = expected.getSortedView();
    final ItemsSketchSortedView<T> actSV = actual.getSortedView();
    final int N = (int)actSV.getN();
    final long[] expCumWts = expSV.getCumulativeWeights();
    final Boolean[] expItemsArr = (Boolean[])expSV.getQuantiles();
    final long[] actCumWts = actSV.getCumulativeWeights();
    final Boolean[] actItemsArr = (Boolean[])actSV.getQuantiles();
    printf("%3s %8s %8s\n", "i","Actual", "Expected");
    for (int i = 0; i < N; i++) {
      printf("%3d %8s %8s\n", i, actItemsArr[i].toString(), expItemsArr[i].toString());
    }
    assertEquals(actCumWts, expCumWts);
    assertEquals(actItemsArr, expItemsArr);
  }

  @Test
  //There is no guarantee that BaseBuffer is sorted after a merge.
  //The issue is, during a merge, BB must be sorted prior to a compaction to a higher level.
  //Otherwise the higher levels would not be sorted properly.
  public void checkL0SortDuringMergeIssue527() throws NumberFormatException {
    final Random rand = new Random();
    final ItemsSketch<String> sk1 = ItemsSketch.getInstance(String.class, 8, Comparator.reverseOrder());
    final ItemsSketch<String> sk2 = ItemsSketch.getInstance(String.class, 8, Comparator.reverseOrder());
    final int n = 24; //don't change this
    for (int i = 1; i <= n; i++ ) {
      final int j = rand.nextInt(n) + 1;
      sk1.update(getString(j, 3));
      sk2.update(getString(j +100, 3));
    }
    final int k = 8;
    final ItemsUnion<String> union = ItemsUnion.getInstance(String.class, k, Comparator.reverseOrder());
    union.union(sk1);
    union.union(sk2);
    final ItemsSketch<String> sk3 = union.getResult();
    println(sk3.toString(true, true)); //L0 and above should be sorted in reverse. Ignore BB.

    final QuantilesGenericSketchIterator<String> itr = sk3.iterator();
    itr.next();
    int prev = Integer.parseInt(itr.getQuantile().trim());
    for (int i = 1; i < (2 * k); i++) {
      if (itr.next()) {
        final int v = Integer.parseInt(itr.getQuantile().trim());
        if (i == k) {
          prev = Integer.parseInt(itr.getQuantile().trim());
          continue;
        }
        assertTrue(v <= prev);
        prev = v;
      }
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  private final static boolean enablePrinting = false;

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
