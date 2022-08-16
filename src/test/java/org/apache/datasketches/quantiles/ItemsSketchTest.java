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

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.QuantileSearchCriteria.EXCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;

import org.apache.datasketches.ArrayOfDoublesSerDe;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfLongsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ItemsSketchTest {

  @BeforeMethod
  public void setUp() {
    ItemsSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void empty() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    assertNotNull(sketch);
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getRetainedItems(), 0);
    assertNull(sketch.getMinValue());
    assertNull(sketch.getMaxValue());
    assertNull(sketch.getQuantile(0.5));
    assertNull(sketch.getQuantiles(2));
    assertNull(sketch.getQuantiles(new double[] {0.0, 1.0}));
    final byte[] byteArr = sketch.toByteArray(new ArrayOfStringsSerDe());
    assertEquals(byteArr.length, 8);
    assertNull(sketch.getPMF(new String[0]));
    assertNull(sketch.getCDF(new String[0]));
    assertTrue(Double.isNaN(sketch.getRank("a")));
  }

  @Test
  public void oneItem() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    sketch.update("a");
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getRetainedItems(), 1);
    assertEquals(sketch.getMinValue(), "a");
    assertEquals(sketch.getMaxValue(), "a");
    assertEquals(sketch.getQuantile(0.5), "a");
    assertEquals(sketch.getRank("a"), 0.0);

    {
      final double[] pmf = sketch.getPMF(new String[0]);
      assertEquals(pmf.length, 1);
      assertEquals(pmf[0], 1.0);
    }

    {
      final double[] pmf = sketch.getPMF(new String[] {"a"});
      assertEquals(pmf.length, 2);
      assertEquals(pmf[0], 0.0);
      assertEquals(pmf[1], 1.0);
    }

    {
      final double[] cdf = sketch.getCDF(new String[0]);
      assertEquals(cdf.length, 1);
      assertEquals(cdf[0], 1.0);
    }

    {
      final double[] cdf = sketch.getCDF(new String[] {"a"});
      assertEquals(cdf.length, 2);
      assertEquals(cdf[0], 0.0);
      assertEquals(cdf[1], 1.0);
    }

    sketch.reset();
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getRetainedItems(), 0);
    assertNull(sketch.getMinValue());
    assertNull(sketch.getMaxValue());
    assertNull(sketch.getQuantile(0.5));
  }

  @Test
  public void tenItems() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    for (int i = 1; i <= 10; i++) { sketch.update(i); }
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 10);
    assertEquals(sketch.getRetainedItems(), 10);
    for (int i = 1; i <= 10; i++) {
      assertEquals(sketch.getRank(i), (i - 1) / 10.0);
      assertEquals(sketch.getRank(i, EXCLUSIVE), (i - 1) / 10.0);
      assertEquals(sketch.getRank(i, INCLUSIVE), i / 10.0);
    }
    // inclusive = false (default)
    assertEquals(sketch.getQuantile(0), 1); // always min value
    assertEquals(sketch.getQuantile(0.1), 2);
    assertEquals(sketch.getQuantile(0.2), 3);
    assertEquals(sketch.getQuantile(0.3), 4);
    assertEquals(sketch.getQuantile(0.4), 5);
    assertEquals(sketch.getQuantile(0.5), 6);
    assertEquals(sketch.getQuantile(0.6), 7);
    assertEquals(sketch.getQuantile(0.7), 8);
    assertEquals(sketch.getQuantile(0.8), 9);
    assertEquals(sketch.getQuantile(0.9), 10);
    assertEquals(sketch.getQuantile(1), 10); // always max value
    // inclusive = true
    assertEquals(sketch.getQuantile(0, INCLUSIVE), 1); // always min value
    assertEquals(sketch.getQuantile(0.1, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.2, INCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.3, INCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.4, INCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.5, INCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.6, INCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.7, INCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.8, INCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.9, INCLUSIVE), 9);
    assertEquals(sketch.getQuantile(1, INCLUSIVE), 10); // always max value

    // getQuantile() and getQuantiles() equivalence
    {
      // inclusive = false (default)
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
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    for (int i = 1; i <= 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getN(), 1000);
    assertTrue(sketch.getRetainedItems() < 1000);
    assertEquals(sketch.getMinValue(), Integer.valueOf(1));
    assertEquals(sketch.getMaxValue(), Integer.valueOf(1000));
    // based on ~1.7% normalized rank error for this particular case
    assertEquals(sketch.getQuantile(0.5), Integer.valueOf(500), 17);

    final double[] normRanks = {0.0, 0.5, 1.0};
    Integer[] quantiles = sketch.getQuantiles(normRanks);

    assertEquals(quantiles[1], Integer.valueOf(500), 17); // median


    final double[] normRanks2 = {.25, 0.5, 0.75};
    final Integer[] quantiles2 = sketch.getQuantiles(normRanks2);
    assertEquals(quantiles2[0], Integer.valueOf(250), 17);
    assertEquals(quantiles2[1], Integer.valueOf(500), 17);
    assertEquals(quantiles2[2], Integer.valueOf(750), 17);



    quantiles = sketch.getQuantiles(3);

    assertEquals(quantiles[1], Integer.valueOf(500), 17); // median


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
        intArr[i] = 20*i +10;
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
    final ItemsSketch<Long> sketch1 = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    for (int i = 1; i <= 500; i++) {
      sketch1.update((long) i);
    }

    final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    final byte[] bytes = sketch1.toByteArray(serDe);
    final ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);

    for (int i = 501; i <= 1000; i++) {
      sketch2.update((long) i);
    }
    assertEquals(sketch2.getN(), 1000);
    assertTrue(sketch2.getRetainedItems() < 1000);
    assertEquals(sketch2.getMinValue(), Long.valueOf(1));
    assertEquals(sketch2.getMaxValue(), Long.valueOf(1000));
    // based on ~1.7% normalized rank error for this particular case
    assertEquals(sketch2.getQuantile(0.5), Long.valueOf(500), 17);
  }

  @Test
  public void serializeDeserializeDouble() {
    final ItemsSketch<Double> sketch1 = ItemsSketch.getInstance(128, Comparator.naturalOrder());
    for (int i = 1; i <= 500; i++) {
      sketch1.update((double) i);
    }

    final ArrayOfItemsSerDe<Double> serDe = new ArrayOfDoublesSerDe();
    final byte[] bytes = sketch1.toByteArray(serDe);
    final ItemsSketch<Double> sketch2 = ItemsSketch.getInstance(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);

    for (int i = 501; i <= 1000; i++) {
      sketch2.update((double) i);
    }
    assertEquals(sketch2.getN(), 1000);
    assertTrue(sketch2.getRetainedItems() < 1000);
    assertEquals(sketch2.getMinValue(), Double.valueOf(1));
    assertEquals(sketch2.getMaxValue(), Double.valueOf(1000));
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
    final ItemsSketch<String> sketch1 = ItemsSketch.getInstance(128, numericOrder);
    for (int i = 1; i <= 500; i++)
     {
      sketch1.update(Integer.toBinaryString(i << 10)); // to make strings longer
    }

    final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();
    final byte[] bytes = sketch1.toByteArray(serDe);
    final ItemsSketch<String> sketch2 = ItemsSketch.getInstance(Memory.wrap(bytes), numericOrder, serDe);

    for (int i = 501; i <= 1000; i++) {
      sketch2.update(Integer.toBinaryString(i << 10));
    }
    assertEquals(sketch2.getN(), 1000);
    assertTrue(sketch2.getRetainedItems() < 1000);
    assertEquals(sketch2.getMinValue(), Integer.toBinaryString(1 << 10));
    assertEquals(sketch2.getMaxValue(), Integer.toBinaryString(1000 << 10));
    // based on ~1.7% normalized rank error for this particular case
    assertEquals(Integer.parseInt(sketch2.getQuantile(0.5), 2) >> 10, Integer.valueOf(500), 17);
  }

  @Test
  public void toStringCrudeCheck() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
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
    ItemsSketch.toString(Memory.wrap(bytes));
    //PreambleUtil.toString(bytes, true); // not a DoublesSketch so this will fail
    //ItemsSketch<String> sketch2 = ItemsSketch.getInstance(Memory.wrap(bytes), Comparator.naturalOrder(), serDe);
  }

  @Test
  public void toStringBiggerCheck() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, Comparator.naturalOrder());
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
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    sketch.downSample(32);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void zeroEvenlySpacedMustThrow() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, Comparator.naturalOrder());
    sketch.update("a");
    sketch.getQuantiles(0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void negativeQuantileMustThrow() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, Comparator.naturalOrder());
    sketch.update("ABC");
    sketch.getQuantile(-0.1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceExcep1() {
    final Memory mem = Memory.wrap(new byte[4]);
    ItemsSketch.getInstance(mem, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceExcep2() {
    final Memory mem = Memory.wrap(new byte[8]);
    ItemsSketch.getInstance(mem, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
  }

  @Test
  public void checkGoodSerDeId() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    final byte[] byteArr = sketch.toByteArray(new ArrayOfStringsSerDe());
    final Memory mem = Memory.wrap(byteArr);
    //println(PreambleUtil.toString(mem));
    ItemsSketch.getInstance(mem, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
  }

  @Test
  public void checkDownsample() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    final ItemsSketch<String> out = sketch.downSample(8);
    assertEquals(out.getK(), 8);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void unorderedSplitPoints() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1);
    sketch.getPMF(new Integer[] {2, 1});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nonUniqueSplitPoints() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1);
    sketch.getPMF(new Integer[] {1, 1});
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void nullInSplitPoints() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1);
    sketch.getPMF(new Integer[] {1, null});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void compactNotSupported() {
    final ArrayOfDoublesSerDe serDe = new ArrayOfDoublesSerDe();
    final ItemsSketch<Double> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    final byte[] byteArr = sketch.toByteArray(serDe);
    final WritableMemory mem = WritableMemory.writableWrap(byteArr);
    mem.clearBits(PreambleUtil.FLAGS_BYTE, (byte) PreambleUtil.COMPACT_FLAG_MASK);
    println(PreambleUtil.toString(mem, false));
    ItemsSketch.getInstance(mem, Comparator.naturalOrder(), serDe);
  }

  @Test
  public void checkPutMemory() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    final byte[] byteArr = new byte[200];
    final WritableMemory mem = WritableMemory.writableWrap(byteArr);
    sketch.putMemory(mem, new ArrayOfStringsSerDe());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPutMemoryException() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, Comparator.naturalOrder());
    for (int i=0; i<40; i++) {
      sketch.update(Integer.toString(i));
    }
    final byte[] byteArr = new byte[100];
    final WritableMemory mem = WritableMemory.writableWrap(byteArr);
    sketch.putMemory(mem, new ArrayOfStringsSerDe());
  }

  @Test
  public void checkPMFonEmpty() {
    final ItemsSketch<String> iss = buildStringIS(32, 32);
    final double[] ranks = new double[0];
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
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
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
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Comparator.<Integer>naturalOrder().reversed());
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
    final ItemsSketch<Double> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
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
    final ItemsSketch<Double> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
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
    Memory mem;
    ItemsSketch<String> is2;
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    //ordered
    byteArr = is.toByteArray(true, serDe);
    mem = Memory.wrap(byteArr);
    is2 = ItemsSketch.getInstance(mem, Comparator.naturalOrder(), serDe);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(is.getQuantile(f), is2.getQuantile(f));
    }

    //Not-ordered
    byteArr = is.toByteArray(false, serDe);
    mem = Memory.wrap(byteArr);
    is2 = ItemsSketch.getInstance(mem, Comparator.naturalOrder(), serDe);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(is.getQuantile(f), is2.getQuantile(f));
    }
  }

  static ItemsSketch<String> buildStringIS(final int k, final int n) {
    return buildStringIS(k, n, 0);
  }

  static ItemsSketch<String> buildStringIS(final int k, final int n, final int start) {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(k, Comparator.naturalOrder());
    for (int i = 0; i < n; i++) {
      sketch.update(Integer.toString(i + start));
    }
    return sketch;
  }

  @Test
  public void testOrdering() {
    final Comparator<String> natural = Comparator.naturalOrder();
    final Comparator<String> reverse = natural.reversed();
    final Comparator<String> numeric = natural.thenComparing(
        new Function<String, Integer>() {
          @Override
          public Integer apply(final String s) {
            return Integer.valueOf(s);
          }
        }
    );
    for (final Comparator<String> c : Arrays.asList(natural, reverse, numeric)) {
      final ItemsSketch<String> sketch = ItemsSketch.getInstance(16, c);
      for (int i = 0; i < 10000; i++) {
        sketch.update(String.valueOf(ItemsSketch.rand.nextInt(1000000)));
      }
      final String[] quantiles = sketch.getQuantiles(100);
      final String[] sorted = Arrays.copyOf(quantiles, quantiles.length);
      Arrays.sort(sorted, c);
      assertEquals(quantiles, sorted, c.toString());
    }
  }

  @Test
  public void sortedView() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(3);
    sketch.update(1);
    sketch.update(2);
    { // cumulative inclusive
      final ItemsSketchSortedView<Integer> view = sketch.getSortedView();
      final ItemsSketchSortedViewIterator<Integer> it = view.iterator();
      assertEquals(it.next(), true);
      assertEquals(it.getItem(), 1);
      assertEquals(it.getWeight(), 1);
      assertEquals(it.getCumulativeWeight(INCLUSIVE), 1);
      assertEquals(it.next(), true);
      assertEquals(it.getItem(), 2);
      assertEquals(it.getWeight(), 1);
      assertEquals(it.getCumulativeWeight(INCLUSIVE), 2);
      assertEquals(it.next(), true);
      assertEquals(it.getItem(), 3);
      assertEquals(it.getWeight(), 1);
      assertEquals(it.getCumulativeWeight(INCLUSIVE), 3);
      assertEquals(it.next(), false);
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
