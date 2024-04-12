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

package org.apache.datasketches.quantilescommon;

import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.digits;
import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.getString;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.testng.annotations.Test;

/**
 * This tests partition boundaries with both KllItemsSketch and classic ItemsSketch
 */
public class PartitionBoundariesTest {
  private ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
  private static String[] hdr     = {"N", "MaxItem", "MinItem", "NumParts", "SearchCriteria"};
  private static String hdrfmt    = "%6s %10s %10s %10s %15s\n";
  private static String hdrdfmt   = "%6d %10s %10s %10d %15s\n";

  private static String[] rowhdr  = {"Row", "NormRanks", "NatRanks", "Boundaries", "DeltaItems"};
  private static String rowhdrfmt = "%5s %12s %12s %12s %12s\n";
  private static String rowdfmt   = "%5d %12.8f %12d %12s %12d\n";

  private static String[] rowhdr2 = {"Row", "NormRanks", "NatRanks", "Boundaries"};
  private static String rowhdrfmt2= "%5s %12s %12s %12s\n";
  private static String rowdfmt2  = "%5d %12.8f %12d %12s\n";

  public void checkSkewWithClassic() {
    int n = 2050;
    int k = 1 << 15;
    int n2 = 200;
    int totalN = n + n2;
    int numDigits = digits(totalN);
    long v2 = 1000L;
    int numParts = 22;
    QuantileSearchCriteria searchCrit = QuantileSearchCriteria.INCLUSIVE;
    ItemsSketch<String> sk = ItemsSketch.getInstance(String.class,k, Comparator.naturalOrder());

    for (long i = 1; i <= n; i++)  { sk.update(getString(i, numDigits)); }
    for (long i = 1; i <= n2; i++) { sk.update(getString(v2, numDigits)); }
    ItemsSketchSortedView<String> sv = sk.getSortedView();
    GenericSortedViewIterator<String> itr = sv.iterator();
    println("SORTED VIEW:");
    printf(rowhdrfmt2, (Object[])rowhdr2);
    int j = 0;
    while (itr.next()) {
      printf(rowdfmt2, j++, itr.getNormalizedRank(searchCrit), itr.getNaturalRank(searchCrit), itr.getQuantile());
    }

    GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundariesFromNumParts(numParts, searchCrit);
    int arrLen = gpb.getBoundaries().length;
    double[] normRanks = gpb.getNormalizedRanks();
    long[] natRanks = gpb.getNaturalRanks();
    String[] boundaries = gpb.getBoundaries();
    long[] numDeltaItems = gpb.getNumDeltaItems();
    println("");
    println("GET PARTITION BOUNDARIES:");
    printf(hdrfmt, (Object[]) hdr);
    printf(hdrdfmt, totalN, gpb.getMaxItem(), gpb.getMinItem(), numParts, searchCrit.toString());
    println("");
    printf(rowhdrfmt, (Object[]) rowhdr);
    for (int i = 0; i < arrLen; i++) {
      printf(rowdfmt, i, normRanks[i], natRanks[i], boundaries[i], numDeltaItems[i]);
    }
  }

  @Test
  public void checkSkewWithKll() {
    int n = 2050;
    int k = 1 << 15;
    int n2 = 200;
    int totalN = n + n2;
    int numDigits = digits(totalN);
    long v2 = 1000L;
    int numParts = 22;
    QuantileSearchCriteria searchCrit = QuantileSearchCriteria.INCLUSIVE;
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);

    for (long i = 1; i <= n; i++)  { sk.update(getString(i, numDigits)); }
    for (long i = 1; i <= n2; i++) { sk.update(getString(v2, numDigits)); }
    ItemsSketchSortedView<String> sv = sk.getSortedView();
    GenericSortedViewIterator<String> itr = sv.iterator();
    println("SORTED VIEW:");
    printf(rowhdrfmt2, (Object[])rowhdr2);
    int j = 0;
    while (itr.next()) {
      printf(rowdfmt2, j++, itr.getNormalizedRank(searchCrit), itr.getNaturalRank(searchCrit), itr.getQuantile());
    }

    GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundariesFromNumParts(numParts, searchCrit);
    int arrLen = gpb.getBoundaries().length;
    double[] normRanks = gpb.getNormalizedRanks();
    long[] natRanks = gpb.getNaturalRanks();
    String[] boundaries = gpb.getBoundaries();
    long[] numDeltaItems = gpb.getNumDeltaItems();
    println("");
    println("GET PARTITION BOUNDARIES:");
    printf(hdrfmt, (Object[]) hdr);
    printf(hdrdfmt, totalN, gpb.getMaxItem(), gpb.getMinItem(), numParts, searchCrit.toString());
    println("");
    printf(rowhdrfmt, (Object[]) rowhdr);
    for (int i = 0; i < arrLen; i++) {
      printf(rowdfmt, i, normRanks[i], natRanks[i], boundaries[i], numDeltaItems[i]);
    }
  }

  @Test
  public void getQuantilesVsPartitionBoundariesKll() {
    final KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sketch.update("A");
    sketch.update("B");
    sketch.update("C");
    sketch.update("D");
    String[] quantiles1 = sketch.getQuantiles(new double[] {0.0, 0.5, 1.0}, EXCLUSIVE);
    String[] quantiles2 = sketch.getPartitionBoundariesFromNumParts(2, EXCLUSIVE).getBoundaries();
    assertEquals(quantiles1, quantiles2);
    quantiles1 = sketch.getQuantiles(new double[] {0.0, 0.5, 1.0}, INCLUSIVE);
    quantiles2 = sketch.getPartitionBoundariesFromNumParts(2, INCLUSIVE).getBoundaries();
    assertEquals(quantiles1, quantiles2);
  }

  @Test
  public void getQuantilesVsPartitionBoundariesClassic() {
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(Integer.class, Comparator.naturalOrder());
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    Integer[] quantiles1 = sketch.getQuantiles(new double[] {0.0, 0.5, 1.0}, EXCLUSIVE);
    Integer[] quantiles2 = sketch.getPartitionBoundariesFromNumParts(2, EXCLUSIVE).getBoundaries();
    assertEquals(quantiles1, quantiles2);
    quantiles1 = sketch.getQuantiles(new double[] {0.0, 0.5, 1.0}, INCLUSIVE);
    quantiles2 = sketch.getPartitionBoundariesFromNumParts(2, INCLUSIVE).getBoundaries();
    assertEquals(quantiles1, quantiles2);
  }

  /**
   * Because both Kll and Classic items sketches use the same Sorted View class
   * this test applies to both. The only difference is a different normalized error given the same k.
   */
  @Test
  public void checkSimpleEndsAdjustment() {
    final String[] quantiles = {"2","4","6","7"};
    final long[] cumWeights = {2, 4, 6, 8};
    final long totalN = 8;
    final Comparator<String> comparator = Comparator.naturalOrder();
    final String maxItem = "8";
    final String minItem = "1";
    ItemsSketchSortedView<String> sv = new ItemsSketchSortedView<>(
        quantiles, cumWeights, totalN, comparator, maxItem, minItem);

    GenericSortedViewIterator<String> itr = sv.iterator();
    while (itr.next()) {
      println(itr.getNaturalRank(INCLUSIVE) + ", " + itr.getQuantile(INCLUSIVE));
    }
    GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundariesFromNumParts(2);
    String[] boundaries = gpb.getBoundaries();
    long[] natRanks = gpb.getNaturalRanks();
    double[] normRanks = gpb.getNormalizedRanks();
    long[] deltaItems = gpb.getNumDeltaItems();
    int numParts = gpb.getNumPartitions();
    String maxItm = gpb.getMaxItem();
    String minItm = gpb.getMinItem();
    assertEquals(boundaries, new String[] {"1","4","8"});
    assertEquals(natRanks, new long[] {1,4,8});
    assertEquals(normRanks, new double[] {.125,.5,1.0});
    assertEquals(deltaItems, new long[] {0,4,4});
    assertEquals(numParts, 2);
    assertEquals(maxItm, "8");
    assertEquals(minItm, "1");
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  private final static boolean enablePrinting = false;

  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }
}
