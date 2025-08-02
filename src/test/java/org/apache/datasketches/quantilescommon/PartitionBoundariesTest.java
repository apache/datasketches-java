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

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.digits;
import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.getString;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.testng.annotations.Test;

/**
 * This tests partition boundaries with both KllItemsSketch and classic ItemsSketch
 */
public class PartitionBoundariesTest {
  private final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
  private static String[] hdr     = {"N", "MaxItem", "MinItem", "NumParts", "SearchCriteria"};
  private static String hdrfmt    = "%6s %10s %10s %10s %15s" + LS;
  private static String hdrdfmt   = "%6d %10s %10s %10d %15s" + LS;

  private static String[] rowhdr  = {"Row", "NormRanks", "NatRanks", "Boundaries", "DeltaItems"};
  private static String rowhdrfmt = "%5s %12s %12s %12s %12s" + LS;
  private static String rowdfmt   = "%5d %12.8f %12d %12s %12d" + LS;

  private static String[] rowhdr2 = {"Row", "NormRanks", "NatRanks", "Boundaries"};
  private static String rowhdrfmt2= "%5s %12s %12s %12s" + LS;
  private static String rowdfmt2  = "%5d %12.8f %12d %12s" + LS;

  //@Test //visual check only. set enablePrinting = true to view.
  public void checkSkewWithClassic() {
    final int n = 2050; //1000000;
    final int k = 1 << 15;
    final int n2 = 200;
    final int totalN = n + n2;
    final int numDigits = digits(totalN);
    final long v2 = 1000L;
    final QuantileSearchCriteria searchCrit = QuantileSearchCriteria.INCLUSIVE;
    final ItemsSketch<String> sk = ItemsSketch.getInstance(String.class,k, Comparator.naturalOrder());

    for (long i = 1; i <= n; i++)  { sk.update(getString(i, numDigits)); }
    for (long i = 1; i <= n2; i++) { sk.update(getString(v2, numDigits)); }
    final int numParts = sk.getMaxPartitions(); //22
    final ItemsSketchSortedView<String> sv = sk.getSortedView();
    final GenericSortedViewIterator<String> itr = sv.iterator();
    println("SORTED VIEW:");
    printf(rowhdrfmt2, (Object[])rowhdr2);
    int j = 0;
    while (itr.next()) {
      printf(rowdfmt2, j++, itr.getNormalizedRank(searchCrit), itr.getNaturalRank(searchCrit), itr.getQuantile());
    }

    final GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundariesFromNumParts(numParts, searchCrit);
    final int arrLen = gpb.getBoundaries().length;
    final double[] normRanks = gpb.getNormalizedRanks();
    final long[] natRanks = gpb.getNaturalRanks();
    final String[] boundaries = gpb.getBoundaries();
    final long[] numDeltaItems = gpb.getNumDeltaItems();
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

  //@Test //visual check only. set enablePrinting = true to view.
  public void checkSkewWithKll() {
    final int n = 2050; //1_000_000;
    final int k = 1 << 15;
    final int n2 = 200;
    final int totalN = n + n2;
    final int numDigits = digits(totalN);
    final long v2 = 1000L;
    final QuantileSearchCriteria searchCrit = QuantileSearchCriteria.INCLUSIVE;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), serDe);

    for (long i = 1; i <= n; i++)  { sk.update(getString(i, numDigits)); }
    for (long i = 1; i <= n2; i++) { sk.update(getString(v2, numDigits)); }
    final int numParts = sk.getMaxPartitions(); //22
    final ItemsSketchSortedView<String> sv = sk.getSortedView();
    final GenericSortedViewIterator<String> itr = sv.iterator();
    println("SORTED VIEW:");
    printf(rowhdrfmt2, (Object[])rowhdr2);
    int j = 0;
    while (itr.next()) {
      printf(rowdfmt2, j++, itr.getNormalizedRank(searchCrit), itr.getNaturalRank(searchCrit), itr.getQuantile());
    }

    final GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundariesFromNumParts(numParts, searchCrit);
    final int arrLen = gpb.getBoundaries().length;
    final double[] normRanks = gpb.getNormalizedRanks();
    final long[] natRanks = gpb.getNaturalRanks();
    final String[] boundaries = gpb.getBoundaries();
    final long[] numDeltaItems = gpb.getNumDeltaItems();
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
   * Because both Kll and Classic items sketches use the same Sorted View class.
   * This test applies to both.
   */
  @Test
  public void checkSimpleEndsAdjustment() {
    final String[] quantiles = {"2","4","6","7"};
    final long[] cumWeights = {2, 4, 6, 8};
    final long totalN = 8;
    final Comparator<String> comparator = Comparator.naturalOrder();
    final String maxItem = "8";
    final String minItem = "1";
    final ItemsSketchSortedView<String> sv = new ItemsSketchSortedView<>(
        quantiles, cumWeights, totalN, comparator, maxItem, minItem,
        String.class, .01, 4);

    final GenericSortedViewIterator<String> itr = sv.iterator();
    while (itr.next()) {
      println(itr.getNaturalRank(INCLUSIVE) + ", " + itr.getQuantile(INCLUSIVE));
    }
    final GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundariesFromNumParts(2);
    final String[] boundaries = gpb.getBoundaries();
    final long[] natRanks = gpb.getNaturalRanks();
    final double[] normRanks = gpb.getNormalizedRanks();
    final long[] deltaItems = gpb.getNumDeltaItems();
    final int numParts = gpb.getNumPartitions();
    final String maxItm = gpb.getMaxItem();
    final String minItm = gpb.getMinItem();
    assertEquals(boundaries, new String[] {"1","4","8"});
    assertEquals(natRanks, new long[] {1,4,8});
    assertEquals(normRanks, new double[] {.125,.5,1.0});
    assertEquals(deltaItems, new long[] {0,4,4});
    assertEquals(numParts, 2);
    assertEquals(maxItm, "8");
    assertEquals(minItm, "1");
  }

  @SuppressWarnings("unused")
  @Test //For visual check, set enablePrinting = true to view.
  public void checkSketchPartitionLimits() {
    final long totalN = 2000; //1_000_000;
    final Comparator<String> comparator = Comparator.naturalOrder();
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final int k = 1 << 15;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, comparator, serDe);
    final int d = digits(totalN);
    for (int i = 1; i <= totalN; i++) {
      sk.update(getString(i, d));
    }
    //***
    final int numRet = sk.getNumRetained();
    println("NumRetained: " + numRet + " /2: " + (numRet / 2));
    final double eps = sk.getNormalizedRankError(true);
    printf("NormRankErr: %10.6f     1/eps: %10.3f" + LS, eps, 1/eps);
    //***
    //this should pass
    final int goodNumPartsRequest = sk.getMaxPartitions();
    println("Good numPartsRequest " + goodNumPartsRequest);
    GenericPartitionBoundaries<String> gpb = sk.getPartitionBoundariesFromNumParts(goodNumPartsRequest);
    //this should fail
    try {
      final int badNumPartsRequest = goodNumPartsRequest + 1;
      println("Bad numPartsRequest " + badNumPartsRequest);
      gpb = sk.getPartitionBoundariesFromNumParts(badNumPartsRequest);
      fail("Bad numPartsRequest should have failed. " + badNumPartsRequest);
    } catch (final SketchesArgumentException e) { } //OK
  }

  @SuppressWarnings("unused")
  @Test //For visual check, set enablePrinting = true to view.
  public void checkSketchPartitionLimits2() {
    final long totalN = 2000; //1_000_000;
    final Comparator<String> comparator = Comparator.naturalOrder();
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final int k = 1 << 15;
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, comparator, serDe);
    final int d = digits(totalN);
    for (int i = 1; i <= totalN; i++) {
      sk.update(getString(i, d));
    }
    final double eps = sk.getNormalizedRankError(true);
    printf("NormRankErr: %10.6f     1/eps: %10.3f" + LS, eps, 1/eps);
    println("N: " + sk.getN());
    println("Max Parts: " + sk.getMaxPartitions());

    //this should pass
    final long goodPartSizeRequest= sk.getMinPartitionSizeItems();
    println("Good partSizeRequest " + goodPartSizeRequest);
    final GenericPartitionBoundaries<String> gpb = sk.getPartitionBoundariesFromPartSize(goodPartSizeRequest);
    //this should fail
    try {
      final long badPartSizeRequest = goodPartSizeRequest - 1;
      println("Bad partSizeRequest " + badPartSizeRequest);
    } catch (final SketchesArgumentException e) { } //OK
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
