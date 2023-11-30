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

import java.util.Comparator;

import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.*;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.GenericPartitionBoundaries;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.testng.annotations.Test;

/**
 * blah
 */
@SuppressWarnings("unused")
public class SkewedDataTest {
  static String[] hdr     = {"N", "MaxItem", "MinItem", "NumParts", "SearchCriteria"};
  static String hdrfmt    = "%6s %10s %10s %10s %15s\n";
  static String hdrdfmt   = "%6d %10s %10s %10d %15s\n";

  static String[] rowhdr  = {"Row", "NormRanks", "NatRanks", "Boundaries", "DeltaItems"};
  static String rowhdrfmt = "%5s %12s %12s %12s %12s\n";
  static String rowdfmt   = "%5d %12.8f %12d %12s %12d\n";

  static String[] rowhdr2 = {"Row", "NormRanks", "NatRanks", "Boundaries"};
  static String rowhdrfmt2= "%5s %12s %12s %12s\n";
  static String rowdfmt2  = "%5d %12.8f %12d %12s\n";

  //@Test //visual only
  public void checkWithSkew() {
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

    GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundaries(numParts, searchCrit);
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

  private final static boolean enablePrinting = true;

  /**
   * @param o the Object to print
   */
  private static final void print(final Object o) {
    if (enablePrinting) { System.out.print(o.toString()); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }


}
