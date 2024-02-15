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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;

import java.util.Comparator;

import org.apache.datasketches.quantilescommon.GenericPartitionBoundaries;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.testng.annotations.Test;

public class ItemsSketchPartitionBoundariesTest {
  private static final int k = 128;

  @Test
  public void checkSimpleEndsAdjustment() {
    final String[] quantiles = {"2","4","6","7"};
    final long[] cumWeights = {2, 4, 6, 8};
    final long totalN = 8;
    final Comparator<String> comparator = Comparator.naturalOrder();
    final String maxItem = "8";
    final String minItem = "1";
    ItemsSketchSortedViewString sv = new ItemsSketchSortedViewString(
        quantiles, cumWeights, totalN, comparator, maxItem, minItem, k);

    GenericSortedViewIterator<String> itr = sv.iterator();
    while (itr.next()) {
      println(itr.getNaturalRank(INCLUSIVE) + ", " + itr.getQuantile(INCLUSIVE));
    }
    GenericPartitionBoundaries<String> gpb = sv.getPartitionBoundaries(2);
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
    println("PRINTING: "+this.getClass().getName());
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
