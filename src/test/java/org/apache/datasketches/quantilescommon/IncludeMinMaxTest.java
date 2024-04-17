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
import static org.apache.datasketches.quantilescommon.IncludeMinMax.DoublesPair;
import static org.apache.datasketches.quantilescommon.IncludeMinMax.FloatsPair;
import static org.apache.datasketches.quantilescommon.IncludeMinMax.ItemsPair;
import static org.testng.Assert.assertEquals;

import java.util.Comparator;

import org.testng.annotations.Test;

/**
 * Checks the IncludeMinMax process
 */
public final class IncludeMinMaxTest {

  @Test
  public static void checkDoublesEndsAdjustment() {
    final double[] quantiles = {2, 4, 6, 7};
    final long[] cumWeights = {2, 4, 6, 8};

    final double maxItem = 8;
    final double minItem = 1;
    final DoublesPair dPair = IncludeMinMax.includeDoublesMinMax(quantiles, cumWeights, maxItem, minItem);
    final double[] adjQuantiles = dPair.quantiles;
    final long[] adjCumWeights = dPair.cumWeights;
    int len = adjCumWeights.length;
    printf("%10s %10s" + LS, "Quantiles", "CumWeights");
    for (int i = 0; i < len; i++) {
      printf("%10.1f %10d" + LS, adjQuantiles[i], adjCumWeights[i]);
    }
    final int topIn = quantiles.length - 1;
    final int topAdj = adjQuantiles.length - 1;
    assertEquals(adjQuantiles[topAdj], maxItem);
    assertEquals(adjQuantiles[0], minItem);
    assertEquals(adjCumWeights[topAdj], cumWeights[topIn]);
    assertEquals(adjCumWeights[0], 1);
    assertEquals(adjQuantiles.length - quantiles.length, 2);
    assertEquals(adjCumWeights.length - cumWeights.length, 2);
  }

  @Test
  public static void checkDoublesEndsAdjustment2() {
    final double[] quantiles = {2, 4, 6, 7};
    final long[] cumWeights = {2, 4, 6, 8};

    final double maxItem = 7;
    final double minItem = 2;
    final DoublesPair dPair = IncludeMinMax.includeDoublesMinMax(quantiles, cumWeights, maxItem, minItem);
    final double[] adjQuantiles = dPair.quantiles;
    final long[] adjCumWeights = dPair.cumWeights;
    int len = adjCumWeights.length;
    printf("%10s %10s" + LS, "Quantiles", "CumWeights");
    for (int i = 0; i < len; i++) {
      printf("%10.1f %10d" + LS, adjQuantiles[i], adjCumWeights[i]);
    }
    final int topIn = quantiles.length - 1;
    final int topAdj = adjQuantiles.length - 1;
    assertEquals(adjQuantiles[topAdj], maxItem);
    assertEquals(adjQuantiles[0], minItem);
    assertEquals(adjCumWeights[topAdj], cumWeights[topIn]);
    assertEquals(adjCumWeights[0], cumWeights[0]);
    assertEquals(adjQuantiles.length - quantiles.length, 0);
    assertEquals(adjCumWeights.length - cumWeights.length, 0);
  }

  @Test
  public static void checkFloatsEndsAdjustment() {
    final float[] quantiles = {2, 4, 6, 7};
    final long[] cumWeights = {2, 4, 6, 8};

    final float maxItem = 8;
    final float minItem = 1;
    final FloatsPair dPair = IncludeMinMax.includeFloatsMinMax(quantiles, cumWeights, maxItem, minItem);
    final float[] adjQuantiles = dPair.quantiles;
    final long[] adjCumWeights = dPair.cumWeights;
    int len = adjCumWeights.length;
    printf("%10s %10s" + LS, "Quantiles", "CumWeights");
    for (int i = 0; i < len; i++) {
      printf("%10.1f %10d" + LS, adjQuantiles[i], adjCumWeights[i]);
    }
    final int topIn = quantiles.length - 1;
    final int topAdj = adjQuantiles.length - 1;
    assertEquals(adjQuantiles[topAdj], maxItem);
    assertEquals(adjQuantiles[0], minItem);
    assertEquals(adjCumWeights[topAdj], cumWeights[topIn]);
    assertEquals(adjCumWeights[0], 1);
    assertEquals(adjQuantiles.length - quantiles.length, 2);
    assertEquals(adjCumWeights.length - cumWeights.length, 2);
  }

  @Test
  public static void checkFloatsEndsAdjustment2() {
    final float[] quantiles = {2, 4, 6, 7};
    final long[] cumWeights = {2, 4, 6, 8};

    final float maxItem = 7;
    final float minItem = 2;
    final FloatsPair dPair = IncludeMinMax.includeFloatsMinMax(quantiles, cumWeights, maxItem, minItem);
    final float[] adjQuantiles = dPair.quantiles;
    final long[] adjCumWeights = dPair.cumWeights;
    int len = adjCumWeights.length;
    printf("%10s %10s" + LS, "Quantiles", "CumWeights");
    for (int i = 0; i < len; i++) {
      printf("%10.1f %10d" + LS, adjQuantiles[i], adjCumWeights[i]);
    }
    final int topIn = quantiles.length - 1;
    final int topAdj = adjQuantiles.length - 1;
    assertEquals(adjQuantiles[topAdj], maxItem);
    assertEquals(adjQuantiles[0], minItem);
    assertEquals(adjCumWeights[topAdj], cumWeights[topIn]);
    assertEquals(adjCumWeights[0], cumWeights[0]);
    assertEquals(adjQuantiles.length - quantiles.length, 0);
    assertEquals(adjCumWeights.length - cumWeights.length, 0);
  }

  @Test
  public static void checkItemsEndsAdjustment() {
    final String[] quantiles = {"2", "4", "6", "7"};
    final long[] cumWeights = {2, 4, 6, 8};

    final String maxItem = "8";
    final String minItem = "1";
    final ItemsPair<String> dPair =
        IncludeMinMax.includeItemsMinMax(quantiles, cumWeights, maxItem, minItem, Comparator.naturalOrder());
    final String[] adjQuantiles = dPair.quantiles;
    final long[] adjCumWeights = dPair.cumWeights;
    int len = adjCumWeights.length;
    printf("%10s %10s" + LS, "Quantiles", "CumWeights");
    for (int i = 0; i < len; i++) {
      printf("%10s %10d" + LS, adjQuantiles[i], adjCumWeights[i]);
    }
    final int topIn = quantiles.length - 1;
    final int topAdj = adjQuantiles.length - 1;
    assertEquals(adjQuantiles[topAdj], maxItem);
    assertEquals(adjQuantiles[0], minItem);
    assertEquals(adjCumWeights[topAdj], cumWeights[topIn]);
    assertEquals(adjCumWeights[0], 1);
    assertEquals(adjQuantiles.length - quantiles.length, 2);
    assertEquals(adjCumWeights.length - cumWeights.length, 2);
  }

  @Test
  public static void checkItemsEndsAdjustment2() {
    final String[] quantiles = {"2", "4", "6", "7"};
    final long[] cumWeights = {2, 4, 6, 8};

    final String maxItem = "7";
    final String minItem = "2";
    final ItemsPair<String> dPair =
        IncludeMinMax.includeItemsMinMax(quantiles, cumWeights, maxItem, minItem, Comparator.naturalOrder());
    final String[] adjQuantiles = dPair.quantiles;
    final long[] adjCumWeights = dPair.cumWeights;
    int len = adjCumWeights.length;
    printf("%10s %10s" + LS, "Quantiles", "CumWeights");
    for (int i = 0; i < len; i++) {
      printf("%10s %10d" + LS, adjQuantiles[i], adjCumWeights[i]);
    }
    final int topIn = quantiles.length - 1;
    final int topAdj = adjQuantiles.length - 1;
    assertEquals(adjQuantiles[topAdj], maxItem);
    assertEquals(adjQuantiles[0], minItem);
    assertEquals(adjCumWeights[topAdj], cumWeights[topIn]);
    assertEquals(adjCumWeights[0], cumWeights[0]);
    assertEquals(adjQuantiles.length - quantiles.length, 0);
    assertEquals(adjCumWeights.length - cumWeights.length, 0);
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
