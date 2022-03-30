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

package org.apache.datasketches.kll;

import static java.lang.Math.pow;
import static org.apache.datasketches.Util.floorPowerOf2;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.MAX_K;
import static org.apache.datasketches.kll.KllPreambleUtil.MAX_M;
import static org.apache.datasketches.kll.KllPreambleUtil.MIN_M;
import static org.apache.datasketches.kll.KllSketch.CDF_COEF;
import static org.apache.datasketches.kll.KllSketch.CDF_EXP;
import static org.apache.datasketches.kll.KllSketch.PMF_COEF;
import static org.apache.datasketches.kll.KllSketch.PMF_EXP;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.KllSketch.SketchType;

/**
 * This class provides some useful sketch analysis tools that are used internally and also can be used by
 * interested users to understand the internal structure of the sketch as well as the growth properties of the
 * sketch given a stream length.
 *
 * @author lrhodes
 *
 */
public class KllHelper {

  public static class GrowthStats {
    SketchType sketchType;
    int k;
    int m;
    long givenN;
    long maxN;
    int numLevels;
    int maxItems;
    int compactBytes;
    int updatableBytes;
  }

  public static class LevelStats {
    long n;
    int numLevels;
    int items;

    LevelStats(final long n, final int numLevels, final int items) {
      this.n = n;
      this.numLevels = numLevels;
      this.items = items;
    }
  }

  /**
   * This is the exact powers of 3 from 3^0 to 3^30 where the exponent is the index
   */
  private static final long[] powersOfThree =
      new long[] {1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
  1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
  3486784401L, 10460353203L, 31381059609L, 94143178827L, 282429536481L,
  847288609443L, 2541865828329L, 7625597484987L, 22876792454961L, 68630377364883L,
  205891132094649L};

  /**
   * Given k, m, and numLevels, this computes and optionally prints the structure of the sketch when the given
   * number of levels are completely filled.
   * @param k the given user configured sketch parameter
   * @param m the given user configured sketch parameter
   * @param numLevels the given number of levels of the sketch
   * @param printSketchStructure if true will print the details of the sketch structure at the given numLevels.
   * @return LevelStats with the final summary of the sketch's cumulative N,
   * and cumulative items at the given numLevels.
   */
  public static LevelStats getFinalSketchStatsAtNumLevels(
      final int k,
      final int m,
      final int numLevels,
      final boolean printSketchStructure) {
    int cumItems = 0;
    long cumN = 0;
    if (printSketchStructure) {
      println("SKETCH STRUCTURE:");
      println("Given K        : " + k);
      println("Given M        : " + m);
      println("Given NumLevels: " + numLevels);
      printf("%6s %8s %12s %18s %18s\n", "Level", "Items", "CumItems", "N at Level", "CumN");
    }
    for (int level = 0; level < numLevels; level++) {
      final LevelStats lvlStats = getLevelCapacityItems(k, m, numLevels, level);
      cumItems += lvlStats.items;
      cumN += lvlStats.n;
      if (printSketchStructure) {
        printf("%6d %,8d %,12d %,18d %,18d\n", level, lvlStats.items, cumItems, lvlStats.n, cumN);
      }
    }
    return new LevelStats(cumN, numLevels, cumItems);
  }

  /**
   * Given k, m, n, and the sketch type, this computes (and optionally prints) the growth scheme for a sketch as it
   * grows large enough to accommodate a stream length of n items.
   * @param k the given user configured sketch parameter
   * @param m the given user configured sketch parameter
   * @param n the desired stream length
   * @param sketchType the given sketch type (DOUBLES_SKETCH or FLOATS_SKETCH)
   * @param printGrowthScheme if true the entire growth scheme of the sketch will be printed.
   * @return GrowthStats with the final values of the growth scheme
   */
  public static GrowthStats getGrowthSchemeForGivenN(
      final int k,
      final int m,
      final long n,
      final SketchType sketchType,
      final boolean printGrowthScheme) {
    int numLevels = 0;
    LevelStats lvlStats;
    final GrowthStats gStats = new GrowthStats();
    gStats.k = k;
    gStats.m = m;
    gStats.givenN = n;
    gStats.sketchType = sketchType;
    if (printGrowthScheme) {
      println("GROWTH SCHEME:");
      println("Given SketchType: " + sketchType.toString());
      println("Given K         : " + k);
      println("Given M         : " + m);
      println("Given N         : " + n);
      printf("%10s %10s %20s %13s %15s\n", "NumLevels", "MaxItems", "MaxN", "CompactBytes", "UpdatableBytes");
    }
    int compactBytes;
    int updatableBytes;
    do {
      numLevels++;
      lvlStats = getFinalSketchStatsAtNumLevels(k, m, numLevels, false);
      final int maxItems = lvlStats.items;
      final long maxN = lvlStats.n;
      if (sketchType == DOUBLES_SKETCH) {
        compactBytes = maxItems * Double.BYTES + numLevels * Integer.BYTES + 2 * Double.BYTES + DATA_START_ADR_DOUBLE;
        updatableBytes = compactBytes + Integer.BYTES;
      } else {
        compactBytes = maxItems * Float.BYTES + numLevels * Integer.BYTES + 2 * Float.BYTES + DATA_START_ADR_FLOAT;
        updatableBytes = compactBytes + Integer.BYTES;
      }
      if (printGrowthScheme) {
        printf("%10d %,10d %,20d %,13d %,15d\n", numLevels, maxItems, maxN, compactBytes, updatableBytes);
      }
    } while (lvlStats.n < n);
    gStats.maxN = lvlStats.n;
    gStats.numLevels = lvlStats.numLevels;
    gStats.maxItems = lvlStats.items;
    gStats.compactBytes = compactBytes;
    gStats.updatableBytes = updatableBytes;
    return gStats;
  }

  /**
   * Given k, m, numLevels, this computes the item capacity of a single level.
   * @param k the given user sketch configuration parameter
   * @param m the given user sketch configuration parameter
   * @param numLevels the given number of levels of the sketch
   * @param level the specific level to compute its item capacity
   * @return LevelStats with the computed N and items for the given level.
   */
  public static LevelStats getLevelCapacityItems(
      final int k,
      final int m,
      final int numLevels,
      final int level) {
    final int items = KllHelper.levelCapacity(k, numLevels, level, m);
    final long n = (long)items << level;
    return new LevelStats(n, numLevels, items);
  }

  /**
   * Checks the validity of the given value k
   * @param k must be greater than 7 and less than 65536.
   */
  static void checkK(final int k, final int m) {
    if (k < m || k > MAX_K) {
      throw new SketchesArgumentException(
          "K must be >= " + m + " and <= " + MAX_K + ": " + k);
    }
  }

  static void checkM(final int m) {
    if (m < MIN_M || m > MAX_M || ((m & 1) == 1)) {
      throw new SketchesArgumentException(
          "M must be >= 2, <= 8 and even: " + m);
    }
  }

  /**
   * Returns the maximum number of items that this sketch can handle
   * @param k The sizing / accuracy parameter of the sketch in items.
   * Note: this method actually works for k values up to k = 2^29 and 61 levels,
   * however only k values up to (2^16 - 1) are currently used by the sketch.
   * @param m the size of the smallest level in items. Default is 8.
   * @param numLevels the upper bound number of levels based on <i>n</i> items.
   * @return the total item capacity of the sketch.
   */
  static int computeTotalItemCapacity(final int k, final int m, final int numLevels) {
    long total = 0;
    for (int level = 0; level < numLevels; level++) {
      total += levelCapacity(k, numLevels, level, m);
    }
    return (int) total;
  }

  static int currentLevelSize(final int level, final int numLevels, final int[] levels) {
    if (level >= numLevels) { return 0; }
    return levels[level + 1] - levels[level];
  }

  /**
   * Finds the first level starting with level 0 that exceeds its nominal capacity
   * @param k configured size of sketch. Range [m, 2^16]
   * @param m minimum level size. Default is 8.
   * @param numLevels one-based number of current levels
   * @return level to compact
   */
  static int findLevelToCompact(final int k, final int m, final int numLevels, final int[] levels) {
    int level = 0;
    while (true) {
      assert level < numLevels;
      final int pop = levels[level + 1] - levels[level];
      final int cap = KllHelper.levelCapacity(k, numLevels, level, m);
      if (pop >= cap) {
        return level;
      }
      level++;
    }
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the <i>getNormalizedRankError(boolean)</i>.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @see KllDoublesSketch
   */
  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  static double getNormalizedRankError(final int k, final boolean pmf) {
    return pmf
        ? PMF_COEF / pow(k, PMF_EXP)
        : CDF_COEF / pow(k, CDF_EXP);
  }

  static int getNumRetainedAboveLevelZero(final int numLevels, final int[] levels) {
    return levels[numLevels] - levels[1];
  }

  /**
   * Returns the item capacity of a specific level.
   * @param k the accuracy parameter of the sketch. Because of the Java limits on array sizes,
   * the theoretical maximum value of k is 2^29. However, this implementation of the KLL sketch
   * limits k to 2^16 -1.
   * @param numLevels the number of current levels in the sketch. Maximum is 61.
   * @param level the zero-based index of a level. This varies from 0 to 60.
   * @param m the minimum level width. Default is 8.
   * @return the capacity of a specific level
   */
  static int levelCapacity(final int k, final int numLevels, final int level, final int m) {
    assert (k <= (1 << 29));
    assert (numLevels >= 1) && (numLevels <= 61);
    assert (level >= 0) && (level < numLevels);
    final int depth = numLevels - level - 1;
    return (int) Math.max(m, intCapAux(k, depth));
  }

  static long sumTheSampleWeights(final int num_levels, final int[] levels) {
    long total = 0;
    long weight = 1;
    for (int i = 0; i < num_levels; i++) {
      total += weight * (levels[i + 1] - levels[i]);
      weight *= 2;
    }
    return total;
  }

  /**
   * Returns very conservative upper bound of the number of levels based on <i>n</i>.
   * @param n the length of the stream
   * @return floor( log_2(n) )
   */
  static int ubOnNumLevels(final long n) {
    return 1 + Long.numberOfTrailingZeros(floorPowerOf2(n));
  }

  /**
   * Computes the actual item capacity of a given level given its depth index.
   * If the depth of levels exceeds 30, this uses a folding technique to accurately compute the
   * actual level capacity up to a depth of 60. Without folding, the internal calculations would
   * exceed the capacity of a long.
   * @param k the configured k of the sketch
   * @param depth the zero-based index of the level being computed.
   * @return the actual capacity of a given level given its depth index.
   */
  private static long intCapAux(final int k, final int depth) {
    if (depth <= 30) { return intCapAuxAux(k, depth); }
    final int half = depth / 2;
    final int rest = depth - half;
    final long tmp = intCapAuxAux(k, half);
    return intCapAuxAux(tmp, rest);
  }

  /**
   * Performs the integer based calculation of an individual level (or folded level).
   * @param k the configured k of the sketch
   * @param depth depth the zero-based index of the level being computed.
   * @return the actual capacity of a given level given its depth index.
   */
  private static long intCapAuxAux(final long k, final int depth) {
    final long twok = k << 1; // for rounding pre-multiply by 2
    final long tmp = ((twok << depth) / powersOfThree[depth]);
    final long result = ((tmp + 1L) >>> 1); // add 1 and divide by 2
    assert (result <= k);
    return result;
  }

  /**
   * @param fmt format
   * @param args arguments
   */
  private static void printf(final String fmt, final Object ... args) {
    System.out.printf(fmt, args); //Disable
  }

  /**
   * Println Object o
   * @param o object to print
   */
  private static void println(final Object o) {
    System.out.println(o.toString());
  }
}

