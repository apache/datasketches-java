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
import static org.apache.datasketches.kll.PreambleUtil.DATA_START_ADR_DOUBLE;
import static org.apache.datasketches.kll.PreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.PreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.PreambleUtil.MAX_K;
import static org.apache.datasketches.kll.PreambleUtil.MIN_K;

import org.apache.datasketches.SketchesArgumentException;

class KllHelper {
  static final String LS = System.getProperty("line.separator");

  /**
   * Copy the old array into a new larger array.
   * The extra space is at the top.
   * @param oldArr the given old array with data
   * @param newLen the new length larger than the oldArr.length.
   * @return the new array
   */
  static int[] growIntArray(final int[] oldArr, final int newLen) {
    final int oldLen = oldArr.length;
    assert newLen > oldLen;
    final int[] newArr = new int[newLen];
    System.arraycopy(oldArr, 0, newArr, 0, oldLen);
    return newArr;
  }

  /**
   * Returns very conservative upper bound of the number of levels based on <i>n</i>.
   * @param n the length of the stream
   * @return floor( log_2(n) )
   */
  static int ubOnNumLevels(final long n) {
    return 1 + Long.numberOfTrailingZeros(floorPowerOf2(n));
  }

  static LevelStats getAllLevelStatsGivenN(final int k, final int m, final long n,
      final boolean printDetail, final boolean printSummaries, final boolean isDouble) {
    long cumN;
    int numLevels = 0;
    LevelStats lvlStats;
    do {
      numLevels++;
      lvlStats = getLevelStats(k, m, numLevels, printDetail, printSummaries, isDouble);
      cumN = lvlStats.getMaxN();
    } while (cumN < n);
    return lvlStats;
  }

  static LevelStats getLevelStats(final int k, final int m, final int numLevels,
      final boolean printDetail, final boolean printSummary, final boolean isDouble) {
    int cumN = 0;
    int cumCap = 0;
    if (printDetail) {
      System.out.println("Total Levels: " + numLevels);
      System.out.printf("%6s%12s%8s%16s\n", "Level","Wt","Cap","N");
    }
    for (int level = 0; level < numLevels; level++) {
      final long levelCap = levelCapacity(k, numLevels, level, m);
      final long maxNAtLevel = levelCap << level;
      cumN += maxNAtLevel;
      cumCap += (int)levelCap;
      if (printDetail) {
        System.out.printf("%6d%,12d%8d%,16d\n", level, 1 << level, levelCap, maxNAtLevel);
      }
    }
    final int compactBytes = getCompactSerializedSizeBytes(numLevels, cumCap, isDouble);
    final int updatableBytes = getUpdatableSerializedSizeBytes(k, m, numLevels, isDouble);
    if (printDetail) {
      System.out.printf(" TOTALS%10s %8d%,16d\n", "", cumCap, cumN);
      System.out.println(" COMPACT BYTES: " + compactBytes);
      System.out.println(" UPDATABLE BYTES: " + updatableBytes);
      System.out.println("");
    }
    final LevelStats lvlStats = new LevelStats(cumN, numLevels, cumCap, compactBytes, updatableBytes);
    if (printSummary) { System.out.println(lvlStats.toString()); }
    return lvlStats;
  }

  static class LevelStats {
    private long maxN;
    private int compactBytes;
    private int updatableBytes;
    private int numLevels;
    private int maxCap;

    LevelStats(final long maxN, final int numLevels, final int maxCap, final int compactBytes,
        final int updatableBytes) {
      this.maxN = maxN;
      this.numLevels = numLevels;
      this.maxCap = maxCap;
      this.compactBytes = compactBytes;
      this.updatableBytes = updatableBytes;

    }

    @Override
    public String toString() {
      final String[] hdr = {"NumLevels", "MaxCap", "MaxN", "TotCompactBytes", "TotUpdatableBytes"};
      final StringBuilder sb = new StringBuilder();
      sb.append("Level Stats Summary:" + LS);
      sb.append(String.format("%10s %10s %14s %17s %17s" + LS, (Object[]) hdr));
      sb.append(String.format("%10d %10d %14d %17d %17d" + LS, numLevels, maxCap, maxN, compactBytes, updatableBytes));
      return sb.toString();
    }

    public long getMaxN() { return maxN; }

    public int getCompactBytes() { return compactBytes; }

    public int getNumLevels() { return numLevels; }

    public int getMaxCap() { return maxCap; }
  }

  static int getCompactSerializedSizeBytes(final int numLevels, final int numRetained, final boolean isDouble) {
    if (numLevels == 1 && numRetained == 1) {
      return DATA_START_ADR_SINGLE_ITEM + (isDouble ? Double.BYTES : Float.BYTES);
    }
    // The last integer in levels_ is not serialized because it can be derived.
    // The + 2 is for min and max
    if (isDouble) {
      return DATA_START_ADR_DOUBLE + numLevels * Integer.BYTES + (numRetained + 2) * Double.BYTES;
    } else {
      return DATA_START_ADR_FLOAT + numLevels * Integer.BYTES + (numRetained + 2) * Float.BYTES;
    }
  }

  static int getUpdatableSerializedSizeBytes(final int k, final int m, final int numLevels, final boolean isDouble) {
    //There are no special accommodations for empty or single item.
    //The last integer in levels IS serialized.
    // The + 2 is for min and max
    final int totCap = computeTotalItemCapacity(k, m, numLevels) + 2;
    if (isDouble) {
      return DATA_START_ADR_DOUBLE + (numLevels + 1) * Integer.BYTES + totCap * Double.BYTES;
    } else {
      return DATA_START_ADR_FLOAT + (numLevels + 1) * Integer.BYTES + totCap * Float.BYTES;
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
   * This is the exact powers of 3 from 3^0 to 3^30 where the exponent is the index
   */
  private static final long[] powersOfThree =
      new long[] {1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
  1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
  3486784401L, 10460353203L, 31381059609L, 94143178827L, 282429536481L,
  847288609443L, 2541865828329L, 7625597484987L, 22876792454961L, 68630377364883L,
  205891132094649L};

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
        ? 2.446 / pow(k, 0.9433)
        : 2.296 / pow(k, 0.9723);
  }

  /**
   * Checks the validity of the given value k
   * @param k must be greater than 7 and less than 65536.
   */
  static void checkK(final int k) {
    if (k < MIN_K || k > MAX_K) {
      throw new SketchesArgumentException(
          "K must be >= " + MIN_K + " and <= " + MAX_K + ": " + k);
    }
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

  static int currentLevelSize(final int level, final int numLevels, final int[] levels) {
    if (level >= numLevels) { return 0; }
    return levels[level + 1] - levels[level];
  }

  static int getNumRetained(final int numLevels, final int[] levels) {
    return levels[numLevels] - levels[0];
  }

  static int getNumRetainedAboveLevelZero(final int numLevels, final int[] levels) {
    return levels[numLevels] - levels[1];
  }


}

