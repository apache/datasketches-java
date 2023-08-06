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

import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static org.apache.datasketches.common.Family.KLL;
import static org.apache.datasketches.common.Util.floorPowerOf2;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.EMPTY_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.LEVEL_ZERO_SORTED_BIT_MASK;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.UNSUPPORTED_MSG;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.kll.KllSketch.SketchStructure;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class provides some useful sketch analysis tools that are used internally.
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("unchecked")
final class KllHelper {

  static class GrowthStats {
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

  static class LevelStats {
    long n;
    public int numLevels;
    int numItems;

    LevelStats(final long n, final int numLevels, final int numItems) {
      this.n = n;
      this.numLevels = numLevels;
      this.numItems = numItems;
    }
  }

  static final double EPS_DELTA_THRESHOLD = 1E-6;
  static final double MIN_EPS = 4.7634E-5;
  static final double PMF_COEF = 2.446;
  static final double PMF_EXP = 0.9433;
  static final double CDF_COEF = 2.296;
  static final double CDF_EXP = 0.9723;

  /**
   * This is the exact powers of 3 from 3^0 to 3^30 where the exponent is the index
   */
  private static long[] powersOfThree =
      new long[] {1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
  1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
  3486784401L, 10460353203L, 31381059609L, 94143178827L, 282429536481L,
  847288609443L, 2541865828329L, 7625597484987L, 22876792454961L, 68630377364883L,
  205891132094649L};

  /**
   * Checks the validity of the given k
   * @param k must be greater than 7 and less than 65536.
   */
  static void checkK(final int k, final int m) {
    if (k < m || k > KllSketch.MAX_K) {
      throw new SketchesArgumentException(
          "K must be >= " + m + " and <= " + KllSketch.MAX_K + ": " + k);
    }
  }

  static void checkM(final int m) {
    if (m < KllSketch.MIN_M || m > KllSketch.MAX_M || ((m & 1) == 1)) {
      throw new SketchesArgumentException(
          "M must be >= 2, <= 8 and even: " + m);
    }
  }

  /**
   * Returns the approximate maximum number of items that this sketch can handle
   * @param k The sizing / accuracy parameter of the sketch in items.
   * Note: this method actually works for k items up to k = 2^29 and 61 levels,
   * however only k items up to (2^16 - 1) are currently used by the sketch.
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
   * Convert the individual weights into cumulative weights.
   * An array of {1,1,1,1} becomes {1,2,3,4}
   * @param array of actual weights from the sketch, where first element is not zero.
   * @return total weight
   */
  public static long convertToCumulative(final long[] array) {
    long subtotal = 0;
    for (int i = 0; i < array.length; i++) {
      final long newSubtotal = subtotal + array[i];
      subtotal = array[i] = newSubtotal;
    }
    return subtotal;
  }

  static int currentLevelSizeItems(final int level, final int numLevels, final int[] levels) {
    if (level >= numLevels) { return 0; }
    return levels[level + 1] - levels[level];
  }

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
  static LevelStats getFinalSketchStatsAtNumLevels(
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
      cumItems += lvlStats.numItems;
      cumN += lvlStats.n;
      if (printSketchStructure) {
        printf("%6d %,8d %,12d %,18d %,18d\n", level, lvlStats.numItems, cumItems, lvlStats.n, cumN);
      }
    }
    return new LevelStats(cumN, numLevels, cumItems);
  }

  /**
   * This method is for direct Double and Float sketches only.
   * Given k, m, n, and the sketch type, this computes (and optionally prints) the growth scheme for a sketch as it
   * grows large enough to accommodate a stream length of n items.
   * @param k the given user configured sketch parameter
   * @param m the given user configured sketch parameter
   * @param n the desired stream length
   * @param sketchType the given sketch type: either DOUBLES_SKETCH or FLOATS_SKETCH.
   * @param printGrowthScheme if true the entire growth scheme of the sketch will be printed.
   * @return GrowthStats with the final numItems of the growth scheme
   */
  static GrowthStats getGrowthSchemeForGivenN(
      final int k,
      final int m,
      final long n,
      final SketchType sketchType,
      final boolean printGrowthScheme) {
    if (sketchType == ITEMS_SKETCH) { throw new SketchesArgumentException(UNSUPPORTED_MSG); }
    LevelStats lvlStats;
    final GrowthStats gStats = new GrowthStats();
    gStats.numLevels = 0;
    gStats.k = k;
    gStats.m = m;
    gStats.givenN = n;
    gStats.sketchType = sketchType;
    if (printGrowthScheme) {
      println("GROWTH SCHEME:");
      println("Given SketchType: " + gStats.sketchType.toString());
      println("Given K         : " + gStats.k);
      println("Given M         : " + gStats.m);
      println("Given N         : " + gStats.givenN);
      printf("%10s %10s %20s %13s %15s\n", "NumLevels", "MaxItems", "MaxN", "CompactBytes", "UpdatableBytes");
    }
    final int typeBytes = sketchType.getBytes();
    do {
      gStats.numLevels++; //
      lvlStats = getFinalSketchStatsAtNumLevels(gStats.k, gStats.m, gStats.numLevels, false);
      gStats.maxItems = lvlStats.numItems;
      gStats.maxN = lvlStats.n; //
      gStats.compactBytes =
          gStats.maxItems * typeBytes + gStats.numLevels * Integer.BYTES + 2 * typeBytes + DATA_START_ADR;
      gStats.updatableBytes = gStats.compactBytes + Integer.BYTES;
      if (printGrowthScheme) {
        printf("%10d %,10d %,20d %,13d %,15d\n",
            gStats.numLevels, gStats.maxItems, gStats.maxN, gStats.compactBytes, gStats.updatableBytes);
      }
    } while (lvlStats.n < n);

    //gStats.numLevels = lvlStats.numLevels; //
    //gStats.maxItems = lvlStats.numItems; //
    return gStats;
  }

  // constants were derived as the best fit to 99 percentile empirically measured max error in
  // thousands of trials
  static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    //Ensure that eps is >= than the lowest possible eps given MAX_K and pmf=false.
    final double eps = max(epsilon, MIN_EPS);
    final double kdbl = pmf
        ? exp(log(PMF_COEF / eps) / PMF_EXP)
        : exp(log(CDF_COEF / eps) / CDF_EXP);
    final double krnd = round(kdbl);
    final double del = abs(krnd - kdbl);
    final int k = (int) (del < EPS_DELTA_THRESHOLD ? krnd : ceil(kdbl));
    return max(KllSketch.MIN_M, min(KllSketch.MAX_K, k));
  }

  /**
   * Given k, m, numLevels, this computes the item capacity of a single level.
   * @param k the given user sketch configuration parameter
   * @param m the given user sketch configuration parameter
   * @param numLevels the given number of levels of the sketch
   * @param level the specific level to compute its item capacity
   * @return LevelStats with the computed N and items for the given level.
   */
  static LevelStats getLevelCapacityItems(
      final int k,
      final int m,
      final int numLevels,
      final int level) {
    final int items = KllHelper.levelCapacity(k, numLevels, level, m);
    final long n = (long)items << level;
    return new LevelStats(n, numLevels, items);
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the <i>getNormalizedRankError(boolean)</i>.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @see KllHeapDoublesSketch
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
   * the theoretical maximum k is 2^29. However, this implementation of the KLL sketch
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
   * This method is for direct Double and Float sketches only and does the following:
   * <ul>
   * <li>Determines if the required sketch bytes will fit in the current Memory.
   * If so, it will stretch the positioning of the arrays to fit. Otherwise:
   * <li>Allocates a new WritableMemory of the required size</li>
   * <li>Copies over the preamble as is (20 bytes)</li>
   * <li>The caller is responsible for filling the remainder and updating the preamble.</li>
   * </ul>
   *
   * @param sketch The current sketch that needs to be expanded.
   * @param newLevelsArrLen the element length of the new Levels array.
   * @param newItemsArrLen the element length of the new Items array.
   * @return the new expanded memory with preamble.
   */
  static WritableMemory memorySpaceMgmt(
      final KllSketch sketch,
      final int newLevelsArrLen,
      final int newItemsArrLen) {
    final KllSketch.SketchType sketchType = sketch.sketchType;
    if (sketchType == ITEMS_SKETCH) { throw new SketchesArgumentException(UNSUPPORTED_MSG); }
    final WritableMemory wmem = sketch.getWritableMemory();
    if (wmem == null) { return null; }
    final WritableMemory oldWmem = wmem;
    final int typeBytes = sketchType.getBytes();
    final int requiredSketchBytes =  DATA_START_ADR
      + newLevelsArrLen * Integer.BYTES
      + 2 * typeBytes
      + newItemsArrLen * typeBytes;
    final WritableMemory newWmem;

    if (requiredSketchBytes > oldWmem.getCapacity()) { //Acquire new WritableMemory
      newWmem = sketch.getMemoryRequestServer().request(oldWmem, requiredSketchBytes);
      oldWmem.copyTo(0, newWmem, 0, DATA_START_ADR); //copy preamble (first 20 bytes)
    }
    else { //Expand or contract in current memory
      newWmem = oldWmem;
    }
    assert requiredSketchBytes <= newWmem.getCapacity();
    return newWmem;
  }

  private static <T> String outputItemsData(final int numLevels, final int[] levelsArr, final Object[] itemsArr,
      final ArrayOfItemsSerDe<T> serDe) {
    final StringBuilder sb =  new StringBuilder();
    sb.append("### KLL items data {index, item}:").append(Util.LS);
    if (levelsArr[0] > 0) {
      sb.append(" Empty/Garbage:" + Util.LS);
      for (int i = 0; i < levelsArr[0]; i++) {
        sb.append("   ").append(i + ", ").append(serDe.toString((T)itemsArr[i])).append(Util.LS);
      }
    }
    int level = 0;
    while (level < numLevels) {
      final int fromIndex = levelsArr[level];
      final int toIndex = levelsArr[level + 1]; // exclusive
      if (fromIndex < toIndex) {
        sb.append(" level[").append(level).append("]: offset: " + levelsArr[level] + " wt: " + (1 << level));
        sb.append(Util.LS);
      }

      for (int i = fromIndex; i < toIndex; i++) {
        sb.append("   ").append(i + ", ").append(serDe.toString((T)itemsArr[i])).append(Util.LS);
      }
      level++;
    }
    sb.append(" level[" + level + "]: offset: " + levelsArr[level] + " (Exclusive)");
    sb.append(Util.LS);
    sb.append("### End items data").append(Util.LS);
    return sb.toString();

  }

  private static String outputDoublesData(final int numLevels, final int[] levelsArr, final double[] doubleItemsArr) {
    final StringBuilder sb =  new StringBuilder();
    sb.append("### KLL items data {index, item}:").append(Util.LS);
    if (levelsArr[0] > 0) {
      sb.append(" Empty/Garbage:" + Util.LS);
      for (int i = 0; i < levelsArr[0]; i++) {
        sb.append("   ").append(i + ", ").append(doubleItemsArr[i]).append(Util.LS);
      }
    }
    int level = 0;
    while (level < numLevels) {
      final int fromIndex = levelsArr[level];
      final int toIndex = levelsArr[level + 1]; // exclusive
      if (fromIndex < toIndex) {
        sb.append(" level[").append(level).append("]: offset: " + levelsArr[level] + " wt: " + (1 << level));
        sb.append(Util.LS);
      }

      for (int i = fromIndex; i < toIndex; i++) {
        sb.append("   ").append(i + ", ").append(doubleItemsArr[i]).append(Util.LS);
      }
      level++;
    }
    sb.append(" level[" + level + "]: offset: " + levelsArr[level] + " (Exclusive)");
    sb.append(Util.LS);
    sb.append("### End items data").append(Util.LS);
    return sb.toString();
  }

  private static String outputFloatsData(final int numLevels, final int[] levelsArr, final float[] floatsItemsArr) {
    final StringBuilder sb =  new StringBuilder();
    sb.append("### KLL items data {index, item}:").append(Util.LS);
    if (levelsArr[0] > 0) {
      sb.append(" Empty/Garbage:" + Util.LS);
      for (int i = 0; i < levelsArr[0]; i++) {
        sb.append("   ").append(i + ", ").append(floatsItemsArr[i]).append(Util.LS);
      }
    }
    int level = 0;
    while (level < numLevels) {
      final int fromIndex = levelsArr[level];
      final int toIndex = levelsArr[level + 1]; // exclusive
      if (fromIndex < toIndex) {
        sb.append(" level[").append(level).append("]: offset: " + levelsArr[level] + " wt: " + (1 << level));
        sb.append(Util.LS);
      }

      for (int i = fromIndex; i < toIndex; i++) {
        sb.append("   ").append(i + ", ").append(floatsItemsArr[i]).append(Util.LS);
      }
      level++;
    }
    sb.append(" level[" + level + "]: offset: " + levelsArr[level] + " (Exclusive)");
    sb.append(Util.LS);
    sb.append("### End items data").append(Util.LS);
    return sb.toString();
  }

  static String outputLevels(final int k, final int m, final int numLevels, final int[] levelsArr) {
    final StringBuilder sb =  new StringBuilder();
    sb.append("### KLL levels array:").append(Util.LS)
    .append(" level, offset: nominal capacity, actual size").append(Util.LS);
    int level = 0;
    for ( ; level < numLevels; level++) {
      sb.append("   ").append(level).append(", ").append(levelsArr[level]).append(": ")
      .append(KllHelper.levelCapacity(k, numLevels, level, m))
      .append(", ").append(KllHelper.currentLevelSizeItems(level, numLevels, levelsArr)).append(Util.LS);
    }
    sb.append("   ").append(level).append(", ").append(levelsArr[level]).append(": (Exclusive)")
    .append(Util.LS);
    sb.append("### End levels array").append(Util.LS);
    return sb.toString();
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

  static  byte[] toByteArray(final KllSketch srcSk, final boolean updatable) {
    //ITEMS_SKETCH byte array is never updatable
    final boolean myUpdatable = srcSk.sketchType == ITEMS_SKETCH ? false : updatable;
    final long srcN = srcSk.getN();
    final SketchStructure tgtStructure;
    if (myUpdatable) { tgtStructure = UPDATABLE; }
    else if (srcN == 0) { tgtStructure = COMPACT_EMPTY; }
    else if (srcN == 1) { tgtStructure = COMPACT_SINGLE; }
    else { tgtStructure = COMPACT_FULL; }
    final int totalBytes = srcSk.currentSerializedSizeBytes(myUpdatable);
    final byte[] bytesOut = new byte[totalBytes];
    final WritableBuffer wbuf = WritableMemory.writableWrap(bytesOut).asWritableBuffer(ByteOrder.LITTLE_ENDIAN);

    //ints 0,1
    final byte preInts = (byte)tgtStructure.getPreInts();
    final byte serVer = (byte)tgtStructure.getSerVer();
    final byte famId = (byte)(KLL.getID());
    final byte flags = (byte) ((srcSk.isEmpty() ? EMPTY_BIT_MASK : 0)
        | (srcSk.isLevelZeroSorted() ? LEVEL_ZERO_SORTED_BIT_MASK : 0));
    final short k = (short) srcSk.getK();
    final byte m = (byte) srcSk.getM();

    //load first 8 bytes
    wbuf.putByte(preInts);
    wbuf.putByte(serVer);
    wbuf.putByte(famId);
    wbuf.putByte(flags);
    wbuf.putShort(k);
    wbuf.putByte(m);
    wbuf.incrementPosition(1);

    if (tgtStructure == COMPACT_EMPTY) {
      return bytesOut;
    }

    if (tgtStructure == COMPACT_SINGLE) {
      final byte[] siByteArr = srcSk.getSingleItemByteArr();
      final int len = siByteArr.length;
      wbuf.putByteArray(siByteArr, 0, len);
      wbuf.incrementPosition(-len);
      return bytesOut;
    }

    // Tgt is either COMPACT_FULL or UPDATABLE
    //ints 2,3
    final long n = srcSk.getN();
    //ints 4
    final short minK = (short) srcSk.getMinK();
    final byte numLevels = (byte) srcSk.getNumLevels();
    //end of full preamble
    final int[] lvlsArr = srcSk.getLevelsArray(tgtStructure);
    final byte[] minMaxByteArr = srcSk.getMinMaxByteArr();
    final byte[] itemsByteArr = tgtStructure == COMPACT_FULL
        ? srcSk.getRetainedItemsByteArr()
        : srcSk.getTotalItemsByteArr();

    wbuf.putLong(n);
    wbuf.putShort(minK);
    wbuf.putByte(numLevels);
    wbuf.incrementPosition(1);
    wbuf.putIntArray(lvlsArr, 0, lvlsArr.length);
    wbuf.putByteArray(minMaxByteArr, 0, minMaxByteArr.length);
    wbuf.putByteArray(itemsByteArr, 0, itemsByteArr.length);
    return bytesOut;
  }

  static <T> String toStringImpl(final KllSketch sketch, final boolean withLevels, final boolean withData,
      final ArrayOfItemsSerDe<T> serDe) {
    final SketchType sketchType = sketch.sketchType;
    final boolean hasMemory = sketch.hasMemory();
    final int k = sketch.getK();
    final int m = sketch.getM();
    final long n = sketch.getN();
    final int numLevels = sketch.getNumLevels();
    final int[] fullLevelsArr = sketch.getLevelsArray(UPDATABLE);
    //final int[] levelsArr = sketch.getLevelsArray(sketch.sketchStructure);
    final String epsPct = String.format("%.3f%%", sketch.getNormalizedRankError(false) * 100);
    final String epsPMFPct = String.format("%.3f%%", sketch.getNormalizedRankError(true) * 100);
    final boolean compact = sketch.isCompactMemoryFormat();

    final StringBuilder sb = new StringBuilder();
    final String directStr = hasMemory ? "Direct" : "";
    final String compactStr = compact ? "Compact" : "";
    final String readOnlyStr = sketch.isReadOnly() ? "true" + ("(" + (compact ? "Format" : "Memory") + ")") : "false";
    final String skTypeStr = sketchType.getName();
    final String className = "Kll" + directStr + compactStr + skTypeStr;

    sb.append(Util.LS).append("### ").append(className).append(" Summary:").append(Util.LS);
    sb.append("   K                      : ").append(k).append(Util.LS);
    sb.append("   Dynamic min K          : ").append(sketch.getMinK()).append(Util.LS);
    sb.append("   M                      : ").append(m).append(Util.LS);
    sb.append("   N                      : ").append(n).append(Util.LS);
    sb.append("   Epsilon                : ").append(epsPct).append(Util.LS);
    sb.append("   Epsilon PMF            : ").append(epsPMFPct).append(Util.LS);
    sb.append("   Empty                  : ").append(sketch.isEmpty()).append(Util.LS);
    sb.append("   Estimation Mode        : ").append(sketch.isEstimationMode()).append(Util.LS);
    sb.append("   Levels                 : ").append(numLevels).append(Util.LS);
    sb.append("   Level 0 Sorted         : ").append(sketch.isLevelZeroSorted()).append(Util.LS);
    sb.append("   Capacity Items         : ").append(fullLevelsArr[numLevels]).append(Util.LS);
    sb.append("   Retained Items         : ").append(sketch.getNumRetained()).append(Util.LS);
    sb.append("   Empty/Garbage Items    : ").append(sketch.levelsArr[0]).append(Util.LS);
    sb.append("   ReadOnly               : ").append(readOnlyStr).append(Util.LS);
    if (sketchType != ITEMS_SKETCH) {
      sb.append("   Updatable Storage Bytes: ").append(sketch.currentSerializedSizeBytes(true))
        .append(Util.LS);
    }
    sb.append("   Compact Storage Bytes  : ").append(sketch.currentSerializedSizeBytes(false))
        .append(Util.LS);

    if (sketchType == DOUBLES_SKETCH) {
      final KllDoublesSketch dblSk = (KllDoublesSketch) sketch;
      sb.append("   Min Item               : ").append(dblSk.isEmpty() ? Double.NaN : dblSk.getMinItem())
          .append(Util.LS);
      sb.append("   Max Item               : ").append(dblSk.isEmpty() ? Double.NaN : dblSk.getMaxItem())
          .append(Util.LS);
    }
    else if (sketchType == FLOATS_SKETCH) {
      final KllFloatsSketch fltSk = (KllFloatsSketch) sketch;
      sb.append("   Min Item               : ").append(fltSk.isEmpty() ? Float.NaN : fltSk.getMinItem())
          .append(Util.LS);
      sb.append("   Max Item               : ").append(fltSk.isEmpty() ? Float.NaN : fltSk.getMaxItem())
          .append(Util.LS);
    }
    else { //sketchType == ITEMS_SKETCH
      final KllItemsSketch<T> itmSk = (KllItemsSketch<T>) sketch;
      sb.append("   Min Item               : ").append(itmSk.isEmpty() ? "null" : serDe.toString(itmSk.getMinItem()))
          .append(Util.LS);
      sb.append("   Max Item               : ").append(itmSk.isEmpty() ? "null" : serDe.toString(itmSk.getMaxItem()))
          .append(Util.LS);
    }
    sb.append("### End sketch summary").append(Util.LS);

    if (withLevels) {
      sb.append(outputLevels(k, m, numLevels, fullLevelsArr));
    }
    if (withData) {
      if (sketchType == DOUBLES_SKETCH) {
        final KllDoublesSketch dblSk = (KllDoublesSketch) sketch;
        final double[] myDoubleItemsArr = dblSk.getDoubleItemsArray();
        sb.append(outputDoublesData(numLevels, fullLevelsArr, myDoubleItemsArr));
      } else if (sketchType == FLOATS_SKETCH) {
        final KllFloatsSketch fltSk = (KllFloatsSketch) sketch;
        final float[] myFloatItemsArr = fltSk.getFloatItemsArray();
        sb.append(outputFloatsData(numLevels, fullLevelsArr, myFloatItemsArr));
      }
      else { //sketchType == KllItemsSketch
        final KllItemsSketch<T> itmSk = (KllItemsSketch<T>) sketch;
        final T[] myItemsArr = (T[]) itmSk.getTotalItemsArray();
        sb.append(outputItemsData(numLevels, fullLevelsArr, myItemsArr, serDe));
      }
    }
    return sb.toString();
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
   * This grows the levels arr by 1 (if needed) and increases the capacity of the items array
   * at the bottom.  Only numLevels, the levels array and the items array are affected.
   * This assumes sketch is writable and UPDATABLE.
   * @param sketch the current sketch
   */
  static void addEmptyTopLevelToCompletelyFullSketch(final KllSketch sketch) {
    final SketchType sketchType = sketch.sketchType;

    final int[] myCurLevelsArr = sketch.getLevelsArray(sketch.sketchStructure);
    final int myCurNumLevels = sketch.getNumLevels();
    final int myCurTotalItemsCapacity = myCurLevelsArr[myCurNumLevels];

    final int myNewNumLevels;
    final int[] myNewLevelsArr;
    final int myNewTotalItemsCapacity;


    double[] myCurDoubleItemsArr = null;
    double[] myNewDoubleItemsArr = null;
    double minDouble = Double.NaN;
    double maxDouble = Double.NaN;

    float[] myCurFloatItemsArr = null;
    float[] myNewFloatItemsArr = null;
    float minFloat = Float.NaN;
    float maxFloat = Float.NaN;

    Object[] myCurItemsArr = null;
    Object[] myNewItemsArr = null;
    Object minItem = null;
    Object maxItem = null;

    if (sketchType == DOUBLES_SKETCH) {
      final KllDoublesSketch dblSk = (KllDoublesSketch) sketch;
      myCurDoubleItemsArr = dblSk.getDoubleItemsArray();
      minDouble = dblSk.getMinItem();
      maxDouble = dblSk.getMaxItem();
      //assert we are following a certain growth scheme
      assert myCurDoubleItemsArr.length == myCurTotalItemsCapacity;
    }
    else if (sketchType == FLOATS_SKETCH) {
      final KllFloatsSketch fltSk = (KllFloatsSketch) sketch;
      myCurFloatItemsArr = fltSk.getFloatItemsArray();
      minFloat = fltSk.getMinItem();
      maxFloat = fltSk.getMaxItem();
      //assert we are following a certain growth scheme
      assert myCurFloatItemsArr.length == myCurTotalItemsCapacity;
    }
    else { //sketchType == ITEMS_SKETCH
      final KllItemsSketch<?> itmSk = (KllItemsSketch<?>) sketch;
      myCurItemsArr = itmSk.getTotalItemsArray();
      minItem = itmSk.getMinItem();
      maxItem = itmSk.getMaxItem();
    }
    assert myCurLevelsArr[0] == 0; //definition of full is part of the growth scheme

    final int deltaItemsCap = levelCapacity(sketch.getK(), myCurNumLevels + 1, 0, sketch.getM());
    myNewTotalItemsCapacity = myCurTotalItemsCapacity + deltaItemsCap;

    // Check if growing the levels arr if required.
    // Note that merging MIGHT over-grow levels_, in which case we might not have to grow it
    final boolean growLevelsArr = myCurLevelsArr.length < myCurNumLevels + 2;

    // GROW LEVELS ARRAY
    if (growLevelsArr) {
      //grow levels arr by one and copy the old data to the new array, extra space at the top.
      myNewLevelsArr = Arrays.copyOf(myCurLevelsArr, myCurNumLevels + 2);
      assert myNewLevelsArr.length == myCurLevelsArr.length + 1;
      myNewNumLevels = myCurNumLevels + 1;
      sketch.incNumLevels(); //increment for off-heap
    } else {
      myNewLevelsArr = myCurLevelsArr;
      myNewNumLevels = myCurNumLevels;
    }
    // This loop updates all level indices EXCLUDING the "extra" index at the top
    for (int level = 0; level <= myNewNumLevels - 1; level++) {
      myNewLevelsArr[level] += deltaItemsCap;
    }
    myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCapacity; // initialize the new "extra" index at the top

    // GROW items ARRAY
    if (sketchType == DOUBLES_SKETCH) {
      myNewDoubleItemsArr = new double[myNewTotalItemsCapacity];
      // copy and shift the current data into the new array
      System.arraycopy(myCurDoubleItemsArr, 0, myNewDoubleItemsArr, deltaItemsCap, myCurTotalItemsCapacity);
    }
    else if (sketchType == FLOATS_SKETCH) {
      myNewFloatItemsArr = new float[myNewTotalItemsCapacity];
      // copy and shift the current items data into the new array
      System.arraycopy(myCurFloatItemsArr, 0, myNewFloatItemsArr, deltaItemsCap, myCurTotalItemsCapacity);
    }
    else { //sketchType == ITEMS_SKETCH
      myNewItemsArr = new Object[myNewTotalItemsCapacity];
      // copy and shift the current items data into the new array
      System.arraycopy(myCurItemsArr, 0, myNewItemsArr, deltaItemsCap, myCurTotalItemsCapacity);
    }

    //MEMORY SPACE MANAGEMENT
    if (sketch.getWritableMemory() != null) {
      final WritableMemory wmem = memorySpaceMgmt(sketch, myNewLevelsArr.length, myNewTotalItemsCapacity);
      sketch.setWritablMemory(wmem);
    }

    //update our sketch with new expanded spaces
    sketch.setNumLevels(myNewNumLevels);   //for off-heap only
    sketch.setLevelsArray(myNewLevelsArr); //the KllSketch copy
    if (sketchType == DOUBLES_SKETCH) {
      final KllDoublesSketch dblSk = (KllDoublesSketch) sketch;
      dblSk.setMinItem(minDouble);
      dblSk.setMaxItem(maxDouble);
      dblSk.setDoubleItemsArray(myNewDoubleItemsArr);
    }
    else if (sketchType == FLOATS_SKETCH) {
      final KllFloatsSketch fltSk = (KllFloatsSketch) sketch;
      fltSk.setMinItem(minFloat);
      fltSk.setMaxItem(maxFloat);
      fltSk.setFloatItemsArray(myNewFloatItemsArr);
    }
    else { //sketchType == ITEMS_SKETCH
      final KllItemsSketch<?> itmSk = (KllItemsSketch<?>) sketch;
      itmSk.setMinItem(minItem);
      itmSk.setMaxItem(maxItem);
      itmSk.setItemsArray(myNewItemsArr);
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
    //System.out.printf(fmt, args); //Disable
  }

  /**
   * Println Object o
   * @param o object to print
   */
  private static void println(final Object o) {
    //System.out.println(o.toString()); //Disable
  }

}
