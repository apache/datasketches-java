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
import static org.apache.datasketches.Util.floorPowerOf2;
import static org.apache.datasketches.Util.isOdd;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.DOUBLES_SKETCH_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.EMPTY_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.FLAGS_BYTE_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.KLL_FAMILY;
import static org.apache.datasketches.kll.KllPreambleUtil.K_SHORT_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SINGLE_ITEM_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.UPDATABLE_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryDoubleSketchFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySerVer;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySingleItemFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryUpdatableFlag;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;

import java.util.Arrays;

import org.apache.datasketches.ByteArrayUtil;
import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class provides some useful sketch analysis tools that are used internally.
 *
 * @author lrhodes
 *
 */
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
    int numLevels;
    int items;

    LevelStats(final long n, final int numLevels, final int items) {
      this.n = n;
      this.numLevels = numLevels;
      this.items = items;
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
   * Checks the validity of the given value k
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
   * The following code is only valid in the special case of exactly reaching capacity while updating.
   * It cannot be used while merging, while reducing k, or anything else.
   * @param mine the current sketch
   */
  static void compressWhileUpdatingSketch(final KllSketch mine) {
    final int level =
        findLevelToCompact(mine.getK(), mine.getM(), mine.getNumLevels(), mine.getLevelsArray());
    if (level == mine.getNumLevels() - 1) {
      //The level to compact is the top level, thus we need to add a level.
      //Be aware that this operation grows the items array,
      //shifts the items data and the level boundaries of the data,
      //and grows the levels array and increments numLevels_.
      KllHelper.addEmptyTopLevelToCompletelyFullSketch(mine);
    }
    //after this point, the levelsArray will not be expanded, only modified.
    final int[] myLevelsArr = mine.getLevelsArray();
    final int rawBeg = myLevelsArr[level];
    final int rawEnd = myLevelsArr[level + 1];
    // +2 is OK because we already added a new top level if necessary
    final int popAbove = myLevelsArr[level + 2] - rawEnd;
    final int rawPop = rawEnd - rawBeg;
    final boolean oddPop = isOdd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    if (mine.sketchType == DOUBLES_SKETCH) {
      final double[] myDoubleItemsArr = mine.getDoubleItemsArray();
      if (level == 0) { // level zero might not be sorted, so we must sort it if we wish to compact it
        Arrays.sort(myDoubleItemsArr, adjBeg, adjBeg + adjPop);
      }
      if (popAbove == 0) {
        KllDoublesHelper.randomlyHalveUpDoubles(myDoubleItemsArr, adjBeg, adjPop, KllSketch.random);
      } else {
        KllDoublesHelper.randomlyHalveDownDoubles(myDoubleItemsArr, adjBeg, adjPop, KllSketch.random);
        KllDoublesHelper.mergeSortedDoubleArrays(
            myDoubleItemsArr, adjBeg, halfAdjPop,
            myDoubleItemsArr, rawEnd, popAbove,
            myDoubleItemsArr, adjBeg + halfAdjPop);
      }

      int newIndex = myLevelsArr[level + 1] - halfAdjPop;  // adjust boundaries of the level above
      mine.setLevelsArrayAt(level + 1, newIndex);

      if (oddPop) {
        mine.setLevelsArrayAt(level, myLevelsArr[level + 1] - 1); // the current level now contains one item
        myDoubleItemsArr[myLevelsArr[level]] = myDoubleItemsArr[rawBeg];  // namely this leftover guy
      } else {
        mine.setLevelsArrayAt(level, myLevelsArr[level + 1]); // the current level is now empty
      }

      // verify that we freed up halfAdjPop array slots just below the current level
      assert myLevelsArr[level] == rawBeg + halfAdjPop;

   // finally, we need to shift up the data in the levels below
      // so that the freed-up space can be used by level zero
      if (level > 0) {
        final int amount = rawBeg - myLevelsArr[0];
        System.arraycopy(myDoubleItemsArr, myLevelsArr[0], myDoubleItemsArr, myLevelsArr[0] + halfAdjPop, amount);
      }
      for (int lvl = 0; lvl < level; lvl++) {
        newIndex = myLevelsArr[lvl] + halfAdjPop; //adjust boundary
        mine.setLevelsArrayAt(lvl, newIndex);
      }
      mine.setDoubleItemsArray(myDoubleItemsArr);
    }
    else { //Float sketch
      final float[] myFloatItemsArr = mine.getFloatItemsArray();
      if (level == 0) { // level zero might not be sorted, so we must sort it if we wish to compact it
        Arrays.sort(myFloatItemsArr, adjBeg, adjBeg + adjPop);
      }
      if (popAbove == 0) {
        KllFloatsHelper.randomlyHalveUpFloats(myFloatItemsArr, adjBeg, adjPop, KllSketch.random);
      } else {
        KllFloatsHelper.randomlyHalveDownFloats(myFloatItemsArr, adjBeg, adjPop, KllSketch.random);
        KllFloatsHelper.mergeSortedFloatArrays(
            myFloatItemsArr, adjBeg, halfAdjPop,
            myFloatItemsArr, rawEnd, popAbove,
            myFloatItemsArr, adjBeg + halfAdjPop);
      }

      int newIndex = myLevelsArr[level + 1] - halfAdjPop;  // adjust boundaries of the level above
      mine.setLevelsArrayAt(level + 1, newIndex);

      if (oddPop) {
        mine.setLevelsArrayAt(level, myLevelsArr[level + 1] - 1); // the current level now contains one item
        myFloatItemsArr[myLevelsArr[level]] = myFloatItemsArr[rawBeg];  // namely this leftover guy
      } else {
        mine.setLevelsArrayAt(level, myLevelsArr[level + 1]); // the current level is now empty
      }

      // verify that we freed up halfAdjPop array slots just below the current level
      assert myLevelsArr[level] == rawBeg + halfAdjPop;

      // finally, we need to shift up the data in the levels below
      // so that the freed-up space can be used by level zero
      if (level > 0) {
        final int amount = rawBeg - myLevelsArr[0];
        System.arraycopy(myFloatItemsArr, myLevelsArr[0], myFloatItemsArr, myLevelsArr[0] + halfAdjPop, amount);
      }
      for (int lvl = 0; lvl < level; lvl++) {
        newIndex = myLevelsArr[lvl] + halfAdjPop; //adjust boundary
        mine.setLevelsArrayAt(lvl, newIndex);
      }
      mine.setFloatItemsArray(myFloatItemsArr);
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
  static GrowthStats getGrowthSchemeForGivenN(
      final int k,
      final int m,
      final long n,
      final SketchType sketchType,
      final boolean printGrowthScheme) {

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
    final int typeBytes = (sketchType == DOUBLES_SKETCH) ? Double.BYTES : Float.BYTES;
    do {
      gStats.numLevels++; //
      lvlStats = getFinalSketchStatsAtNumLevels(gStats.k, gStats.m, gStats.numLevels, false);
      gStats.maxItems = lvlStats.items; //
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
    //gStats.maxItems = lvlStats.items; //
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
    final WritableMemory oldWmem = sketch.wmem;
    final int typeBytes = (sketchType == DOUBLES_SKETCH) ? Double.BYTES : Float.BYTES;

    final int requiredSketchBytes =  DATA_START_ADR
      + newLevelsArrLen * Integer.BYTES
      + 2 * typeBytes
      + newItemsArrLen * typeBytes;
    final WritableMemory newWmem;

    if (requiredSketchBytes > oldWmem.getCapacity()) { //Acquire new WritableMemory
      newWmem = sketch.memReqSvr.request(oldWmem, requiredSketchBytes);
      oldWmem.copyTo(0, newWmem, 0, DATA_START_ADR); //copy preamble (first 20 bytes)
    }
    else { //Expand or contract in current memory
      newWmem = oldWmem;
    }
    assert requiredSketchBytes <= newWmem.getCapacity();
    return newWmem;
  }

  static String outputData(final boolean doubleType, final int numLevels, final int[] levelsArr,
      final float[] floatItemsArr, final double[] doubleItemsArr) {
    final StringBuilder sb =  new StringBuilder();
    sb.append("### KLL items data {index, item}:").append(Util.LS);
    if (levelsArr[0] > 0) {
      sb.append(" Garbage:" + Util.LS);
      if (doubleType) {
        for (int i = 0; i < levelsArr[0]; i++) {
          sb.append("   ").append(i + ", ").append(doubleItemsArr[i]).append(Util.LS);
        }
      } else {
        for (int i = 0; i < levelsArr[0]; i++) {
          sb.append("   ").append(i + ", ").append(floatItemsArr[i]).append(Util.LS);
        }
      }
    }
    int level = 0;
    if (doubleType) {
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
    }
    else {
      while (level < numLevels) {
        final int fromIndex = levelsArr[level];
        final int toIndex = levelsArr[level + 1]; // exclusive
        if (fromIndex <= toIndex) {
          sb.append(" level[").append(level).append("]: offset: " + levelsArr[level] + " wt: " + (1 << level));
          sb.append(Util.LS);
        }

        for (int i = fromIndex; i < toIndex; i++) {
          sb.append("   ").append(i + ", ").append(floatItemsArr[i]).append(Util.LS);
        }
        level++;
      }
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
      .append(", ").append(KllHelper.currentLevelSize(level, numLevels, levelsArr)).append(Util.LS);
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

  static byte[] toCompactByteArrayImpl(final KllSketch mine) {
    if (mine.isEmpty()) { return fastEmptyCompactByteArray(mine); }
    if (mine.isSingleItem()) { return fastSingleItemCompactByteArray(mine); }
    final byte[] byteArr = new byte[mine.getCurrentCompactSerializedSizeBytes()];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    loadFirst8Bytes(mine, wmem, false);
    if (mine.getN() == 0) { return byteArr; } //empty
    final boolean doubleType = (mine.sketchType == DOUBLES_SKETCH);

    //load data
    int offset = DATA_START_ADR_SINGLE_ITEM;
    final int[] myLevelsArr = mine.getLevelsArray();
    if (mine.getN() == 1) { //single item
      if (doubleType) {
        wmem.putDouble(offset,  mine.getDoubleItemsArray()[myLevelsArr[0]]);
      } else {
        wmem.putFloat(offset, mine.getFloatItemsArray()[myLevelsArr[0]]);
      }
    } else { // n > 1
      //remainder of preamble after first 8 bytes
      setMemoryN(wmem, mine.getN());
      setMemoryMinK(wmem, mine.getMinK());
      setMemoryNumLevels(wmem, mine.getNumLevels());
      offset = DATA_START_ADR;

      //LOAD LEVELS ARR the last integer in levels_ is NOT serialized
      final int len = myLevelsArr.length - 1;
      wmem.putIntArray(offset, myLevelsArr, 0, len);
      offset += len * Integer.BYTES;

      //LOAD MIN, MAX VALUES FOLLOWED BY ITEMS ARRAY
      if (doubleType) {
        wmem.putDouble(offset,mine. getMinDoubleValue());
        offset += Double.BYTES;
        wmem.putDouble(offset, mine.getMaxDoubleValue());
        offset += Double.BYTES;
        wmem.putDoubleArray(offset, mine.getDoubleItemsArray(), myLevelsArr[0], mine.getNumRetained());
      } else {
        wmem.putFloat(offset, mine.getMinFloatValue());
        offset += Float.BYTES;
        wmem.putFloat(offset, mine.getMaxFloatValue());
        offset += Float.BYTES;
        wmem.putFloatArray(offset, mine.getFloatItemsArray(), myLevelsArr[0], mine.getNumRetained());
      }
    }
    return byteArr;
  }

  static byte[] fastEmptyCompactByteArray(final KllSketch mine) {
    final int doubleFlagBit = (mine.sketchType == DOUBLES_SKETCH) ? DOUBLES_SKETCH_BIT_MASK : 0;
    final byte[] byteArr = new byte[8];
    byteArr[0] = PREAMBLE_INTS_EMPTY_SINGLE; //2
    byteArr[1] = SERIAL_VERSION_EMPTY_FULL;  //1
    byteArr[2] = KLL_FAMILY; //15
    byteArr[3] = (byte) (EMPTY_BIT_MASK | doubleFlagBit);
    ByteArrayUtil.putShortLE(byteArr, K_SHORT_ADR, (short)mine.getK());
    byteArr[6] = (byte)mine.getM();
    return byteArr;
  }

  static byte[] fastSingleItemCompactByteArray(final KllSketch mine) {
    final boolean doubleSketch = mine.sketchType == DOUBLES_SKETCH;
    final int doubleFlagBit = doubleSketch ? DOUBLES_SKETCH_BIT_MASK : 0;
    final byte[] byteArr = new byte[8 + (doubleSketch ? Double.BYTES : Float.BYTES)];
    byteArr[0] = PREAMBLE_INTS_EMPTY_SINGLE; //2
    byteArr[1] = SERIAL_VERSION_SINGLE;      //2
    byteArr[2] = KLL_FAMILY; //15
    byteArr[3] = (byte) (SINGLE_ITEM_BIT_MASK | doubleFlagBit);
    ByteArrayUtil.putShortLE(byteArr, K_SHORT_ADR, (short)mine.getK());
    byteArr[6] = (byte)mine.getM();
    if (doubleSketch) {
      ByteArrayUtil.putDoubleLE(byteArr, DATA_START_ADR_SINGLE_ITEM, mine.getDoubleSingleItem());
    } else {
      ByteArrayUtil.putFloatLE(byteArr, DATA_START_ADR_SINGLE_ITEM, mine.getFloatSingleItem());
    }
    return byteArr;
  }

  static String toStringImpl(final KllSketch mine, final boolean withLevels, final boolean withData) {
    final boolean doubleType = (mine.sketchType == DOUBLES_SKETCH);
    final int k = mine.getK();
    final int m = mine.getM();
    final int numLevels = mine.getNumLevels();
    final int[] levelsArr = mine.getLevelsArray();
    final String epsPct = String.format("%.3f%%", mine.getNormalizedRankError(false) * 100);
    final String epsPMFPct = String.format("%.3f%%", mine.getNormalizedRankError(true) * 100);
    final StringBuilder sb = new StringBuilder();
    final String skType = (mine.updatableMemFormat ? "Direct" : "") + (doubleType ? "Doubles" : "Floats");
    sb.append(Util.LS).append("### Kll").append(skType).append("Sketch Summary:").append(Util.LS);
    sb.append("   K                      : ").append(k).append(Util.LS);
    sb.append("   Dynamic min K          : ").append(mine.getMinK()).append(Util.LS);
    sb.append("   M                      : ").append(m).append(Util.LS);
    sb.append("   N                      : ").append(mine.getN()).append(Util.LS);
    sb.append("   Epsilon                : ").append(epsPct).append(Util.LS);
    sb.append("   Epsison PMF            : ").append(epsPMFPct).append(Util.LS);
    sb.append("   Empty                  : ").append(mine.isEmpty()).append(Util.LS);
    sb.append("   Estimation Mode        : ").append(mine.isEstimationMode()).append(Util.LS);
    sb.append("   Levels                 : ").append(numLevels).append(Util.LS);
    sb.append("   Level 0 Sorted         : ").append(mine.isLevelZeroSorted()).append(Util.LS);
    sb.append("   Capacity Items         : ").append(levelsArr[numLevels]).append(Util.LS);
    sb.append("   Retained Items         : ").append(mine.getNumRetained()).append(Util.LS);
    if (mine.updatableMemFormat) {
      sb.append("   Updatable Storage Bytes: ").append(mine.getCurrentUpdatableSerializedSizeBytes()).append(Util.LS);
    } else {
      sb.append("   Compact Storage Bytes  : ").append(mine.getCurrentCompactSerializedSizeBytes()).append(Util.LS);
    }

    if (doubleType) {
      sb.append("   Min Value              : ").append(mine.getMinDoubleValue()).append(Util.LS);
      sb.append("   Max Value              : ").append(mine.getMaxDoubleValue()).append(Util.LS);
    } else {
      sb.append("   Min Value              : ").append(mine.getMinFloatValue()).append(Util.LS);
      sb.append("   Max Value              : ").append(mine.getMaxFloatValue()).append(Util.LS);
    }
    sb.append("### End sketch summary").append(Util.LS);

    double[] myDoubleItemsArr = null;
    float[] myFloatItemsArr = null;
    if (doubleType) {
      myDoubleItemsArr = mine.getDoubleItemsArray();
    } else {
      myFloatItemsArr = mine.getFloatItemsArray();
    }
    if (withLevels) {
      sb.append(outputLevels(k, m, numLevels, levelsArr));
    }
    if (withData) {
      sb.append(outputData(doubleType, numLevels, levelsArr, myFloatItemsArr, myDoubleItemsArr));
    }
    return sb.toString();
  }

  /**
   * This method exists for testing purposes only.  The resulting byteArray
   * structure is an internal format and not supported for general transport
   * or compatibility between systems and may be subject to change in the future.
   * @param mine the current sketch to be serialized.
   * @return a byte array in an updatable form.
   */
  private static byte[] toUpdatableByteArrayFromUpdatableMemory(final KllSketch mine) {
    final boolean doubleType = (mine.sketchType == SketchType.DOUBLES_SKETCH);
    final int curBytes = mine.getCurrentUpdatableSerializedSizeBytes();
    final long n = mine.getN();
    final byte flags = (byte) (UPDATABLE_BIT_MASK
        | ((n == 0) ? EMPTY_BIT_MASK : 0)
        | ((n == 1) ? SINGLE_ITEM_BIT_MASK : 0)
        | (doubleType ? DOUBLES_SKETCH_BIT_MASK : 0));
    final byte[] byteArr = new byte[curBytes];
    mine.wmem.getByteArray(0, byteArr, 0, curBytes);
    byteArr[FLAGS_BYTE_ADR] = flags;
    return byteArr;
  }

  /**
   * This method exists for testing purposes only.  The resulting byteArray
   * structure is an internal format and not supported for general transport
   * or compatibility between systems and may be subject to change in the future.
   * @param mine the current sketch to be serialized.
   * @return a byte array in an updatable form.
   */
  static byte[] toUpdatableByteArrayImpl(final KllSketch mine) {
    if (mine.hasMemory() && mine.updatableMemFormat) {
      return toUpdatableByteArrayFromUpdatableMemory(mine);
    }
    final byte[] byteArr = new byte[mine.getCurrentUpdatableSerializedSizeBytes()];
    final WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    loadFirst8Bytes(mine, wmem, true);
    //remainder of preamble after first 8 bytes
    setMemoryN(wmem, mine.getN());
    setMemoryMinK(wmem, mine.getMinK());
    setMemoryNumLevels(wmem, mine.getNumLevels());

    //load data
    final boolean doubleType = (mine.sketchType == DOUBLES_SKETCH);
    int offset = DATA_START_ADR;

    //LOAD LEVELS ARRAY the last integer in levels_ IS serialized
    final int[] myLevelsArr = mine.getLevelsArray();
    final int len = myLevelsArr.length;
    wmem.putIntArray(offset, myLevelsArr, 0, len);
    offset += len * Integer.BYTES;

    //LOAD MIN, MAX VALUES FOLLOWED BY ITEMS ARRAY
    if (doubleType) {
      wmem.putDouble(offset, mine.getMinDoubleValue());
      offset += Double.BYTES;
      wmem.putDouble(offset, mine.getMaxDoubleValue());
      offset += Double.BYTES;
      final double[] doubleItemsArr = mine.getDoubleItemsArray();
      wmem.putDoubleArray(offset, doubleItemsArr, 0, doubleItemsArr.length);
    } else {
      wmem.putFloat(offset, mine.getMinFloatValue());
      offset += Float.BYTES;
      wmem.putFloat(offset,mine.getMaxFloatValue());
      offset += Float.BYTES;
      final float[] floatItemsArr = mine.getFloatItemsArray();
      wmem.putFloatArray(offset, floatItemsArr, 0, floatItemsArr.length);
    }
    return byteArr;
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
   * @param mine the current sketch
   */
  private static void addEmptyTopLevelToCompletelyFullSketch(final KllSketch mine) {
    final int[] myCurLevelsArr = mine.getLevelsArray();
    final int myCurNumLevels = mine.getNumLevels();
    final int myCurTotalItemsCapacity = myCurLevelsArr[myCurNumLevels];
    double minDouble = Double.NaN;
    double maxDouble = Double.NaN;
    float minFloat = Float.NaN;
    float maxFloat = Float.NaN;

    double[] myCurDoubleItemsArr = null;
    float[] myCurFloatItemsArr = null;

    final int myNewNumLevels;
    final int[] myNewLevelsArr;
    final int myNewTotalItemsCapacity;

    float[] myNewFloatItemsArr = null;
    double[] myNewDoubleItemsArr = null;

    if (mine.sketchType == DOUBLES_SKETCH) {
      minDouble = mine.getMinDoubleValue();
      maxDouble = mine.getMaxDoubleValue();
      myCurDoubleItemsArr = mine.getDoubleItemsArray();
      //assert we are following a certain growth scheme
      assert myCurDoubleItemsArr.length == myCurTotalItemsCapacity;
    } else { //FLOATS_SKETCH
      minFloat = mine.getMinFloatValue();
      maxFloat = mine.getMaxFloatValue();
      myCurFloatItemsArr = mine.getFloatItemsArray();
      assert myCurFloatItemsArr.length == myCurTotalItemsCapacity;
    }
    assert myCurLevelsArr[0] == 0; //definition of full is part of the growth scheme

    final int deltaItemsCap = levelCapacity(mine.getK(), myCurNumLevels + 1, 0, mine.getM());
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
      mine.incNumLevels(); //increment the class member
    } else {
      myNewLevelsArr = myCurLevelsArr;
      myNewNumLevels = myCurNumLevels;
    }
    // This loop updates all level indices EXCLUDING the "extra" index at the top
    for (int level = 0; level <= myNewNumLevels - 1; level++) {
      myNewLevelsArr[level] += deltaItemsCap;
    }
    myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCapacity; // initialize the new "extra" index at the top

    // GROW ITEMS ARRAY
    if (mine.sketchType == DOUBLES_SKETCH) {
      myNewDoubleItemsArr = new double[myNewTotalItemsCapacity];
      // copy and shift the current data into the new array
      System.arraycopy(myCurDoubleItemsArr, 0, myNewDoubleItemsArr, deltaItemsCap, myCurTotalItemsCapacity);
    } else {
      myNewFloatItemsArr = new float[myNewTotalItemsCapacity];
      // copy and shift the current items data into the new array
      System.arraycopy(myCurFloatItemsArr, 0, myNewFloatItemsArr, deltaItemsCap, myCurTotalItemsCapacity);
    }

    //MEMORY SPACE MANAGEMENT
    if (mine.updatableMemFormat) {
      mine.wmem = memorySpaceMgmt(mine, myNewLevelsArr.length, myNewTotalItemsCapacity);
    }
    //update our sketch with new expanded spaces
    mine.setNumLevels(myNewNumLevels);
    mine.setLevelsArray(myNewLevelsArr);
    if (mine.sketchType == DOUBLES_SKETCH) {
      mine.setMinDoubleValue(minDouble);
      mine.setMaxDoubleValue(maxDouble);
      mine.setDoubleItemsArray(myNewDoubleItemsArr);
    } else { //Float sketch
      mine.setMinFloatValue(minFloat);
      mine.setMaxFloatValue(maxFloat);
      mine.setFloatItemsArray(myNewFloatItemsArr);
    }
  }

  /**
   * Finds the first level starting with level 0 that exceeds its nominal capacity
   * @param k configured size of sketch. Range [m, 2^16]
   * @param m minimum level size. Default is 8.
   * @param numLevels one-based number of current levels
   * @return level to compact
   */
  private static int findLevelToCompact(final int k, final int m, final int numLevels, final int[] levels) {
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

  private static void loadFirst8Bytes(final KllSketch sk, final WritableMemory wmem,
      final boolean updatableFormat) {
    final boolean empty = sk.getN() == 0;
    final boolean lvlZeroSorted = sk.isLevelZeroSorted();
    final boolean singleItem = sk.getN() == 1;
    final boolean doubleType = (sk.sketchType == DOUBLES_SKETCH);
    final int preInts = updatableFormat
        ? PREAMBLE_INTS_FULL
        : (empty || singleItem) ? PREAMBLE_INTS_EMPTY_SINGLE : PREAMBLE_INTS_FULL;
    //load the preamble
    setMemoryPreInts(wmem, preInts);
    final int server = updatableFormat ? SERIAL_VERSION_UPDATABLE
        : (singleItem ? SERIAL_VERSION_SINGLE : SERIAL_VERSION_EMPTY_FULL);
    setMemorySerVer(wmem, server);
    setMemoryFamilyID(wmem, Family.KLL.getID());
    setMemoryEmptyFlag(wmem, empty);
    setMemoryLevelZeroSortedFlag(wmem, lvlZeroSorted);
    setMemorySingleItemFlag(wmem, singleItem);
    setMemoryDoubleSketchFlag(wmem, doubleType);
    setMemoryUpdatableFlag(wmem, updatableFormat);
    setMemoryK(wmem, sk.getK());
    setMemoryM(wmem, sk.getM());
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
