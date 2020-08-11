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

import static org.apache.datasketches.Util.floorPowerOf2;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.SketchesArgumentException;

/**
 * Static methods to support KllSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
class KllHelper {

  private static final Random random = new Random();

  static boolean isEven(final int value) {
    return (value & 1) == 0;
  }

  static boolean isOdd(final int value) {
    return (value & 1) == 1;
  }

  /**
   * Checks the sequential validity of the given array of float values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of values
   */
  static final void validateValues(final float[] values) {
    for (int i = 0; i < values.length ; i++) {
      if (Float.isNaN(values[i])) {
        throw new SketchesArgumentException("Values must not be NaN");
      }
      if ((i < (values.length - 1)) && (values[i] >= values[i + 1])) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

  /**
   * Copy the old array into a new larger array.
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
   * Returns the upper bound of the number of levels based on <i>n</i>.
   * @param n the length of the stream
   * @return floor( log_2(n) )
   */
  static int ubOnNumLevels(final long n) {
    return 1 + Long.numberOfTrailingZeros(floorPowerOf2(n));
  }

  /**
   * Returns the maximum number of items that this sketch can handle
   * @param k The sizing / accuracy parameter of the sketch in items.
   * Note: this method actually works for k values up to k = 2^29 and 61 levels,
   * however only k values up to (2^16 - 1) are currently used by the sketch.
   * @param m the size of the smallest level in items.
   * @param numLevels the upper bound number of levels based on <i>n</i> items.
   * @return the total item capacity of the sketch.
   */
  static int computeTotalCapacity(final int k, final int m, final int numLevels) {
    long total = 0;
    for (int h = 0; h < numLevels; h++) {
      total += levelCapacity(k, numLevels, h, m);
    }
    return (int) total;
  }

  /**
   * Returns the capacity of a specific level.
   * @param k the accuracy parameter of the sketch. Maximum is 2^29.
   * @param numLevels the number of current levels in the sketch. Maximum is 61.
   * @param height the zero-based index of a level with respect to the smallest level.
   * This varies from 0 to 60.
   * @param minWidth the minimum level width. Default is 8.
   * @return the capacity of a specific level
   */
  static int levelCapacity(final int k, final int numLevels, final int height, final int minWidth) {
    assert (k <= (1 << 29));
    assert (numLevels >= 1) && (numLevels <= 61);
    assert (height >= 0) && (height < numLevels);
    final int depth = numLevels - height - 1;
    return (int) Math.max(minWidth, intCapAux(k, depth));
  }

  /**
   * Computes the actual capacity of a given level given its depth index.
   * If the depth of levels exceeds 30, this uses a folding technique to accurately compute the
   * actual leval capacity upto a depth of 60. Without folding, the internal calculations would
   * excceed the capacity of a long.
   * @param k the configured k of the sketch
   * @param depth the zero-based index of the level being computed.
   * @return the actual capacity of a given level given its depth index.
   */
  private static long intCapAux(final int k, final int depth) {
    if (depth <= 30) { return (int) intCapAuxAux(k, depth); }
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
    final long twok = k << 1; // for rounding, we pre-multiply by 2
    final long tmp = ((twok << depth) / powersOfThree[depth]);
    final long result = ((tmp + 1) >> 1); // then here we add 1 and divide by 2
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

  static void mergeSortedArrays(final float[] bufA, final int startA, final int lenA,
      final float[] bufB, final int startB, final int lenB, final float[] bufC, final int startC) {
    final int lenC = lenA + lenB;
    final int limA = startA + lenA;
    final int limB = startB + lenB;
    final int limC = startC + lenC;

    int a = startA;
    int b = startB;

    for (int c = startC; c < limC; c++) {
      if (a == limA) {
        bufC[c] = bufB[b];
        b++;
      } else if (b == limB) {
        bufC[c] = bufA[a];
        a++;
      } else if (bufA[a] < bufB[b]) {
        bufC[c] = bufA[a];
        a++;
      } else {
        bufC[c] = bufB[b];
        b++;
      }
    }
    assert a == limA;
    assert b == limB;
  }

  /**
   * Compression algorithm used to merge higher levels.
   * <p>Here is what we do for each level:</p>
   * <ul><li>If it does not need to be compacted, then simply copy it over.</li>
   * <li>Otherwise, it does need to be compacted, so...
   *   <ul><li>Copy zero or one guy over.</li>
   *       <li>If the level above is empty, halve up.</li>
   *       <li>Else the level above is nonempty, so halve down, then merge up.</li>
   *   </ul></li>
   * <li>Adjust the boundaries of the level above.</li>
   * </ul>
   *
   * <p>It can be proved that generalCompress returns a sketch that satisfies the space constraints
   * no matter how much data is passed in.
   * We are pretty sure that it works correctly when inBuf and outBuf are the same.
   * All levels except for level zero must be sorted before calling this, and will still be
   * sorted afterwards.
   * Level zero is not required to be sorted before, and may not be sorted afterwards.</p>
   *
   * <p>This trashes inBuf and inLevels and modifies outBuf and outLevels.</p>
   *
   * @param k The sketch parameter k
   * @param m The minimum level size
   * @param numLevelsIn provisional number of number of levels = max(this.numLevels, other.numLevels)
   * @param inBuf work buffer of size = this.getNumRetained() + other.getNumRetainedAboveLevelZero().
   * This contains the float[] of the other sketch
   * @param inLevels work levels array size = ubOnNumLevels(this.n + other.n) + 2
   * @param outBuf the same array as inBuf
   * @param outLevels the same size as inLevels
   * @param isLevelZeroSorted true if this.level 0 is sorted
   * @return int array of: {numLevels, targetItemCount, currentItemCount)
   */
  static int[] generalCompress(
      final int k,
      final int m,
      final int numLevelsIn,
      final float[] inBuf,
      final int[] inLevels,
      final float[] outBuf,
      final int[] outLevels,
      final boolean isLevelZeroSorted) {
    assert numLevelsIn > 0; // things are too weird if zero levels are allowed
    int numLevels = numLevelsIn;
    int currentItemCount = inLevels[numLevels] - inLevels[0]; // decreases with each compaction
    int targetItemCount = computeTotalCapacity(k, m, numLevels); // increases if we add levels
    boolean doneYet = false;
    outLevels[0] = 0;
    int curLevel = -1;
    while (!doneYet) {
      curLevel++; // start out at level 0

      // If we are at the current top level, add an empty level above it for convenience,
      // but do not increment numLevels until later
      if (curLevel == (numLevels - 1)) {
        inLevels[curLevel + 2] = inLevels[curLevel + 1];
      }

      final int rawBeg = inLevels[curLevel];
      final int rawLim = inLevels[curLevel + 1];
      final int rawPop = rawLim - rawBeg;

      if ((currentItemCount < targetItemCount) || (rawPop < levelCapacity(k, numLevels, curLevel, m))) {
        // copy level over as is
        // because inBuf and outBuf could be the same, make sure we are not moving data upwards!
        assert (rawBeg >= outLevels[curLevel]);
        System.arraycopy(inBuf, rawBeg, outBuf, outLevels[curLevel], rawPop);
        outLevels[curLevel + 1] = outLevels[curLevel] + rawPop;
      }
      else {
        // The sketch is too full AND this level is too full, so we compact it
        // Note: this can add a level and thus change the sketch's capacity

        final int popAbove = inLevels[curLevel + 2] - rawLim;
        final boolean oddPop = isOdd(rawPop);
        final int adjBeg = oddPop ? 1 + rawBeg : rawBeg;
        final int adjPop = oddPop ? rawPop - 1 : rawPop;
        final int halfAdjPop = adjPop / 2;

        if (oddPop) { // copy one guy over
          outBuf[outLevels[curLevel]] = inBuf[rawBeg];
          outLevels[curLevel + 1] = outLevels[curLevel] + 1;
        } else { // copy zero guys over
          outLevels[curLevel + 1] = outLevels[curLevel];
        }

        // level zero might not be sorted, so we must sort it if we wish to compact it
        if ((curLevel == 0) && !isLevelZeroSorted) {
          Arrays.sort(inBuf, adjBeg, adjBeg + adjPop);
        }

        if (popAbove == 0) { // Level above is empty, so halve up
          randomlyHalveUp(inBuf, adjBeg, adjPop);
        } else { // Level above is nonempty, so halve down, then merge up
          randomlyHalveDown(inBuf, adjBeg, adjPop);
          mergeSortedArrays(inBuf, adjBeg, halfAdjPop, inBuf, rawLim, popAbove, inBuf, adjBeg + halfAdjPop);
        }

        // track the fact that we just eliminated some data
        currentItemCount -= halfAdjPop;

        // Adjust the boundaries of the level above
        inLevels[curLevel + 1] = inLevels[curLevel + 1] - halfAdjPop;

        // Increment numLevels if we just compacted the old top level
        // This creates some more capacity (the size of the new bottom level)
        if (curLevel == (numLevels - 1)) {
          numLevels++;
          targetItemCount += levelCapacity(k, numLevels, 0, m);
        }

      } // end of code for compacting a level

      // determine whether we have processed all levels yet (including any new levels that we created)

      if (curLevel == (numLevels - 1)) { doneYet = true; }

    } // end of loop over levels

    assert (outLevels[numLevels] - outLevels[0]) == currentItemCount;

    return new int[] {numLevels, targetItemCount, currentItemCount};
  }

  static void randomlyHalveDown(final float[] buf, final int start, final int length) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);
    //final int offset = deterministicOffset(); // for validation
    int j = start + offset;
    for (int i = start; i < (start + half_length); i++) {
      buf[i] = buf[j];
      j += 2;
    }
  }

  static void randomlyHalveUp(final float[] buf, final int start, final int length) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);
    //final int offset = deterministicOffset(); // for validation
    int j = (start + length) - 1 - offset;
    for (int i = (start + length) - 1; i >= (start + half_length); i--) {
      buf[i] = buf[j];
      j -= 2;
    }
  }

  // Enable the following to use KllValidationTest

  //  static int nextOffset = 0;

  //  private static int deterministicOffset() {
  //    final int result = nextOffset;
  //    nextOffset = 1 - nextOffset;
  //    return result;
  //  }

}
