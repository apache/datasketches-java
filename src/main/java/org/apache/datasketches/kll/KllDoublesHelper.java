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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.Util.isEven;
import static org.apache.datasketches.Util.isOdd;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.SketchesArgumentException;

/**
 * Static methods to support KllDoublesSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
final class KllDoublesHelper {

  static double getDoubleRank(final KllSketch mine, final double value) {
    if (mine.isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    final double[] myDoubleItemsArr = mine.getDoubleItemsArray();
    final int[] myLevelsArr = mine.getLevelsArray();
    while (level < mine.getNumLevels()) {
      final int fromIndex = myLevelsArr[level];
      final int toIndex = myLevelsArr[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (myDoubleItemsArr[i] < value) {
          total += weight;
        } else if (level > 0 || mine.isLevelZeroSorted()) {
          break; // levels above 0 are sorted, no point comparing further
        }
      }
      level++;
      weight *= 2;
    }
    return (double) total / mine.getN();
  }

  static double[] getDoublesPmfOrCdf(final KllSketch mine, final double[] splitPoints, final boolean isCdf) {
    if (mine.isEmpty()) { return null; }
    validateDoubleValues(splitPoints);
    final double[] buckets = new double[splitPoints.length + 1];
    final int myNumLevels = mine.getNumLevels();
    final int[] myLevelsArr = mine.getLevelsArray();
    int level = 0;
    int weight = 1;
    while (level < myNumLevels) {
      final int fromIndex = myLevelsArr[level];
      final int toIndex = myLevelsArr[level + 1]; // exclusive
      if (level == 0 && !mine.isLevelZeroSorted()) {
        KllDoublesHelper.incrementDoublesBucketsUnsortedLevel(mine, fromIndex, toIndex, weight, splitPoints, buckets);
      } else {
        KllDoublesHelper.incrementDoublesBucketsSortedLevel(mine, fromIndex, toIndex, weight, splitPoints, buckets);
      }
      level++;
      weight *= 2;
    }
    // normalize and, if CDF, convert to cumulative
    if (isCdf) {
      double subtotal = 0;
      for (int i = 0; i < buckets.length; i++) {
        subtotal += buckets[i];
        buckets[i] = subtotal / mine.getN();
      }
    } else {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] /= mine.getN();
      }
    }
    return buckets;
  }

  static double getDoublesQuantile(final KllSketch mine, final double fraction) {
    if (mine.isEmpty()) { return Double.NaN; }
    if (fraction < 0.0 || fraction > 1.0) {
      throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
    }
    //These two assumptions make KLL compatible with the previous classic Quantiles Sketch
    if (fraction == 0.0) { return mine.getMinDoubleValue(); }
    if (fraction == 1.0) { return mine.getMaxDoubleValue(); }
    final KllDoublesQuantileCalculator quant = KllDoublesHelper.getDoublesQuantileCalculator(mine);
    return quant.getQuantile(fraction);
  }

  static double[] getDoublesQuantiles(final KllSketch mine, final double[] fractions) {
    if (mine.isEmpty()) { return null; }
    KllDoublesQuantileCalculator quant = null;
    final double[] quantiles = new double[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      final double fraction = fractions[i];
      if (fraction < 0.0 || fraction > 1.0) {
        throw new SketchesArgumentException("Fraction cannot be less than zero nor greater than 1.0");
      }
      if      (fraction == 0.0) { quantiles[i] = mine.getMinDoubleValue(); }
      else if (fraction == 1.0) { quantiles[i] = mine.getMaxDoubleValue(); }
      else {
        if (quant == null) {
          quant = KllDoublesHelper.getDoublesQuantileCalculator(mine);
        }
        quantiles[i] = quant.getQuantile(fraction);
      }
    }
    return quantiles;
  }

  static void mergeDoubleImpl(final KllSketch mine, final KllSketch other) {
    if (other.isEmpty()) { return; }
    final long finalN = mine.getN() + other.getN();
    final int otherNumLevels = other.getNumLevels();
    final int[] otherLevelsArr = other.getLevelsArray();
    final double[] otherDoubleItemsArr;
    //capture my min & max, minK
    final double myMin = mine.getMinDoubleValue();
    final double myMax = mine.getMaxDoubleValue();
    final int myMinK = mine.getMinK();

    //update this sketch with level0 items from the other sketch
    if (other.isCompactSingleItem()) {
      updateDouble(mine, other.getDoubleSingleItem());
      otherDoubleItemsArr = new double[0];
    } else {
      otherDoubleItemsArr = other.getDoubleItemsArray();
      for (int i = otherLevelsArr[0]; i < otherLevelsArr[1]; i++) {
        KllDoublesHelper.updateDouble(mine, otherDoubleItemsArr[i]);
      }
    }
    // after the level 0 update, we capture the state of levels and items arrays
    final int myCurNumLevels = mine.getNumLevels();
    final int[] myCurLevelsArr = mine.getLevelsArray();
    final double[] myCurDoubleItemsArr = mine.getDoubleItemsArray();

    int myNewNumLevels = myCurNumLevels;
    int[] myNewLevelsArr = myCurLevelsArr;
    double[] myNewDoubleItemsArr = myCurDoubleItemsArr;

    if (otherNumLevels > 1 && !other.isCompactSingleItem()) { //now merge other levels if they exist
      final int tmpSpaceNeeded = mine.getNumRetained()
          + KllHelper.getNumRetainedAboveLevelZero(otherNumLevels, otherLevelsArr);
      final double[] workbuf = new double[tmpSpaceNeeded];
      final int ub = KllHelper.ubOnNumLevels(finalN);
      final int[] worklevels = new int[ub + 2]; // ub+1 does not work
      final int[] outlevels  = new int[ub + 2];

      final int provisionalNumLevels = max(myCurNumLevels, otherNumLevels);

      populateDoubleWorkArrays(workbuf, worklevels, provisionalNumLevels,
          myCurNumLevels, myCurLevelsArr, myCurDoubleItemsArr,
          otherNumLevels, otherLevelsArr, otherDoubleItemsArr);

      // notice that workbuf is being used as both the input and output
      final int[] result = generalDoublesCompress(mine.getK(), mine.getM(), provisionalNumLevels,
          workbuf, worklevels, workbuf, outlevels, mine.isLevelZeroSorted(), KllSketch.random);
      final int targetItemCount = result[1]; //was finalCapacity. Max size given k, m, numLevels
      final int curItemCount = result[2]; //was finalPop

      // now we need to finalize the results for the "self" sketch

      //THE NEW NUM LEVELS
      myNewNumLevels = result[0]; //was finalNumLevels
      assert myNewNumLevels <= ub; // ub may be much bigger

      // THE NEW ITEMS ARRAY (was newbuf)
      myNewDoubleItemsArr = (targetItemCount == myCurDoubleItemsArr.length)
          ? myCurDoubleItemsArr
          : new double[targetItemCount];
      final int freeSpaceAtBottom = targetItemCount - curItemCount;
      //shift the new items array
      System.arraycopy(workbuf, outlevels[0], myNewDoubleItemsArr, freeSpaceAtBottom, curItemCount);
      final int theShift = freeSpaceAtBottom - outlevels[0];

      //calculate the new levels array length
      final int finalLevelsArrLen;
      if (myCurLevelsArr.length < myNewNumLevels + 1) { finalLevelsArrLen = myNewNumLevels + 1; }
      else { finalLevelsArrLen = myCurLevelsArr.length; }

      //THE NEW LEVELS ARRAY
      myNewLevelsArr = new int[finalLevelsArrLen];
      for (int lvl = 0; lvl < myNewNumLevels + 1; lvl++) { // includes the "extra" index
        myNewLevelsArr[lvl] = outlevels[lvl] + theShift;
      }

      //MEMORY SPACE MANAGEMENT
      if (mine.updatableMemFormat) {
        mine.wmem = KllHelper.memorySpaceMgmt(mine, myNewLevelsArr.length, myNewDoubleItemsArr.length);
      }
    }

    //Update Preamble:
    mine.setN(finalN);
    if (other.isEstimationMode()) { //otherwise the merge brings over exact items.
      mine.setMinK(min(myMinK, other.getMinK()));
    }

    //Update numLevels, levelsArray, items
    mine.setNumLevels(myNewNumLevels);
    mine.setLevelsArray(myNewLevelsArr);
    mine.setDoubleItemsArray(myNewDoubleItemsArr);

    //Update min, max values
    final double otherMin = other.getMinDoubleValue();
    final double otherMax = other.getMaxDoubleValue();
    mine.setMinDoubleValue(resolveDoubleMinValue(myMin, otherMin));
    mine.setMaxDoubleValue(resolveDoubleMaxValue(myMax, otherMax));
    assert KllHelper.sumTheSampleWeights(mine.getNumLevels(), mine.getLevelsArray()) == mine.getN();
  }

  static void mergeSortedDoubleArrays(
      final double[] bufA, final int startA, final int lenA,
      final double[] bufB, final int startB, final int lenB,
      final double[] bufC, final int startC) {
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
   * Validation Method. This must be modified to test validation
   * @param buf the items array
   * @param start data start
   * @param length items length
   * @param random instance of Random
   */ //NOTE Validation Method: Need to modify.
  static void randomlyHalveDownDoubles(final double[] buf, final int start, final int length, final Random random) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);       // disable for validation
    //final int offset = deterministicOffset(); // enable for validation
    int j = start + offset;
    for (int i = start; i < (start + half_length); i++) {
      buf[i] = buf[j];
      j += 2;
    }
  }

  /**
   * Validation Method. This must be modified to test validation
   * @param buf the items array
   * @param start data start
   * @param length items length
   * @param random instance of Random
   */ //NOTE Validation Method: Need to modify.
  static void randomlyHalveUpDoubles(final double[] buf, final int start, final int length, final Random random) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);       // disable for validation
    //final int offset = deterministicOffset(); // enable for validation
    int j = (start + length) - 1 - offset;
    for (int i = (start + length) - 1; i >= (start + half_length); i--) {
      buf[i] = buf[j];
      j -= 2;
    }
  }

  static void updateDouble(final KllSketch mine, final double value) {
    if (Double.isNaN(value)) { return; }
    final double prevMin = mine.getMinDoubleValue();
    final double prevMax = mine.getMaxDoubleValue();
    mine.setMinDoubleValue(resolveDoubleMinValue(prevMin, value));
    mine.setMaxDoubleValue(resolveDoubleMaxValue(prevMax, value));
    if (mine.getLevelsArray()[0] == 0) { KllHelper.compressWhileUpdatingSketch(mine); }
    final int myLevelsArrAtZero = mine.getLevelsArray()[0]; //LevelsArr could be expanded
    mine.incN();
    mine.setLevelZeroSorted(false);
    final int nextPos = myLevelsArrAtZero - 1;
    assert myLevelsArrAtZero >= 0;
    mine.setLevelsArrayAt(0, nextPos);
    mine.setDoubleItemsArrayAt(nextPos, value);
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
   * This contains the double[] of the other sketch
   * @param inLevels work levels array size = ubOnNumLevels(this.n + other.n) + 2
   * @param outBuf the same array as inBuf
   * @param outLevels the same size as inLevels
   * @param isLevelZeroSorted true if this.level 0 is sorted
   * @param random instance of java.util.Random
   * @return int array of: {numLevels, targetItemCount, currentItemCount)
   */
  private static int[] generalDoublesCompress(
      final int k,
      final int m,
      final int numLevelsIn,
      final double[] inBuf,
      final int[] inLevels,
      final double[] outBuf,
      final int[] outLevels,
      final boolean isLevelZeroSorted,
      final Random random) {
    assert numLevelsIn > 0; // things are too weird if zero levels are allowed
    int numLevels = numLevelsIn;
    int currentItemCount = inLevels[numLevels] - inLevels[0]; // decreases with each compaction
    int targetItemCount = KllHelper.computeTotalItemCapacity(k, m, numLevels); // increases if we add levels
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

      if ((currentItemCount < targetItemCount) || (rawPop < KllHelper.levelCapacity(k, numLevels, curLevel, m))) {
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
          randomlyHalveUpDoubles(inBuf, adjBeg, adjPop, random);
        } else { // Level above is nonempty, so halve down, then merge up
          randomlyHalveDownDoubles(inBuf, adjBeg, adjPop, random);
          mergeSortedDoubleArrays(inBuf, adjBeg, halfAdjPop, inBuf, rawLim, popAbove, inBuf, adjBeg + halfAdjPop);
        }

        // track the fact that we just eliminated some data
        currentItemCount -= halfAdjPop;

        // Adjust the boundaries of the level above
        inLevels[curLevel + 1] = inLevels[curLevel + 1] - halfAdjPop;

        // Increment numLevels if we just compacted the old top level
        // This creates some more capacity (the size of the new bottom level)
        if (curLevel == (numLevels - 1)) {
          numLevels++;
          targetItemCount += KllHelper.levelCapacity(k, numLevels, 0, m);
        }
      } // end of code for compacting a level

      // determine whether we have processed all levels yet (including any new levels that we created)
      if (curLevel == (numLevels - 1)) { doneYet = true; }
    } // end of loop over levels

    assert (outLevels[numLevels] - outLevels[0]) == currentItemCount;
    return new int[] {numLevels, targetItemCount, currentItemCount};
  }

  private static KllDoublesQuantileCalculator getDoublesQuantileCalculator(final KllSketch mine) {
    final int[] myLevelsArr = mine.getLevelsArray();
    final double[] myDoubleItemsArr = mine.getDoubleItemsArray();
    if (!mine.isLevelZeroSorted()) {
      Arrays.sort(myDoubleItemsArr,  myLevelsArr[0], myLevelsArr[1]);
      if (!mine.hasMemory()) { mine.setLevelZeroSorted(true); }
    }
    return new KllDoublesQuantileCalculator(myDoubleItemsArr, myLevelsArr, mine.getNumLevels(), mine.getN());
  }

  private static void incrementDoublesBucketsSortedLevel(
      final KllSketch mine, final int fromIndex, final int toIndex,
      final int weight, final double[] splitPoints, final double[] buckets) {
    final double[] myDoubleItemsArr = mine.getDoubleItemsArray();
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
      if (myDoubleItemsArr[i] < splitPoints[j]) {
        buckets[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket
      }
    }
    // now either i == toIndex (we are out of samples), or
    // j == numSplitPoints (we are out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case
    if (j == splitPoints.length) {
      buckets[j] += weight * (toIndex - i);
    }
  }

  private static void incrementDoublesBucketsUnsortedLevel(
      final KllSketch mine, final int fromIndex, final int toIndex,
      final int weight, final double[] splitPoints, final double[] buckets) {
    final double[] myDoubleItemsArr = mine.getDoubleItemsArray();
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (myDoubleItemsArr[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
  }

  private static void populateDoubleWorkArrays(
      final double[] workbuf, final int[] worklevels, final int provisionalNumLevels,
      final int myCurNumLevels, final int[] myCurLevelsArr, final double[] myCurDoubleItemsArr,
      final int otherNumLevels, final int[] otherLevelsArr, final double[] otherDoubleItemsArr) {
    worklevels[0] = 0;

    // Note: the level zero data from "other" was already inserted into "self"
    final int selfPopZero = KllHelper.currentLevelSize(0, myCurNumLevels,myCurLevelsArr);
    System.arraycopy(myCurDoubleItemsArr, myCurLevelsArr[0], workbuf, worklevels[0], selfPopZero);
    worklevels[1] = worklevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = KllHelper.currentLevelSize(lvl, myCurNumLevels, myCurLevelsArr);
      final int otherPop = KllHelper.currentLevelSize(lvl, otherNumLevels, otherLevelsArr);
      worklevels[lvl + 1] = worklevels[lvl] + selfPop + otherPop;

      if (selfPop > 0 && otherPop == 0) {
        System.arraycopy(myCurDoubleItemsArr, myCurLevelsArr[lvl], workbuf, worklevels[lvl], selfPop);
      } else if (selfPop == 0 && otherPop > 0) {
        System.arraycopy(otherDoubleItemsArr, otherLevelsArr[lvl], workbuf, worklevels[lvl], otherPop);
      } else if (selfPop > 0 && otherPop > 0) {
        mergeSortedDoubleArrays(myCurDoubleItemsArr, myCurLevelsArr[lvl], selfPop, otherDoubleItemsArr,
            otherLevelsArr[lvl], otherPop, workbuf, worklevels[lvl]);
      }
    }
  }

  private static double resolveDoubleMaxValue(final double myMax, final double otherMax) {
    if (Double.isNaN(myMax) && Double.isNaN(otherMax)) { return Double.NaN; }
    if (Double.isNaN(myMax)) { return otherMax; }
    if (Double.isNaN(otherMax)) { return myMax; }
    return max(myMax, otherMax);
  }

  private static double resolveDoubleMinValue(final double myMin, final double otherMin) {
    if (Double.isNaN(myMin) && Double.isNaN(otherMin)) { return Double.NaN; }
    if (Double.isNaN(myMin)) { return otherMin; }
    if (Double.isNaN(otherMin)) { return myMin; }
    return min(myMin, otherMin);
  }

  /**
   * Checks the sequential validity of the given array of double values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of values
   */
  private static void validateDoubleValues(final double[] values) {
    for (int i = 0; i < values.length; i++) {
      if (!Double.isFinite(values[i])) {
        throw new SketchesArgumentException("Values must be finite");
      }
      if (i < values.length - 1 && values[i] >= values[i + 1]) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

  /*
   * Validation Method.
   * The following must be enabled for use with the KllDoublesValidationTest,
   * which is only enabled for manual testing. In addition, two Validation Methods
   * above need to be modified.
   */ //NOTE Validation Method: Need to uncomment
  //    static int nextOffset = 0;
  //
  //    private static int deterministicOffset() {
  //      final int result = nextOffset;
  //      nextOffset = 1 - nextOffset;
  //      return result;
  //    }

}
