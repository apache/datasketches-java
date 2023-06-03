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
import static org.apache.datasketches.common.Util.isEven;
import static org.apache.datasketches.common.Util.isOdd;

import java.util.Arrays;
import java.util.Random;

/**
 * Static methods to support KllFloatsSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
final class KllFloatsHelper {

  //Called from KllSketch
  static void mergeFloatImpl(final KllFloatsSketch mySketch, final KllSketch other) {
    final KllFloatsSketch otherFltSk = (KllFloatsSketch) other;
    if (otherFltSk.isEmpty()) { return; }
    mySketch.nullSortedView();
    final long finalN = mySketch.getN() + otherFltSk.getN();
    final int otherNumLevels = otherFltSk.getNumLevels();
    final int[] otherLevelsArr = otherFltSk.getLevelsArray();
    final float[] otherFloatItemsArr;
    //capture my min & max, minK
    final float myMin = mySketch.isEmpty() ? Float.NaN : mySketch.getMinFloatItem();
    final float myMax = mySketch.isEmpty() ? Float.NaN : mySketch.getMaxFloatItem();
    final int myMinK = mySketch.getMinK();

    //update this sketch with level0 items from the other sketch
    if (otherFltSk.isCompactSingleItem()) {
      updateFloat(mySketch, otherFltSk.getFloatSingleItem());
      otherFloatItemsArr = new float[0];
    } else {
      otherFloatItemsArr = otherFltSk.getFloatItemsArray();
      for (int i = otherLevelsArr[0]; i < otherLevelsArr[1]; i++) {
       updateFloat(mySketch, otherFloatItemsArr[i]);
      }
    }
    // after the level 0 update, we capture the state of levels and items arrays
    final int myCurNumLevels = mySketch.getNumLevels();
    final int[] myCurLevelsArr = mySketch.getLevelsArray();
    final float[] myCurFloatItemsArr = mySketch.getFloatItemsArray();

    int myNewNumLevels = myCurNumLevels;
    int[] myNewLevelsArr = myCurLevelsArr;
    float[] myNewFloatItemsArr = myCurFloatItemsArr;

    if (otherNumLevels > 1  && !otherFltSk.isCompactSingleItem()) { //now merge higher levels if they exist
      final int tmpSpaceNeeded = mySketch.getNumRetained()
          + KllHelper.getNumRetainedAboveLevelZero(otherNumLevels, otherLevelsArr);
      final float[] workbuf = new float[tmpSpaceNeeded];
      final int ub = KllHelper.ubOnNumLevels(finalN);
      final int[] worklevels = new int[ub + 2]; // ub+1 does not work
      final int[] outlevels  = new int[ub + 2];

      final int provisionalNumLevels = max(myCurNumLevels, otherNumLevels);

      populateFloatWorkArrays(workbuf, worklevels, provisionalNumLevels,
          myCurNumLevels, myCurLevelsArr, myCurFloatItemsArr,
          otherNumLevels, otherLevelsArr, otherFloatItemsArr);

      // notice that workbuf is being used as both the input and output
      final int[] result = generalFloatsCompress(mySketch.getK(), mySketch.getM(), provisionalNumLevels,
          workbuf, worklevels, workbuf, outlevels, mySketch.isLevelZeroSorted(), KllSketch.random);
      final int targetItemCount = result[1]; //was finalCapacity. Max size given k, m, numLevels
      final int curItemCount = result[2]; //was finalPop

      // now we need to finalize the results for the "self" sketch

      //THE NEW NUM LEVELS
      myNewNumLevels = result[0]; //was finalNumLevels
      assert myNewNumLevels <= ub; // ub may be much bigger

      // THE NEW ITEMS ARRAY (was newbuf)
      myNewFloatItemsArr = (targetItemCount == myCurFloatItemsArr.length)
          ? myCurFloatItemsArr
          : new float[targetItemCount];
      final int freeSpaceAtBottom = targetItemCount - curItemCount;
      //shift the new items array
      System.arraycopy(workbuf, outlevels[0], myNewFloatItemsArr, freeSpaceAtBottom, curItemCount);
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
      if (mySketch.updatableMemFormat) {
        mySketch.wmem = KllHelper.memorySpaceMgmt(mySketch, myNewLevelsArr.length, myNewFloatItemsArr.length);
      }
    }

    //Update Preamble:
    mySketch.setN(finalN);
    if (otherFltSk.isEstimationMode()) { //otherwise the merge brings over exact items.
      mySketch.setMinK(min(myMinK, otherFltSk.getMinK()));
    }

    //Update numLevels, levelsArray, items
    mySketch.setNumLevels(myNewNumLevels);
    mySketch.setLevelsArray(myNewLevelsArr);
    mySketch.setFloatItemsArray(myNewFloatItemsArr);

    //Update min, max items
    final float otherMin = otherFltSk.getMinFloatItem();
    final float otherMax = otherFltSk.getMaxFloatItem();
    mySketch.setMinFloatItem(resolveFloatMinItem(myMin, otherMin));
    mySketch.setMaxFloatItem(resolveFloatMaxItem(myMax, otherMax));
    assert KllHelper.sumTheSampleWeights(mySketch.getNumLevels(), mySketch.getLevelsArray()) == mySketch.getN();
  }

  //Called from KllHelper and this.generalFloatssCompress(...), this.populateFloatWorkArrays(...)
  static void mergeSortedFloatArrays(
      final float[] bufA, final int startA, final int lenA,
      final float[] bufB, final int startB, final int lenB,
      final float[] bufC, final int startC) {
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
   * Validation Method. This must be modified to use the validation test
   * @param buf the items array
   * @param start data start
   * @param length items array length
   * @param random instance of Random
   */
  //NOTE Validation Method: Need to modify to run.
  //Called from KllHelper, this.generalFloatsCompress(...)
  static void randomlyHalveDownFloats(final float[] buf, final int start, final int length, final Random random) {
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
   * Validation Method. This must be modified to use the validation test
   * @param buf the items array
   * @param start data start
   * @param length items array length
   * @param random instance of Random
   */
  //NOTE Validation Method: Need to modify to run.
  //Called from KllHelper, this.generalFloatsCompress(...)
  static void randomlyHalveUpFloats(final float[] buf, final int start, final int length, final Random random) {
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

  //Called from KllFloatsSketch, this.mergeFloatImpl(...)
  static void updateFloat(final KllFloatsSketch fltSk, final float item) {
    if (Float.isNaN(item)) { return; }
    final float prevMin = fltSk.getMinFloatItem();
    final float prevMax = fltSk.getMaxFloatItem();
    fltSk.setMinFloatItem(resolveFloatMinItem(prevMin, item));
    fltSk.setMaxFloatItem(resolveFloatMaxItem(prevMax, item));
    if (fltSk.getLevelsArray()[0] == 0) { KllHelper.compressWhileUpdatingSketch(fltSk); }
    final int myLevelsArrAtZero = fltSk.getLevelsArray()[0]; //LevelsArr could be expanded
    fltSk.incN();
    fltSk.setLevelZeroSorted(false);
    final int nextPos = myLevelsArrAtZero - 1;
    assert myLevelsArrAtZero >= 0;
    fltSk.setLevelsArrayAt(0, nextPos);
    fltSk.setFloatItemsArrayAt(nextPos, item);
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
   * @param random instance of java.util.Random
   * @return int array of: {numLevels, targetItemCount, currentItemCount)
   */
  private static int[] generalFloatsCompress(
      final int k,
      final int m,
      final int numLevelsIn,
      final float[] inBuf,
      final int[] inLevels,
      final float[] outBuf,
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
          randomlyHalveUpFloats(inBuf, adjBeg, adjPop, random);
        } else { // Level above is nonempty, so halve down, then merge up
          randomlyHalveDownFloats(inBuf, adjBeg, adjPop, random);
          mergeSortedFloatArrays(inBuf, adjBeg, halfAdjPop, inBuf, rawLim, popAbove, inBuf, adjBeg + halfAdjPop);
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

  private static void populateFloatWorkArrays(
      final float[] workbuf, final int[] worklevels, final int provisionalNumLevels,
      final int myCurNumLevels, final int[] myCurLevelsArr, final float[] myCurFloatItemsArr,
      final int otherNumLevels, final int[] otherLevelsArr, final float[] otherFloatItemsArr) {
    worklevels[0] = 0;

    // Note: the level zero data from "other" was already inserted into "self"
    final int selfPopZero = KllHelper.currentLevelSize(0, myCurNumLevels, myCurLevelsArr);
    System.arraycopy( myCurFloatItemsArr, myCurLevelsArr[0], workbuf, worklevels[0], selfPopZero);
    worklevels[1] = worklevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = KllHelper.currentLevelSize(lvl, myCurNumLevels, myCurLevelsArr);
      final int otherPop = KllHelper.currentLevelSize(lvl, otherNumLevels, otherLevelsArr);
      worklevels[lvl + 1] = worklevels[lvl] + selfPop + otherPop;

      if (selfPop > 0 && otherPop == 0) {
        System.arraycopy(myCurFloatItemsArr, myCurLevelsArr[lvl], workbuf, worklevels[lvl], selfPop);
      } else if (selfPop == 0 && otherPop > 0) {
        System.arraycopy(otherFloatItemsArr, otherLevelsArr[lvl], workbuf, worklevels[lvl], otherPop);
      } else if (selfPop > 0 && otherPop > 0) {
        mergeSortedFloatArrays( myCurFloatItemsArr, myCurLevelsArr[lvl], selfPop, otherFloatItemsArr,
            otherLevelsArr[lvl], otherPop, workbuf, worklevels[lvl]);
      }
    }
  }

  private static float resolveFloatMaxItem(final float myMax, final float otherMax) {
    if (Float.isNaN(myMax) && Float.isNaN(otherMax)) { return Float.NaN; }
    if (Float.isNaN(myMax)) { return otherMax; }
    if (Float.isNaN(otherMax)) { return myMax; }
    return max(myMax, otherMax);
  }

  private static float resolveFloatMinItem(final float myMin, final float otherMin) {
    if (Float.isNaN(myMin) && Float.isNaN(otherMin)) { return Float.NaN; }
    if (Float.isNaN(myMin)) { return otherMin; }
    if (Float.isNaN(otherMin)) { return myMin; }
    return min(myMin, otherMin);
  }

  /*
   * Validation Method.
   * The following must be enabled for use with the KllFloatsValidationTest,
   * which is only enabled for manual testing. In addition, two Validation Methods
   * above need to be modified.
   */ //NOTE Validation Method: Need to uncomment to use
  //    static int nextOffset = 0;
  //
  //    private static int deterministicOffset() {
  //      final int result = nextOffset;
  //      nextOffset = 1 - nextOffset;
  //      return result;
  //    }

}
