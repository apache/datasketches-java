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
import static org.apache.datasketches.kll.KllHelper.findLevelToCompact;
import static org.apache.datasketches.kll.KllSketch.DEFAULT_M;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.memory.WritableMemory;

//
//
/**
 * Static methods to support KllDoublesSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
final class KllDoublesHelper {

  /**
   * Create Items Array from given item and weight.
   * Used with weighted update only.
   * @param item the given item
   * @param weight the given weight
   * @return the Items Array.
   */
  static double[] createItemsArray(final double item, final long weight) {
    final int itemsArrLen = Long.bitCount(weight);
    final double[] itemsArr = new double[itemsArrLen];
    Arrays.fill(itemsArr, item);
    return itemsArr;
  }

  /**
   * The following code is only valid in the special case of exactly reaching capacity while updating.
   * It cannot be used while merging, while reducing k, or anything else.
   * @param dblSk the current KllDoublesSketch
   */
  private static void compressWhileUpdatingSketch(final KllDoublesSketch dblSk) {
    final int level =
        findLevelToCompact(dblSk.getK(), dblSk.getM(), dblSk.getNumLevels(), dblSk.levelsArr);
    if (level == dblSk.getNumLevels() - 1) {
      //The level to compact is the top level, thus we need to add a level.
      //Be aware that this operation grows the items array,
      //shifts the items data and the level boundaries of the data,
      //and grows the levels array and increments numLevels_.
      KllHelper.addEmptyTopLevelToCompletelyFullSketch(dblSk);
    }
    //after this point, the levelsArray will not be expanded, only modified.
    final int[] myLevelsArr = dblSk.levelsArr;
    final int rawBeg = myLevelsArr[level];
    final int rawEnd = myLevelsArr[level + 1];
    // +2 is OK because we already added a new top level if necessary
    final int popAbove = myLevelsArr[level + 2] - rawEnd;
    final int rawPop = rawEnd - rawBeg;
    final boolean oddPop = isOdd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    //the following is specific to Doubles
    final double[] myDoubleItemsArr = dblSk.getDoubleItemsArray();
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
    dblSk.setLevelsArrayAt(level + 1, newIndex);

    if (oddPop) {
      dblSk.setLevelsArrayAt(level, myLevelsArr[level + 1] - 1); // the current level now contains one item
      myDoubleItemsArr[myLevelsArr[level]] = myDoubleItemsArr[rawBeg];  // namely this leftover guy
    } else {
      dblSk.setLevelsArrayAt(level, myLevelsArr[level + 1]); // the current level is now empty
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
      dblSk.setLevelsArrayAt(lvl, newIndex);
    }
    dblSk.setDoubleItemsArray(myDoubleItemsArr);
  }

  //assumes readOnly = false and UPDATABLE, called from KllDoublesSketch::merge
  static void mergeDoubleImpl(final KllDoublesSketch mySketch,
      final KllDoublesSketch otherDblSk) {
    if (otherDblSk.isEmpty()) { return; }

    //capture my key mutable fields before doing any merging
    final boolean myEmpty = mySketch.isEmpty();
    final double myMin = myEmpty ? Double.NaN : mySketch.getMinItem();
    final double myMax = myEmpty ? Double.NaN : mySketch.getMaxItem();
    final int myMinK = mySketch.getMinK();
    final long finalN = mySketch.getN() + otherDblSk.getN();

    //buffers that are referenced multiple times
    final int otherNumLevels = otherDblSk.getNumLevels();
    final int[] otherLevelsArr = otherDblSk.levelsArr;
    final double[] otherDoubleItemsArr;

    //MERGE: update this sketch with level0 items from the other sketch
    if (otherDblSk.isCompactSingleItem()) {
      updateDouble(mySketch, otherDblSk.getDoubleSingleItem());
      otherDoubleItemsArr = new double[0];
    } else {
      otherDoubleItemsArr = otherDblSk.getDoubleItemsArray();
      for (int i = otherLevelsArr[0]; i < otherLevelsArr[1]; i++) {
       updateDouble(mySketch, otherDoubleItemsArr[i]);
      }
    }

    //After the level 0 update, we capture the intermediate state of my levels and items arrays...
    final int myCurNumLevels = mySketch.getNumLevels();
    final int[] myCurLevelsArr = mySketch.levelsArr;
    final double[] myCurDoubleItemsArr = mySketch.getDoubleItemsArray();

    // create aliases in case there are no higher levels
    int myNewNumLevels = myCurNumLevels;
    int[] myNewLevelsArr = myCurLevelsArr;
    double[] myNewDoubleItemsArr = myCurDoubleItemsArr;

    //merge higher levels if they exist
    if (otherNumLevels > 1  && !otherDblSk.isCompactSingleItem()) {
      final int tmpSpaceNeeded = mySketch.getNumRetained()
          + KllHelper.getNumRetainedAboveLevelZero(otherNumLevels, otherLevelsArr);
      final double[] workbuf = new double[tmpSpaceNeeded];

      final int provisionalNumLevels = max(myCurNumLevels, otherNumLevels);

      final int ub = max(KllHelper.ubOnNumLevels(finalN), provisionalNumLevels);
      final int[] worklevels = new int[ub + 2]; // ub+1 does not work
      final int[] outlevels  = new int[ub + 2];

      populateDoubleWorkArrays(workbuf, worklevels, provisionalNumLevels,
          myCurNumLevels, myCurLevelsArr, myCurDoubleItemsArr,
          otherNumLevels, otherLevelsArr, otherDoubleItemsArr);

      // notice that workbuf is being used as both the input and output
      final int[] result = generalDoublesCompress(mySketch.getK(), mySketch.getM(), provisionalNumLevels,
          workbuf, worklevels, workbuf, outlevels, mySketch.isLevelZeroSorted(), KllSketch.random);
      final int targetItemCount = result[1]; //was finalCapacity. Max size given k, m, numLevels
      final int curItemCount = result[2]; //was finalPop

      // now we need to finalize the results for mySketch

      //THE NEW NUM LEVELS
      myNewNumLevels = result[0];
      assert myNewNumLevels <= ub; // ub may be much bigger

      // THE NEW ITEMS ARRAY
      myNewDoubleItemsArr = (targetItemCount == myCurDoubleItemsArr.length)
          ? myCurDoubleItemsArr
          : new double[targetItemCount];
      final int freeSpaceAtBottom = targetItemCount - curItemCount;

      //shift the new items array create space at bottom
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
      if (mySketch.getWritableMemory() != null) {
        final WritableMemory wmem =
            KllHelper.memorySpaceMgmt(mySketch, myNewLevelsArr.length, myNewDoubleItemsArr.length);
        mySketch.setWritableMemory(wmem);
      }
    } //end of updating levels above level 0

    //Update Preamble:
    mySketch.setN(finalN);
    if (otherDblSk.isEstimationMode()) { //otherwise the merge brings over exact items.
      mySketch.setMinK(min(myMinK, otherDblSk.getMinK()));
    }

    //Update numLevels, levelsArray, items
    mySketch.setNumLevels(myNewNumLevels);
    mySketch.setLevelsArray(myNewLevelsArr);
    mySketch.setDoubleItemsArray(myNewDoubleItemsArr);

    //Update min, max items
    final double otherMin = otherDblSk.getMinItem();
    final double otherMax = otherDblSk.getMaxItem();
    if (myEmpty) {
      mySketch.setMinItem(otherMin);
      mySketch.setMaxItem(otherMax);
    } else {
      mySketch.setMinItem(min(myMin, otherMin));
      mySketch.setMaxItem(max(myMax, otherMax));
    }
    assert KllHelper.sumTheSampleWeights(mySketch.getNumLevels(), mySketch.levelsArr) == mySketch.getN();
  }

  private static void mergeSortedDoubleArrays( //only bufC is modified
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
   * Validation Method. This must be modified to use the validation test
   * @param buf the items array
   * @param start data start
   * @param length items array length
   * @param random instance of Random
   */
  //NOTE For validation Method: Need to modify to run.
  private static void randomlyHalveDownDoubles(final double[] buf, final int start, final int length,
      final Random random) {
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
  //NOTE For validation Method: Need to modify to run.
  private static void randomlyHalveUpDoubles(final double[] buf, final int start, final int length,
      final Random random) {
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

  //Called from KllDoublesSketch::update and merge
  static void updateDouble(final KllDoublesSketch dblSk, final double item) {
    dblSk.updateMinMax(item);
    int freeSpace = dblSk.levelsArr[0];
    assert (freeSpace >= 0);
    if (freeSpace == 0) {
      compressWhileUpdatingSketch(dblSk);
      freeSpace = dblSk.levelsArr[0];
      assert (freeSpace > 0);
    }
    dblSk.incN();
    dblSk.setLevelZeroSorted(false);
    final int nextPos = freeSpace - 1;
    dblSk.setLevelsArrayAt(0, nextPos);
    dblSk.setDoubleItemsArrayAt(nextPos, item);
  }

  //Called from KllDoublesSketch::update with weight
  static void updateDouble(final KllDoublesSketch dblSk, final double item, final long weight) {
    if (weight < dblSk.levelsArr[0]) {
      for (int i = 0; i < (int)weight; i++) { updateDouble(dblSk, item); }
    } else {
      dblSk.updateMinMax(item);
      final KllHeapDoublesSketch tmpSk = new KllHeapDoublesSketch(dblSk.getK(), DEFAULT_M, item, weight);

      dblSk.merge(tmpSk);
    }
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
  //
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

  private static void populateDoubleWorkArrays( //workBuf and workLevels are modified
      final double[] workBuf, final int[] workLevels, final int provisionalNumLevels,
      final int myCurNumLevels, final int[] myCurLevelsArr, final double[] myCurDoubleItemsArr,
      final int otherNumLevels, final int[] otherLevelsArr, final double[] otherDoubleItemsArr) {

    workLevels[0] = 0;

    // Note: the level zero data from "other" was already inserted into "self".
    // This copies into workbuf.
    final int selfPopZero = KllHelper.currentLevelSizeItems(0, myCurNumLevels, myCurLevelsArr);
    System.arraycopy(myCurDoubleItemsArr, myCurLevelsArr[0], workBuf, workLevels[0], selfPopZero);
    workLevels[1] = workLevels[0] + selfPopZero;

    for (int lvl = 1; lvl < provisionalNumLevels; lvl++) {
      final int selfPop = KllHelper.currentLevelSizeItems(lvl, myCurNumLevels, myCurLevelsArr);
      final int otherPop = KllHelper.currentLevelSizeItems(lvl, otherNumLevels, otherLevelsArr);
      workLevels[lvl + 1] = workLevels[lvl] + selfPop + otherPop;
      assert selfPop >= 0 && otherPop >= 0;
      if (selfPop == 0 && otherPop == 0) { continue; }
      else if (selfPop > 0 && otherPop == 0) {
        System.arraycopy(myCurDoubleItemsArr, myCurLevelsArr[lvl], workBuf, workLevels[lvl], selfPop);
      }
      else if (selfPop == 0 && otherPop > 0) {
        System.arraycopy(otherDoubleItemsArr, otherLevelsArr[lvl], workBuf, workLevels[lvl], otherPop);
      }
      else if (selfPop > 0 && otherPop > 0) {
        mergeSortedDoubleArrays( //only workbuf is modified
            myCurDoubleItemsArr, myCurLevelsArr[lvl], selfPop,
            otherDoubleItemsArr, otherLevelsArr[lvl], otherPop,
            workBuf, workLevels[lvl]);
      }
    }
  }

  /*
   * Validation Method.
   * The following must be enabled for use with the KllDoublesValidationTest,
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
