package org.apache.datasketches.kll;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.common.Util.isOdd;
import static org.apache.datasketches.kll.KllHelper.findLevelToCompact;

import java.util.Arrays;
import java.util.Random;

/**
 * Static methods to support KllItemsSketch
 * @author Lee Rhodes
 */
final class KllItemsHelper {

  /**
   * The following code is only valid in the special case of exactly reaching capacity while updating.
   * It cannot be used while merging, while reducing k, or anything else.
   * @param itmSk the current KllItemsSketch
   */
  static <T> void compressWhileUpdatingSketch(final KllItemsSketch<T> itmSk) {
    final int level =
        findLevelToCompact(itmSk.getK(), itmSk.getM(), itmSk.getNumLevels(), itmSk.getLevelsArray());
    if (level == itmSk.getNumLevels() - 1) {
      //The level to compact is the top level, thus we need to add a level.
      //Be aware that this operation grows the items array,
      //shifts the items data and the level boundaries of the data,
      //and grows the levels array and increments numLevels_.
      KllHelper.addEmptyTopLevelToCompletelyFullSketch(itmSk);
    }
    //after this point, the levelsArray will not be expanded, only modified.
    final int[] myLevelsArr = itmSk.getLevelsArray();
    final int rawBeg = myLevelsArr[level];
    final int rawEnd = myLevelsArr[level + 1];
    // +2 is OK because we already added a new top level if necessary
    final int popAbove = myLevelsArr[level + 2] - rawEnd;
    final int rawPop = rawEnd - rawBeg;
    final boolean oddPop = isOdd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    //the following is specific to Floats
    final float[] myItemsItemsArr = itmSk.getItemsArray();
    if (level == 0) { // level zero might not be sorted, so we must sort it if we wish to compact it
      Arrays.sort(myItemsItemsArr, adjBeg, adjBeg + adjPop);
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
    fltSk.setLevelsArrayAt(level + 1, newIndex);

    if (oddPop) {
      fltSk.setLevelsArrayAt(level, myLevelsArr[level + 1] - 1); // the current level now contains one item
      myFloatItemsArr[myLevelsArr[level]] = myFloatItemsArr[rawBeg];  // namely this leftover guy
    } else {
      fltSk.setLevelsArrayAt(level, myLevelsArr[level + 1]); // the current level is now empty
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
      fltSk.setLevelsArrayAt(lvl, newIndex);
    }
    fltSk.setFloatItemsArray(myFloatItemsArr);
  }

  static <T> void mergeFloatImpl(final KllItemsSketch<T> mySketch, final KllItemsSketch<T> otherFltSk) {

  }

  static <T> void mergeSortedItemArrays(
      final T[] bufA, final int startA, final int lenA,
      final T[] bufB, final int startB, final int lenB,
      final T[] bufC, final int startC) {

  }

  static <T> void randomlyHalveDownItems(final T[] buf, final int start, final int length, final Random random) {

  }

  static <T> void randomlyHalveUpItems(final T[] buf, final int start, final int length, final Random random) {

  }

  static <T> void updateItem(final KllItemsSketch<T> itmSk, final T item) {

  }

  private static <T> int[] generalItemsCompress(
      final int k,
      final int m,
      final int numLevelsIn,
      final T[] inBuf,
      final int[] inLevels,
      final T[] outBuf,
      final int[] outLevels,
      final boolean isLevelZeroSorted,
      final Random random) {

    return null;
  }

  private static <T> void populateItemWorkArrays(
      final T[] workbuf, final int[] worklevels, final int provisionalNumLevels,
      final int myCurNumLevels, final int[] myCurLevelsArr, final T[] myCurFloatItemsArr,
      final int otherNumLevels, final int[] otherLevelsArr, final T[] otherFloatItemsArr) {

  }

  private static <T> T resolveItemMaxItem(final T myMax, final T otherMax) {
    if (myMax == null) && otherMax == null) { return null; }
    if (myMax == null) { return otherMax; }
    if (otherMax == null) { return myMax; }
    return max(myMax, otherMax);
  }

  private static <T> T resolveItemMinItem(final T myMin, final T otherMin) {
    if (myMin == null && otherMin == null) { return null; }
    if (myMin == null) { return otherMin; }
    if (otherMin == null) { return myMin; }
    return min(myMin, otherMin);
  }

}
