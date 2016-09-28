/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Arrays;

public class DoublesUpdateImpl {

  static void growBaseBuffer(final DoublesSketch sketch) { //n has not been incremented yet
    final double[] baseBuffer = sketch.getCombinedBuffer(); //in this case it is just the BB
    final int oldSize = sketch.getCombinedBufferItemCapacity(); //current array size
    final int k = sketch.getK();
    assert oldSize < 2 * k;
    final int newSize = Math.max(Math.min(2 * k, 2 * oldSize), 1);
    sketch.putCombinedBufferItemCapacity(newSize);
    sketch.putCombinedBuffer(Arrays.copyOf(baseBuffer, newSize));
  }
  
  /**
   * Called when the base buffer has just acquired 2*k elements.
   * @param sketch the given quantiles sketch
   */
  //important: n_ was incremented by update before we got here
  static void processFullBaseBuffer(final HeapDoublesSketch sketch) {
    final int bbCount = sketch.getBaseBufferCount();
    final int k = sketch.getK();
    final long newN = sketch.getN();
    assert bbCount == 2 * k; // internal consistency check

    // make sure there will be enough levels for the propagation
    maybeGrowLevels(newN, sketch);

    // notice that this is acquired after the possible resizing
    final double[] baseBuffer = sketch.getCombinedBuffer(); 

    Arrays.sort(baseBuffer, 0, bbCount); //sort the BB
    inPlacePropagateCarry(
        0,           //starting level
        null,        //sizeKbuf,   not needed here
        0,           //sizeKStart, not needed here
        baseBuffer,  //size2Kbuf, the base buffer = the Combined Buffer 
        0,           //size2KStart
        true,        //doUpdateVersion 
        sketch);     //the sketch
    sketch.baseBufferCount_ = 0;
    assert newN / (2 * k) == sketch.getBitPattern(); // internal consistency check
  }

  //important: newN might not equal n_
  // This only increases the size and does not touch or move any data.
  static void maybeGrowLevels(final long newN, final HeapDoublesSketch sketch) {
    final int k = sketch.getK();
    final int numLevelsNeeded = Util.computeNumLevelsNeeded(k, newN);
    if (numLevelsNeeded == 0) {
      // don't need any levels yet, and might have small base buffer; this can happen during a merge
      return; 
    }
    // from here on we need a full-size base buffer and at least one level
    assert newN >= 2L * k;
    assert numLevelsNeeded > 0; 
    final int spaceNeeded = (2 + numLevelsNeeded) * k;
    if (spaceNeeded <= sketch.getCombinedBufferItemCapacity()) {
      return;
    }
    // copies base buffer plus old levels
    sketch.combinedBuffer_ = Arrays.copyOf(sketch.getCombinedBuffer(), spaceNeeded); 
    sketch.combinedBufferItemCapacity_ = spaceNeeded;
  }
  
  static void inPlacePropagateCarry(
      final int startingLevel,
      final double[] sizeKBuf, final int sizeKStart,
      final double[] size2KBuf, final int size2KStart,
      final boolean doUpdateVersion, final HeapDoublesSketch sketch
    ) { // else doMergeIntoVersion
    final double[] levelsArr = sketch.getCombinedBuffer();
    final int k = sketch.getK();
    final long bitPattern = sketch.bitPattern_; //the one prior to the last increment of n_
    final int endingLevel = Util.positionOfLowestZeroBitStartingAt(bitPattern, startingLevel);
  
    if (doUpdateVersion) { // update version of computation
      // its is okay for sizeKbuf to be null in this case
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    } else { // mergeInto version of computation
      System.arraycopy(
          sizeKBuf, sizeKStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    }
  
    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      mergeTwoSizeKBuffers(
          levelsArr, (2 + lvl) * k,
          levelsArr, (2 + endingLevel) * k,
          size2KBuf, size2KStart,
          k);
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    sketch.bitPattern_ = bitPattern + (1L << startingLevel);
  }
  
  private static void zipSize2KBuffer(
      final double[] bufA, final int startA, // input
      final double[] bufC, final int startC, // output
      final int k) {
    final int randomOffset = DoublesSketch.rand.nextBoolean() ? 1 : 0;
    final int limC = startC + k;
    for (int a = startA + randomOffset, c = startC; c < limC; a += 2, c++) {
      bufC[c] = bufA[a];
    }
  }
  
  private static void mergeTwoSizeKBuffers(
      final double[] keySrc1, final int arrStart1,
      final double[] keySrc2, final int arrStart2,
      final double[] keyDst,  final int arrStart3,
      final int k) {
    final int arrStop1 = arrStart1 + k;
    final int arrStop2 = arrStart2 + k;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc2[i2] < keySrc1[i1]) { 
        keyDst[i3++] = keySrc2[i2++];
      } else { 
        keyDst[i3++] = keySrc1[i1++];
      } 
    }
  
    if (i1 < arrStop1) {
      System.arraycopy(keySrc1, i1, keyDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      System.arraycopy(keySrc1, i2, keyDst, i3, arrStop2 - i2);
    }
  }
  
}
