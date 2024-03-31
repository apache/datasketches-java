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
import static org.apache.datasketches.common.ByteArrayUtil.putDoubleLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;

import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SuppressFBWarnings;
import org.apache.datasketches.kll.KllDirectDoublesSketch.KllDirectCompactDoublesSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.DoublesSketchSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesDoublesAPI;
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;

/**
 * This variation of the KllSketch implements primitive doubles.
 *
 * @see org.apache.datasketches.kll.KllSketch
 */
public abstract class KllDoublesSketch extends KllSketch implements QuantilesDoublesAPI {
  private DoublesSketchSortedView doublesSV = null;
  final static int ITEM_BYTES = Double.BYTES;

  KllDoublesSketch(
      final SketchStructure sketchStructure) {
    super(SketchType.DOUBLES_SKETCH, sketchStructure);
  }

  //Factories for new heap instances.

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @return new KllDoublesSketch on the Java heap.
   */
  public static KllDoublesSketch newHeapInstance() {
    return newHeapInstance(DEFAULT_K);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be between 8, inclusive, and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @return new KllDoublesSketch on the Java heap.
   */
  public static KllDoublesSketch newHeapInstance(final int k) {
    return new KllHeapDoublesSketch(k, DEFAULT_M);
  }

  //Factories for new direct instances.

  /**
   * Create a new direct updatable instance of this sketch with the default <em>k</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllDoublesSketch newDirectInstance(
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    return newDirectInstance(DEFAULT_K, dstMem, memReqSvr);
  }

  /**
   * Create a new direct updatable instance of this sketch with a given <em>k</em>.
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllDoublesSketch newDirectInstance(
      final int k,
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(dstMem, "Parameter 'dstMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    return KllDirectDoublesSketch.newDirectUpdatableInstance(k, DEFAULT_M, dstMem, memReqSvr);
  }

  //Factory to create an heap instance from a Memory image

  /**
   * Factory heapify takes a compact sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a compact Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static KllDoublesSketch heapify(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    return KllHeapDoublesSketch.heapifyImpl(srcMem);
  }

  //Factory to wrap a Read-Only Memory

  /**
   * Wrap a sketch around the given read only compact source Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem the read only source Memory
   * @return instance of this sketch
   */
  public static KllDoublesSketch wrap(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, DOUBLES_SKETCH, null);
    if (memVal.sketchStructure == UPDATABLE) {
      final MemoryRequestServer memReqSvr = new DefaultMemoryRequestServer(); //dummy
      return new KllDirectDoublesSketch(memVal.sketchStructure, (WritableMemory)srcMem, memReqSvr, memVal);
    } else {
      return new KllDirectCompactDoublesSketch(memVal.sketchStructure, srcMem, memVal);
    }
  }

  //Factory to wrap a WritableMemory image

  /**
   * Wrap a sketch around the given source Writable Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem a WritableMemory that contains data.
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return instance of this sketch
   */
  public static KllDoublesSketch writableWrap(
      final WritableMemory srcMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, DOUBLES_SKETCH);
    if (memVal.sketchStructure == UPDATABLE) {
      return new KllDirectDoublesSketch(UPDATABLE, srcMem, memReqSvr, memVal);
    } else {
      return new KllDirectCompactDoublesSketch(memVal.sketchStructure, srcMem, memVal);
    }
  }

  //END of Constructors

  @Override
  public double[] getCDF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public double[] getPMF(final double[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public double getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getQuantile(rank, searchCrit);
  }

  @Override
  public double[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = ranks.length;
    final double[] quantiles = new double[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = doublesSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public double getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public double getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public double getRank(final double quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return doublesSV.getRank(quantile, searchCrit);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankLowerBound(final double rank) {
    return max(0.0, rank - KllHelper.getNormalizedRankError(getMinK(), false));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankUpperBound(final double rank) {
    return min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false));
  }

  @Override
  public double[] getRanks(final double[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = doublesSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "OK in this case.")
  public DoublesSketchSortedView getSortedView() {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return doublesSV;
  }

  @Override
  public QuantilesDoublesSketchIterator iterator() {
    return new KllDoublesSketchIterator(
        getDoubleItemsArray(), getLevelsArray(SketchStructure.UPDATABLE), getNumLevels());
  }

  @Override
  public final void merge(final KllSketch other) {
    if (readOnly || sketchStructure != UPDATABLE) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (this == other) { throw new SketchesArgumentException(SELF_MERGE_MSG); }
    final KllDoublesSketch othDblSk = (KllDoublesSketch)other;
    if (othDblSk.isEmpty()) { return; }
    KllDoublesHelper.mergeDoubleImpl(this, othDblSk);
    doublesSV = null;
  }

  /**
   * {@inheritDoc}
   * <p>The parameter <i>k</i> will not change.</p>
   */
  @Override
  public final void reset() {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelZeroSorted(false);
    setLevelsArray(new int[] {k, k});
    setMinItem(Double.NaN);
    setMaxItem(Double.NaN);
    setDoubleItemsArray(new double[k]);
    doublesSV = null;
  }

  @Override
  public byte[] toByteArray() {
    return KllHelper.toByteArray(this, false);
  }

  @Override
  public String toString(final boolean withLevels, final boolean withLevelsAndItems) {
    KllSketch sketch = this;
    if (withLevelsAndItems && sketchStructure != UPDATABLE) {
      final Memory mem = getWritableMemory();
      assert mem != null;
      sketch = KllDoublesSketch.heapify(getWritableMemory());
    }
    return KllHelper.toStringImpl(sketch, withLevels, withLevelsAndItems, getSerDe());
  }

  @Override
  public void update(final double item) {
    if (Double.isNaN(item)) { return; } //ignore
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    KllDoublesHelper.updateDouble(this, item);
    doublesSV = null;
  }

  /**
   * Weighted update. Updates this sketch with the given item the number of times specified by the given integer weight.
   * @param item the item to be repeated. NaNs are ignored.
   * @param weight the number of times the update of item is to be repeated. It must be &ge; one.
   */
  public void update(final double item, final long weight) {
    if (Double.isNaN(item)) { return; } //ignore
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (weight < 1L) { throw new SketchesArgumentException("Weight is less than one."); }
    if (weight == 1L) { KllDoublesHelper.updateDouble(this, item); }
    else { KllDoublesHelper.updateDouble(this, item, weight); }
    doublesSV = null;
  }

  // VECTOR UPDATE

  /**
   * Vector update. Updates this sketch with the given array (vector) of items, starting at the items
   * offset for a length number of items. This is not supported for direct sketches.
   * @param items the vector of items
   * @param offset the starting index of the items[] array
   * @param length the number of items
   */
//  public void update(final double[] items, final int offset, final int length) {
//    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
//    if (length == 0) { return; }
//    boolean hasNaN = false;
//    boolean allNaNs = true;
//    for (int i = 0; i < length; i++) {
//      final boolean isNaN = Double.isNaN(items[offset + i]);
//      hasNaN |= isNaN;
//      allNaNs &= isNaN;
//    }
//    if (allNaNs) { return; }
//    else if (!hasNaN) { // fast path
//      KllDoublesHelper.updateDouble(this, items, offset, length);
//    } else {
//      for (int i = 0; i < length; i++) {
//        final double v = items[offset + i];
//        if (!Double.isNaN(v)) {
//          KllDoublesHelper.updateDouble(this, v);
//        }
//      }
//    }
//    doublesSV = null;
//  }

  public void update(final double[] items, final int offset, final int length) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (length == 0) { return; }
    if (!hasNaN(items, offset, length)) {
      KllDoublesHelper.updateDouble(this, items, offset, length); //fast path
    }
    else { //has at least one NaN
      boolean oneUpdate = false;
      final int end = offset + length;
      for (int i = offset; i < end; i++) {
        final double v = items[i];
        if (!Double.isNaN(v)) {
          KllDoublesHelper.updateDouble(this, v); //normal path
          oneUpdate = true;
        }
      }
      if (oneUpdate) { doublesSV = null; }
    }
  }

  //restricted

  // this returns on the first detected NaN.
  private final static boolean hasNaN(final double[] items, final int offset, final int length) {
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      if (Double.isNaN(items[i])) { return true; } else { continue; }
    }
    return false;
  }

  // END VECTOR UPDATE

  /**
   * @return full size of internal items array including empty space at bottom.
   */
  abstract double[] getDoubleItemsArray();

  /**
   * @return items array of retained items.
   */
  abstract double[] getDoubleRetainedItemsArray();

  abstract double getDoubleSingleItem();

  @Override
  abstract byte[] getMinMaxByteArr();

  @Override
  int getMinMaxSizeBytes() {
    return Double.BYTES * 2;
  }

  @Override
  abstract byte[] getRetainedItemsByteArr();

  @Override
  int getRetainedItemsSizeBytes() {
    return getNumRetained() * Double.BYTES;
  }

  @Override
  ArrayOfItemsSerDe<?> getSerDe() { return null; }

  @Override
  final byte[] getSingleItemByteArr() {
    final byte[] bytes = new byte[ITEM_BYTES];
    putDoubleLE(bytes, 0, getDoubleSingleItem());
    return bytes;
  }

  @Override
  int getSingleItemSizeBytes() {
    return Double.BYTES;
  }

  @Override
  abstract byte[] getTotalItemsByteArr();

  @Override
  int getTotalItemsNumBytes() {
    return levelsArr[getNumLevels()] * Double.BYTES;
  }

  abstract void setDoubleItemsArray(double[] doubleItems);

  abstract void setDoubleItemsArrayAt(int index, double item);

  abstract void setDoubleItemsArrayAt(int index, double[] items, int offset, int length);

  abstract void setMaxItem(double item);

  abstract void setMinItem(double item);

  final void updateMinMax(final double item) {
    if (isEmpty()) {
      setMinItem(item);
      setMaxItem(item);
    } else {
      setMinItem(min(getMinItem(), item));
      setMaxItem(max(getMaxItem(), item));
    }
  }

  final void updateMinMax(final double[] items, final int offset, final int length) {
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      updateMinMax(items[i]);
    }
  }

  //************SORTED VIEW****************************

  private final DoublesSketchSortedView refreshSortedView() {
    if (doublesSV == null) {
      final CreateSortedView csv = new CreateSortedView();
      doublesSV = csv.getSV();
    }
    return doublesSV;
  }

  private final class CreateSortedView {
    double[] quantiles;
    long[] cumWeights;

    DoublesSketchSortedView getSV() {
      if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
      if (getN() == 0) { throw new SketchesArgumentException(EMPTY_MSG); }
      final double[] srcQuantiles = getDoubleItemsArray();
      final int[] srcLevels = levelsArr;
      final int srcNumLevels = getNumLevels();

      if (!isLevelZeroSorted()) {
        Arrays.sort(srcQuantiles, srcLevels[0], srcLevels[1]);
        if (!hasMemory()) { setLevelZeroSorted(true); }
        //we don't sort level0 in Memory, only our copy.
      }
      final int numQuantiles = getNumRetained();
      quantiles = new double[numQuantiles];
      cumWeights = new long[numQuantiles];
      populateFromSketch(srcQuantiles, srcLevels, srcNumLevels, numQuantiles);
      return new DoublesSketchSortedView(
          quantiles, cumWeights, getN(), getMaxItem(), getMinItem());
    }

    private void populateFromSketch(final double[] srcQuantiles, final int[] srcLevels,
      final int srcNumLevels, final int numItems) {
      final int[] myLevels = new int[srcNumLevels + 1];
      final int offset = srcLevels[0];
      System.arraycopy(srcQuantiles, offset, quantiles, 0, numItems);
      int srcLevel = 0;
      int dstLevel = 0;
      long weight = 1;
      while (srcLevel < srcNumLevels) {
        final int fromIndex = srcLevels[srcLevel] - offset;
        final int toIndex = srcLevels[srcLevel + 1] - offset; // exclusive
        if (fromIndex < toIndex) { // if equal, skip empty level
          Arrays.fill(cumWeights, fromIndex, toIndex, weight);
          myLevels[dstLevel] = fromIndex;
          myLevels[dstLevel + 1] = toIndex;
          dstLevel++;
        }
        srcLevel++;
        weight *= 2;
      }
      final int numLevels = dstLevel;
      blockyTandemMergeSort(quantiles, cumWeights, myLevels, numLevels); //create unit weights
      KllHelper.convertToCumulative(cumWeights);
    }
  } //End of class CreateSortedView

  private static void blockyTandemMergeSort(final double[] quantiles, final long[] weights,
      final int[] levels, final int numLevels) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final double[] quantilesTmp = Arrays.copyOf(quantiles, quantiles.length);
    final long[] weightsTmp = Arrays.copyOf(weights, quantiles.length); // don't need the extra one

    blockyTandemMergeSortRecursion(quantilesTmp, weightsTmp, quantiles, weights, levels, 0, numLevels);
  }

  private static void blockyTandemMergeSortRecursion(
      final double[] quantilesSrc, final long[] weightsSrc,
      final double[] quantilesDst, final long[] weightsDst,
      final int[] levels, final int startingLevel, final int numLevels) {
    if (numLevels == 1) { return; }
    final int numLevels1 = numLevels / 2;
    final int numLevels2 = numLevels - numLevels1;
    assert numLevels1 >= 1;
    assert numLevels2 >= numLevels1;
    final int startingLevel1 = startingLevel;
    final int startingLevel2 = startingLevel + numLevels1;
    // swap roles of src and dst
    blockyTandemMergeSortRecursion(
        quantilesDst, weightsDst,
        quantilesSrc, weightsSrc,
        levels, startingLevel1, numLevels1);
    blockyTandemMergeSortRecursion(
        quantilesDst, weightsDst,
        quantilesSrc, weightsSrc,
        levels, startingLevel2, numLevels2);
    tandemMerge(
        quantilesSrc, weightsSrc,
        quantilesDst, weightsDst,
        levels,
        startingLevel1, numLevels1,
        startingLevel2, numLevels2);
  }

  private static void tandemMerge(
      final double[] quantilesSrc, final long[] weightsSrc,
      final double[] quantilesDst, final long[] weightsDst,
      final int[] levelStarts,
      final int startingLevel1, final int numLevels1,
      final int startingLevel2, final int numLevels2) {
    final int fromIndex1 = levelStarts[startingLevel1];
    final int toIndex1 = levelStarts[startingLevel1 + numLevels1]; // exclusive
    final int fromIndex2 = levelStarts[startingLevel2];
    final int toIndex2 = levelStarts[startingLevel2 + numLevels2]; // exclusive
    int iSrc1 = fromIndex1;
    int iSrc2 = fromIndex2;
    int iDst = fromIndex1;

    while (iSrc1 < toIndex1 && iSrc2 < toIndex2) {
      if (quantilesSrc[iSrc1] < quantilesSrc[iSrc2]) {
        quantilesDst[iDst] = quantilesSrc[iSrc1];
        weightsDst[iDst] = weightsSrc[iSrc1];
        iSrc1++;
      } else {
        quantilesDst[iDst] = quantilesSrc[iSrc2];
        weightsDst[iDst] = weightsSrc[iSrc2];
        iSrc2++;
      }
      iDst++;
    }
    if (iSrc1 < toIndex1) {
      System.arraycopy(quantilesSrc, iSrc1, quantilesDst, iDst, toIndex1 - iSrc1);
      System.arraycopy(weightsSrc, iSrc1, weightsDst, iDst, toIndex1 - iSrc1);
    } else if (iSrc2 < toIndex2) {
      System.arraycopy(quantilesSrc, iSrc2, quantilesDst, iDst, toIndex2 - iSrc2);
      System.arraycopy(weightsSrc, iSrc2, weightsDst, iDst, toIndex2 - iSrc2);
    }
  }

}
