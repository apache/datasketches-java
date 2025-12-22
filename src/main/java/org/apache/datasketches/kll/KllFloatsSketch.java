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
import static org.apache.datasketches.common.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_FLOATS_SKETCH;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.FloatsSketchSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesFloatsAPI;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;

/**
 * This variation of the KllSketch implements primitive floats.
 *
 * @see org.apache.datasketches.kll.KllSketch
 */
public abstract class KllFloatsSketch extends KllSketch implements QuantilesFloatsAPI {
  private FloatsSketchSortedView floatsSV = null;
  final static int ITEM_BYTES = Float.BYTES;

  /**
   * Constructor for on-heap.
   * @param sketchStructure the current sketch structure
   */
  KllFloatsSketch(final SketchStructure sketchStructure) {
    super(SketchType.KLL_FLOATS_SKETCH, sketchStructure);
  }

  /**
   * Constructor for MemorySegment based sketch
   * @param segVal MemorySegment validator.
   */
  KllFloatsSketch(final KllMemorySegmentValidate segVal) {
    super(segVal);
  }

  //Factories for new heap instances.

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @return new KllFloatsSketch on the Java heap.
   */
  public static KllFloatsSketch newHeapInstance() {
    return newHeapInstance(DEFAULT_K);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be between 8, inclusive, and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @return new KllFloatsSketch on the Java heap.
   */
  public static KllFloatsSketch newHeapInstance(final int k) {
    return new KllHeapFloatsSketch(k, DEFAULT_M);
  }

  //Factories for new direct instances.

  /**
   * Create a new direct updatable instance of this sketch with the default <em>k</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param dstSeg the given destination MemorySegment object for use by the sketch
   * @return a new direct instance of this sketch
   */
  public static KllFloatsSketch newDirectInstance(final MemorySegment dstSeg) {
    return newDirectInstance(DEFAULT_K, dstSeg, null);
  }

  /**
   * Create a new direct updatable instance of this sketch with a given <em>k</em>.
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param dstSeg the given destination MemorySegment object for use by the sketch
   * @param mSegReq the callback for the sketch to request a larger MemorySegment. It may be null.
   * @return a new direct instance of this sketch
   */
  public static KllFloatsSketch newDirectInstance(
      final int k,
      final MemorySegment dstSeg,
      final MemorySegmentRequest mSegReq) {
    Objects.requireNonNull(dstSeg, "Parameter 'dstSeg' must not be null");
    return KllDirectFloatsSketch.newDirectUpdatableInstance(k, DEFAULT_M, dstSeg, mSegReq);
  }

  //Factory to create an heap instance from a MemorySegment image

  /**
   * Factory heapify takes a compact sketch image in MemorySegment and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source MemorySegment.
   * @param srcSeg a compact MemorySegment image of a sketch serialized by this sketch.
   * @return a heap-based sketch based on the given MemorySegment.
   */
  public static KllFloatsSketch heapify(final MemorySegment srcSeg) {
    Objects.requireNonNull(srcSeg, "Parameter 'srcSeg' must not be null");
    return KllHeapFloatsSketch.heapifyImpl(srcSeg);
  }

  //Factories to wrap a MemorySegment image

  /**
   * Wrap a sketch around the given source MemorySegment containing sketch data that originated from this sketch.
   *
   * <p>The given MemorySegment must be writable and it must contain a <i>KllFloatsSketch</i> in updatable form.
   * The sketch will be updated and managed totally within the MemorySegment. If the given source
   * MemorySegment is created off-heap, then all the management of the sketch's internal data will be off-heap as well.</p>
   *
   * <p><b>NOTE:</b>If during updating of the sketch the sketch requires more capacity than the given size of the MemorySegment, the sketch
   * will request more capacity using the {@link MemorySegmentRequest MemorySegmentRequest} interface. The default of this interface will
   * return a new MemorySegment on the heap.</p>
   *
   * @param srcSeg a MemorySegment that contains sketch data.
   * @return an instance of this sketch that wraps the given MemorySegment.
   */
  public static KllFloatsSketch wrap(final MemorySegment srcSeg) {
    return wrap(srcSeg, null);
  }

  /**
   * Wrap a sketch around the given source MemorySegment containing sketch data that originated from this sketch and including an
   * optional, user defined {@link MemorySegmentRequest MemorySegmentRequest}.
   *
   * <p>The given MemorySegment must be writable and it must contain a <i>KllFloatsSketch</i> in updatable form.
   * The sketch will be updated and managed totally within the MemorySegment. If the given source
   * MemorySegment is created off-heap, then all the management of the sketch's internal data will be off-heap as well.</p>
   *
   * <p><b>NOTE:</b>If during updating of the sketch the sketch requires more capacity than the given size of the MemorySegment, the sketch
   * will request more capacity using the {@link MemorySegmentRequest MemorySegmentRequest} interface. The default of this interface will
   * return a new MemorySegment on the heap. It is up to the user to optionally extend this interface if more flexible
   * handling of requests for more capacity is required.</p>
   *
   * @param srcSeg a MemorySegment that contains sketch data.
   * @param mSegReq the MemorySegmentRequest used if the given MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return an instance of this sketch that wraps the given MemorySegment.
   */
  public static KllFloatsSketch wrap(final MemorySegment srcSeg, final MemorySegmentRequest mSegReq) {
    Objects.requireNonNull(srcSeg, "Parameter 'srcSeg' must not be null");
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(srcSeg, KLL_FLOATS_SKETCH);
    return new KllDirectFloatsSketch(srcSeg, segVal, mSegReq);
  }

  //END of Constructors

  @Override
  public double[] getCDF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return floatsSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public double[] getPMF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return floatsSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public float getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return floatsSV.getQuantile(rank, searchCrit);
  }

  @Override
  public float[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = ranks.length;
    final float[] quantiles = new float[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = floatsSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public float getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public float getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public double getRank(final float quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return floatsSV.getRank(quantile, searchCrit);
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
  public double[] getRanks(final float[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = floatsSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  public QuantilesFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(
        getFloatItemsArray(), getLevelsArray(SketchStructure.UPDATABLE), getNumLevels());
  }

  @Override
  public final void merge(final KllSketch other, final Random random) {
    if (readOnly || (sketchStructure != UPDATABLE)) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (this == other) { throw new SketchesArgumentException(SELF_MERGE_MSG); }
    final KllFloatsSketch othFltSk = (KllFloatsSketch)other;
    if (othFltSk.isEmpty()) { return; }
    KllFloatsHelper.mergeFloatImpl(this, othFltSk, random);
    floatsSV = null;
  }

  /**
   * {@inheritDoc}
   *
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
    setMinItem(Float.NaN);
    setMaxItem(Float.NaN);
    setFloatItemsArray(new float[k]);
    floatsSV = null;
  }

  @Override
  public byte[] toByteArray() {
    return KllHelper.toByteArray(this, false);
  }

  @Override
  public String toString(final boolean withLevels, final boolean withLevelsAndItems) {
    KllSketch sketch = this;
    if (withLevelsAndItems && (sketchStructure != UPDATABLE)) {
      final MemorySegment seg = getMemorySegment();
      assert seg != null;
      sketch = KllFloatsSketch.heapify(getMemorySegment());
    }
    return KllHelper.toStringImpl(sketch, withLevels, withLevelsAndItems);
  }

  //SINGLE UPDATE

  @Override
  public void update(final float item) {
    if (Float.isNaN(item)) { return; } //ignore
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    updateFloat(this, item, random);
    floatsSV = null;
  }

  //Also Called from KllFloatsHelper::merge
  static void updateFloat(final KllFloatsSketch fltSk, final float item, final Random random) {
    fltSk.updateMinMax(item);
    int freeSpace = fltSk.levelsArr[0];
    assert (freeSpace >= 0);
    if (freeSpace == 0) {
      KllFloatsHelper.compressWhileUpdatingSketch(fltSk, random);
      freeSpace = fltSk.levelsArr[0];
      assert (freeSpace > 0);
    }
    fltSk.incN(1);
    fltSk.setLevelZeroSorted(false);
    final int nextPos = freeSpace - 1;
    fltSk.setLevelsArrayAt(0, nextPos);
    fltSk.setFloatItemsArrayAt(nextPos, item);
  }

  /**
   * Single update of min and max
   * @param item the source item, it must not be a NaN.
   */
  final void updateMinMax(final float item) {
    if (isEmpty() || Float.isNaN(getMinItemInternal())) {
      setMinItem(item);
      setMaxItem(item);
    } else {
      setMinItem(min(getMinItemInternal(), item));
      setMaxItem(max(getMaxItemInternal(), item));
    }
  }

  //WEIGHTED UPDATE

  /**
   * Weighted update. Updates this sketch with the given item the number of times specified by the given integer weight.
   * @param item the item to be repeated. NaNs are ignored.
   * @param weight the number of times the update of item is to be repeated. It must be &ge; one.
   */
  public void update(final float item, final long weight) {
    if (Float.isNaN(item)) { return; } //ignore
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (weight < 1L) { throw new SketchesArgumentException("Weight is less than one."); }
    if (weight == 1L) { updateFloat(this, item, random); }
    else if (weight < levelsArr[0]) {
      for (int i = 0; i < (int)weight; i++) { updateFloat(this, item, random); }
    } else {
      final KllHeapFloatsSketch tmpSk = new KllHeapFloatsSketch(getK(), DEFAULT_M, item, weight);
      merge(tmpSk);
    }
    floatsSV = null;
  }

  // VECTOR UPDATE

  /**
   * Vector update. Updates this sketch with the given array (vector) of items, starting at the items
   * offset for a length number of items. This is not supported for direct sketches.
   * <p>Note: a single occurrence of a NaN in the array will force this method to use the conventional update path
   * rather than the fast update path.</p>
   * @param items the vector of items
   * @param offset the starting index of the items[] array
   * @param length the number of items
   */
  public void update(final float[] items, final int offset, final int length) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (length == 0) { return; }
    if (!hasNaN(items, offset, length)) {
      updateFloat(items, offset, length); //fast path
      floatsSV = null;
      return;
    }
    //has at least one NaN
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      final float v = items[i];
      if (!Float.isNaN(v)) {
        updateFloat(this, v, random); //normal path
        floatsSV = null;
      }
    }
  }

  // No NaNs are allowed at this point
  private void updateFloat(final float[] srcItems, final int srcOffset, final int length) {
    if (isEmpty() || Float.isNaN(getMinItemInternal())) {
      setMinItem(srcItems[srcOffset]); //initialize with a real value
      setMaxItem(srcItems[srcOffset]);
    }

    int count = 0;
    while (count < length) {
      if (levelsArr[0] == 0) {
        KllFloatsHelper.compressWhileUpdatingSketch(this, random);
      }
      final int spaceNeeded = length - count;
      final int freeSpace = levelsArr[0];
      assert (freeSpace > 0);
      final int numItemsToCopy = min(spaceNeeded, freeSpace);
      final int dstOffset = freeSpace - numItemsToCopy;
      final int localSrcOffset = srcOffset + count;
      setFloatItemsArrayAt(dstOffset, srcItems, localSrcOffset, numItemsToCopy);
      updateMinMax(srcItems, localSrcOffset, numItemsToCopy);
      count += numItemsToCopy;
      incN(numItemsToCopy);
      setLevelsArrayAt(0, dstOffset);
    }
    setLevelZeroSorted(false);
  }

  /**
   * Vector update of min and max.
   * @param srcItems the input source array of values, no NaNs allowed.
   * @param srcOffset the starting offset in srcItems
   * @param length the number of items to update min and max
   */
  private void updateMinMax(final float[] srcItems, final int srcOffset, final int length) {
    final int end = srcOffset + length;
    for (int i = srcOffset; i < end; i++) {
      setMinItem(min(getMinItemInternal(), srcItems[i]));
      setMaxItem(max(getMaxItemInternal(), srcItems[i]));
    }
  }

  // this returns on the first detected NaN.
  private static boolean hasNaN(final float[] items, final int offset, final int length) {
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      if (Float.isNaN(items[i])) { return true; }
    }
    return false;
  }

  // END ALL UPDATE METHODS

  /**
   * @return full size of internal items array including empty space at bottom.
   */
  abstract float[] getFloatItemsArray();

  /**
   * @return items array of retained items.
   */
  abstract float[] getFloatRetainedItemsArray();

  abstract float getFloatSingleItem();

  // Min & Max Methods

  abstract float getMaxItemInternal();

  abstract void setMaxItem(float item);

  abstract float getMinItemInternal();

  abstract void setMinItem(float item);

  @Override
  abstract byte[] getMinMaxByteArr();

  @Override
  int getMinMaxSizeBytes() {
    return Float.BYTES * 2;
  }

  //END Min & Max Methods

  @Override
  abstract byte[] getRetainedItemsByteArr();

  @Override
  int getRetainedItemsSizeBytes() {
    return getNumRetained() * Float.BYTES;
  }

  @Override
  ArrayOfItemsSerDe<?> getSerDe() { return null; }

  @Override
  final byte[] getSingleItemByteArr() {
    final byte[] bytes = new byte[ITEM_BYTES];
    putFloatLE(bytes, 0, getFloatSingleItem());
    return bytes;
  }

  @Override
  int getSingleItemSizeBytes() {
    return Float.BYTES;
  }

  @Override
  abstract byte[] getTotalItemsByteArr();

  @Override
  int getTotalItemsNumBytes() {
    return levelsArr[getNumLevels()] * Float.BYTES;
  }

  abstract void setFloatItemsArray(float[] floatItems);

  abstract void setFloatItemsArrayAt(int index, float item);

  abstract void setFloatItemsArrayAt(int dstIndex, float[] srcItems, int srcOffset, int length);

  // SORTED VIEW

  @Override
  public FloatsSketchSortedView getSortedView() {
    refreshSortedView();
    return floatsSV;
  }

  private final FloatsSketchSortedView refreshSortedView() {
    if (floatsSV == null) {
      final CreateSortedView csv = new CreateSortedView();
      floatsSV = csv.getSV();
    }
    return floatsSV;
  }

  private final class CreateSortedView {
    float[] quantiles;
    long[] cumWeights;

    FloatsSketchSortedView getSV() {
      if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
      final float[] srcQuantiles = getFloatItemsArray();
      final int[] srcLevels = levelsArr;
      final int srcNumLevels = getNumLevels();

      if (!isLevelZeroSorted()) {
        Arrays.sort(srcQuantiles, srcLevels[0], srcLevels[1]);
        if (!hasMemorySegment()) { setLevelZeroSorted(true); }
        //we don't sort level0 in MemorySegment, only our copy.
      }
      final int numQuantiles = getNumRetained();
      quantiles = new float[numQuantiles];
      cumWeights = new long[numQuantiles];
      populateFromSketch(srcQuantiles, srcLevels, srcNumLevels, numQuantiles);
      return new FloatsSketchSortedView(quantiles, cumWeights, KllFloatsSketch.this);
    }

    private void populateFromSketch(final float[] srcQuantiles, final int[] srcLevels,
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

  private static void blockyTandemMergeSort(final float[] quantiles, final long[] weights,
      final int[] levels, final int numLevels) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final float[] quantilesTmp = Arrays.copyOf(quantiles, quantiles.length);
    final long[] weightsTmp = Arrays.copyOf(weights, quantiles.length); // don't need the extra one

    blockyTandemMergeSortRecursion(quantilesTmp, weightsTmp, quantiles, weights, levels, 0, numLevels);
  }

  private static void blockyTandemMergeSortRecursion(
      final float[] quantilesSrc, final long[] weightsSrc,
      final float[] quantilesDst, final long[] weightsDst,
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
      final float[] quantilesSrc, final long[] weightsSrc,
      final float[] quantilesDst, final long[] weightsDst,
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

    while ((iSrc1 < toIndex1) && (iSrc2 < toIndex2)) {
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

  // END SORTED VIEW

}
