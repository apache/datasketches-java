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
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_ITEMS_SKETCH;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.quantilescommon.GenericPartitionBoundaries;
import org.apache.datasketches.quantilescommon.ItemsSketchSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesGenericAPI;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;

/**
 * This variation of the KllSketch implements generic data types. The user must provide
 * a suitable implementation of the <i>java.lang.Comparator</i> as well as an implementation of
 * the serializer / deserializer, <i>org.apache.datasketches.common.ArrayOfItemsSerDe</i>.
 * @param <T> The sketch data type.
 * @see org.apache.datasketches.kll.KllSketch
 */
@SuppressWarnings("unchecked")
public abstract class KllItemsSketch<T> extends KllSketch implements QuantilesGenericAPI<T> {
  private ItemsSketchSortedView<T> itemsSV = null;
  final Comparator<? super T> comparator;
  final ArrayOfItemsSerDe<T> serDe;

  KllItemsSketch( //pass-through constructor
      final SketchStructure skStructure,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(KLL_ITEMS_SKETCH, skStructure);
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    Objects.requireNonNull(serDe, "SerDe must not be null.");
    this.comparator = comparator;
    this.serDe = serDe;
  }

  //Factories for new heap instances.

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for an array of items, <i>T[]</i>.
   * @param <T> The sketch data type.
   * @return new KllItemsSketch on the Java heap.
   */
  public static <T> KllItemsSketch<T> newHeapInstance(
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    return new KllHeapItemsSketch<>(DEFAULT_K, DEFAULT_M, comparator, serDe);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be between DEFAULT_M and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   * @param <T> The sketch data type
   * @return new KllItemsSketch on the heap.
   */
  public static <T> KllItemsSketch<T> newHeapInstance(
      final int k,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    return new KllHeapItemsSketch<>(k, DEFAULT_M, comparator, serDe);
  }

  // Factory to create an heap instance from a MemorySegment image

  /**
   * Factory heapify takes a compact sketch image in MemorySegment and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source MemorySegment.
   * @param srcSeg a compact MemorySegment image of a sketch serialized by this sketch and of the same type of T.
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   * @param <T> The sketch data type
   * @return a heap-based sketch based on the given MemorySegment.
   */
  public static <T> KllItemsSketch<T> heapify(
      final MemorySegment srcSeg,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    return new KllHeapItemsSketch<>(srcSeg, comparator, serDe);
  }

  //Factory to wrap a Read-Only MemorySegment

  /**
   * Constructs a thin wrapper on the heap around a MemorySegment (or MemorySegment) already initialized with a
   * validated sketch image of a type T consistent with the given comparator and serDe.
   * A reference to the MemorySegment is kept in the sketch and must remain in scope consistent
   * with the temporal scope of this sketch. The amount of data kept on the heap is very small.
   * All of the item data originally collected by the given MemorySegment sketch object remains in the
   * MemorySegment object.
   * @param srcSeg the MemorySegment object that this sketch will wrap. It will not be modified.
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   * @param <T> The sketch data type
   * @return a heap-base sketch that is a thin wrapper around the given srcSeg.
   */
  public static <T> KllItemsSketch<T> wrap(
      final MemorySegment srcSeg,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(srcSeg, SketchType.KLL_ITEMS_SKETCH, serDe);
    return new KllDirectCompactItemsSketch<>(segVal, comparator, serDe);
  }

  //END of Constructors

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return itemsSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public Class<T> getClassOfT() { return serDe.getClassOfT(); }

  @Override
  public Comparator<? super T> getComparator() {
    return comparator;
  }

  @Override
  public GenericPartitionBoundaries<T> getPartitionBoundariesFromNumParts(
      final int numEquallySizedParts,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return itemsSV.getPartitionBoundariesFromNumParts(numEquallySizedParts, searchCrit);
  }

  @Override
  public GenericPartitionBoundaries<T> getPartitionBoundariesFromPartSize(
      final long nominalPartSizeItems,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return itemsSV.getPartitionBoundariesFromPartSize(nominalPartSizeItems, searchCrit);
  }

  @Override
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return itemsSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return itemsSV.getQuantile(rank, searchCrit);
  }

  @Override
  public T[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = ranks.length;
    final T[] quantiles = (T[]) Array.newInstance(getMinItem().getClass(), len);
    for (int i = 0; i < len; i++) {
      quantiles[i] = itemsSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  @Override
  public T getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public T getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public double getRank(final T quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    return itemsSV.getRank(quantile, searchCrit);
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
  public double[] getRanks(final T[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = itemsSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  public final ItemsSketchSortedView<T> getSortedView() {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    return refreshSortedView();
  }

  @Override
  public QuantilesGenericSketchIterator<T> iterator() {
    return new KllItemsSketchIterator<>(
        getTotalItemsArray(), getLevelsArray(SketchStructure.UPDATABLE), getNumLevels());
  }

  @Override
  public final void merge(final KllSketch other) {
    if (readOnly || (sketchStructure != UPDATABLE)) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (this == other) { throw new SketchesArgumentException(SELF_MERGE_MSG); }
    final KllItemsSketch<T> othItmSk = (KllItemsSketch<T>)other;
    if (othItmSk.isEmpty()) { return; }
    KllItemsHelper.mergeItemImpl(this, othItmSk, comparator);
    itemsSV = null;
  }

  @Override
  public void reset() {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelZeroSorted(false);
    setLevelsArray(new int[] {k, k});
    setMinItem(null);
    setMaxItem(null);
    setItemsArray(new Object[k]);
    itemsSV = null;
  }

  /**
   * Export the current sketch as a compact byte array.
   * @return the current sketch as a compact byte array.
   */
  public byte[] toByteArray() {
    return KllHelper.toByteArray(this, false);
  }

  @Override
  public String toString(final boolean withLevels, final boolean withLevelsAndItems) {
    KllSketch sketch = this;
    if (hasMemorySegment()) {
      final MemorySegment seg = getMemorySegment();
      assert seg != null;
      sketch = KllItemsSketch.heapify(getMemorySegment(), comparator, serDe);
    }
    return KllHelper.toStringImpl(sketch, withLevels, withLevelsAndItems);
  }

  @Override
  public void update(final T item) {
    if (item == null) { return; } //ignore
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    KllItemsHelper.updateItem(this, item);
    itemsSV = null;
  }

  /**
   * Weighted update. Updates this sketch with the given item the number of times specified by the given integer weight.
   * @param item the item to be repeated. NaNs are ignored.
   * @param weight the number of times the update of item is to be repeated. It must be &ge; one.
   */
  public void update(final T item, final long weight) {
    if (item == null) { return; } //ignore
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    if (weight < 1L) { throw new SketchesArgumentException("Weight is less than one."); }
    if (weight == 1L) { KllItemsHelper.updateItem(this, item); }
    else { KllItemsHelper.updateItem(this, item, weight); }
    itemsSV = null;
  }

  //restricted

  @Override
  abstract byte[] getMinMaxByteArr();

  @Override
  abstract int getMinMaxSizeBytes();

  abstract T[] getRetainedItemsArray();

  @Override
  abstract byte[] getRetainedItemsByteArr();

  @Override
  abstract int getRetainedItemsSizeBytes();

  //abstract Object[] getRetainedItemsArray();

  @Override
  ArrayOfItemsSerDe<T> getSerDe() { return serDe; }

  abstract T getSingleItem();

  @Override
  abstract byte[] getSingleItemByteArr();

  @Override
  abstract int getSingleItemSizeBytes();

  /**
   * @return a full array of items as if the sketch was in COMPACT_FULL or UPDATABLE format.
   * This will include zeros and possibly some free space.
   */
  abstract T[] getTotalItemsArray();

  @Override
  byte[] getTotalItemsByteArr() {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  int getTotalItemsNumBytes() {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void incNumLevels() {
    //this is not used and must be a no-op.
  }

  abstract void setItemsArray(Object[] ItemsArr);

  abstract void setItemsArrayAt(int index, Object item);

  abstract void setMaxItem(Object item);

  abstract void setMinItem(Object item);

  @Override
  void setNumLevels(final int numLevels) {
    // this is not used and must be a no-op.
  }

  @Override
  void setMemorySegment(final MemorySegment wseg) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG + "Sketch not writable.");
  }

  void updateMinMax(final T item) {
    if (isEmpty()) {
      setMinItem(item);
      setMaxItem(item);
    } else {
      setMinItem(Util.minT(getMinItem(), item, comparator));
      setMaxItem(Util.maxT(getMaxItem(), item, comparator));
    }
  }

  private final ItemsSketchSortedView<T> refreshSortedView() {
    if (itemsSV == null) {
      final CreateSortedView csv = new CreateSortedView();
      itemsSV = csv.getSV();
    }
    return itemsSV;
  }

  @SuppressWarnings({"rawtypes"})
  private final class CreateSortedView {
    T[] quantiles;
    long[] cumWeights; //The new cumWeights array

    ItemsSketchSortedView<T> getSV() {
      if (isEmpty() || (getN() == 0)) { throw new SketchesArgumentException(EMPTY_MSG); }
      final T[] srcQuantiles = getTotalItemsArray();
      final int[] srcLevelsArr = levelsArr;
      final int srcNumLevels = getNumLevels();

      if (!isLevelZeroSorted()) {
        Arrays.sort(srcQuantiles, srcLevelsArr[0], srcLevelsArr[1], comparator);
        if (!hasMemorySegment()) { setLevelZeroSorted(true); }
      }
      final int numQuantiles = getNumRetained();
      quantiles = (T[]) Array.newInstance(serDe.getClassOfT(), numQuantiles);
      cumWeights = new long[numQuantiles];
      populateFromSketch(srcQuantiles, srcLevelsArr, srcNumLevels, numQuantiles);
      final QuantilesGenericAPI<T> sk = KllItemsSketch.this;
      return new ItemsSketchSortedView(quantiles, cumWeights, sk);
    }

    private void populateFromSketch(final Object[] srcQuantiles, final int[] srcLevelsArr,
        final int srcNumLevels, final int numItems) {
        //Remove free space from both itemsArray and levels array
        final int[] myLevelsArr = new int[srcLevelsArr.length];
        final int offset = srcLevelsArr[0];
        System.arraycopy(srcQuantiles, offset, quantiles, 0, numItems); //remove free space from quantiles arr
        //fill the new cumWeights array with the correct weights and adjust the levels array to match.
        int srcLevel = 0;
        int dstLevel = 0;
        long weight = 1;
        while (srcLevel < srcNumLevels) {
          final int fromIndex = srcLevelsArr[srcLevel] - offset;
          final int toIndex = srcLevelsArr[srcLevel + 1] - offset; // exclusive
          if (fromIndex < toIndex) { // if equal, skip empty level
            Arrays.fill(cumWeights, fromIndex, toIndex, weight);
            myLevelsArr[dstLevel] = fromIndex;
            myLevelsArr[dstLevel + 1] = toIndex;
            dstLevel++;
          }
          srcLevel++;
          weight *= 2;
        }
        final int numLevels = dstLevel;
        blockyTandemMergeSort(quantiles, cumWeights, myLevelsArr, numLevels, comparator); //create unit weights
        KllHelper.convertToCumulative(cumWeights);
      }
  } //End of class CreateSortedView

    private static <T> void blockyTandemMergeSort(final Object[] quantiles, final long[] weights,
        final int[] levels, final int numLevels, final Comparator<? super T> comp) {
      if (numLevels == 1) { return; }

      // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
      final Object[] quantilesTmp = Arrays.copyOf(quantiles, quantiles.length);
      final long[] weightsTmp = Arrays.copyOf(weights, quantiles.length); // don't need the extra one here

      blockyTandemMergeSortRecursion(quantilesTmp, weightsTmp, quantiles, weights, levels, 0, numLevels, comp);
    }

    private static <T> void blockyTandemMergeSortRecursion(
        final Object[] quantilesSrc, final long[] weightsSrc,
        final Object[] quantilesDst, final long[] weightsDst,
        final int[] levels, final int startingLevel, final int numLevels, final Comparator<? super T> comp) {
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
          levels, startingLevel1, numLevels1, comp);
      blockyTandemMergeSortRecursion(
          quantilesDst, weightsDst,
          quantilesSrc, weightsSrc,
          levels, startingLevel2, numLevels2, comp);
      tandemMerge(
          quantilesSrc, weightsSrc,
          quantilesDst, weightsDst,
          levels,
          startingLevel1, numLevels1,
          startingLevel2, numLevels2, comp);
    }

    private static <T> void tandemMerge(
        final Object[] quantilesSrc, final long[] weightsSrc,
        final Object[] quantilesDst, final long[] weightsDst,
        final int[] levelStarts,
        final int startingLevel1, final int numLevels1,
        final int startingLevel2, final int numLevels2, final Comparator<? super T> comp) {
      final int fromIndex1 = levelStarts[startingLevel1];
      final int toIndex1 = levelStarts[startingLevel1 + numLevels1]; // exclusive
      final int fromIndex2 = levelStarts[startingLevel2];
      final int toIndex2 = levelStarts[startingLevel2 + numLevels2]; // exclusive
      int iSrc1 = fromIndex1;
      int iSrc2 = fromIndex2;
      int iDst = fromIndex1;

      while ((iSrc1 < toIndex1) && (iSrc2 < toIndex2)) {
        if (Util.lt((T) quantilesSrc[iSrc1], (T) quantilesSrc[iSrc2], comp)) {
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
