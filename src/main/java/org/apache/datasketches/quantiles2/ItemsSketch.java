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

package org.apache.datasketches.quantiles2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.quantiles2.ClassicUtil.MIN_K;
import static org.apache.datasketches.quantiles2.ClassicUtil.checkFamilyID;
import static org.apache.datasketches.quantiles2.ClassicUtil.checkK;
import static org.apache.datasketches.quantiles2.ClassicUtil.checkPreLongsFlagsCap;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeBaseBufferItems;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeBitPattern;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeCombinedBufferItemCapacity;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeRetainedItems;
import static org.apache.datasketches.quantiles2.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractFlags;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractK;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractN;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractSerVer;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Random;

import org.apache.datasketches.common.ArrayOfItemsSerDe2;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.quantilescommon.GenericPartitionBoundaries;
import org.apache.datasketches.quantilescommon.ItemsSketchSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesAPI;
import org.apache.datasketches.quantilescommon.QuantilesGenericAPI;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;

/**
 * This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch, using generic items,
 * described in section 3.2 of the journal version of the paper "Mergeable Summaries"
 * by Agarwal, Cormode, Huang, Phillips, Wei, and Yi:
 *
 * <p>Reference: <a href="http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13"></a></p>
 *
 * <p>A <i>k</i> of 128 produces a normalized, rank error of about 1.7%.
 * For example, the median returned from getQuantile(0.5) will be between the actual quantiles
 * from the hypothetically sorted array of input quantiles at normalized ranks of 0.483 and 0.517, with
 * a confidence of about 99%.</p>
 *
 * <p>The size of an ItemsSketch is very dependent on the size of the generic Items input into the sketch,
 * so there is no comparable size table as there is for the DoublesSketch.</p>
 *
 * @see QuantilesAPI
 *
 * @param <T> The sketch data type
 */
public final class ItemsSketch<T> implements QuantilesGenericAPI<T> {
  final Class<T> clazz;
  private final Comparator<? super T> comparator_;
  final int k_;
  long n_;
  T maxItem_; //The largest item ever seen in the stream.
  T minItem_; //The smallest item ever seen in the stream.

  /**
   * In the on-heap version, this equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't even be a java array,
   * so it won't know its own length.
   */
  int combinedBufferItemCapacity_;

  /**
   * Number of items currently in base buffer.
   *
   * <p>Count = N % (2 * K)</p>
   */
  int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   *
   * <p>Pattern = N / (2 * K)</p>
   */
  long bitPattern_;

  /**
   * This single array contains the base buffer plus all levels some of which may not be used.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers.
   *
   * <p>The levels arrays require quite a bit of explanation, which we defer until later.</p>
   */
  Object[] combinedBuffer_;

  ItemsSketchSortedView<T> classicQisSV = null;

  /**
   * Setting the seed makes the results of the sketch deterministic if the input items are
   * received in exactly the same order. This is only useful when performing test comparisons,
   * otherwise, it is not recommended.
   */
  public static final Random rand = new Random();

  private ItemsSketch(
      final int k,
      final Class<T> clazz,
      final Comparator<? super T> comparator) {
    Objects.requireNonNull(clazz, "Class<T> must not be null.");
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    checkK(k);
    k_ = k;
    this.clazz = clazz;
    comparator_ = comparator;
  }

  /**
   * Obtains a new instance of an ItemsSketch using the DEFAULT_K.
   * @param <T> The sketch data type
   * @param clazz the given class of T
   * @param comparator to compare items
   * @return an ItemSketch&lt;T&gt;.
   */
  public static <T> ItemsSketch<T> getInstance(
      final Class<T> clazz,
      final Comparator<? super T> comparator) {
    return getInstance(clazz, PreambleUtil.DEFAULT_K, comparator);
  }

  /**
   * Obtains a new instance of an ItemsSketch.
   * @param clazz the given class of T
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 2 and less than 65536 and a power of 2.
   * @param comparator to compare items
   * @param <T> The sketch data type
   * @return an ItemSketch&lt;T&gt;.
   */
  public static <T> ItemsSketch<T> getInstance(
      final Class<T> clazz,
      final int k,
      final Comparator<? super T> comparator) {
    final ItemsSketch<T> qs = new ItemsSketch<>(k, clazz, comparator);
    final int bufAlloc = 2 * Math.min(MIN_K, k); //the min is important
    qs.n_ = 0;
    qs.combinedBufferItemCapacity_ = bufAlloc;
    qs.combinedBuffer_ = new Object[bufAlloc];
    qs.baseBufferCount_ = 0;
    qs.bitPattern_ = 0;
    qs.minItem_ = null;
    qs.maxItem_ = null;
    return qs;
  }

  /**
   * Heapifies the given srcSeg, which must be a MemorySegment image of a ItemsSketch
   * @param clazz the given class of T
   * @param srcSeg a MemorySegment image of a sketch.
   * @param comparator to compare items
   * @param serDe an instance of ArrayOfItemsSerDe
   * @param <T> The sketch data type
   * @return a ItemSketch&lt;T&gt; on the Java heap.
   */
  public static <T> ItemsSketch<T> getInstance(
      final Class<T> clazz,
      final MemorySegment srcSeg,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe2<T> serDe) {
    final long segCapBytes = srcSeg.byteSize();
    if (segCapBytes < 8) {
      throw new SketchesArgumentException("MemorySegment too small: " + segCapBytes);
    }

    final int preambleLongs = extractPreLongs(srcSeg);
    final int serVer = extractSerVer(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    final int flags = extractFlags(srcSeg);
    final int k = extractK(srcSeg);

    ItemsUtil.checkItemsSerVer(serVer);

    if ((serVer == 3) && ((flags & COMPACT_FLAG_MASK) == 0)) {
      throw new SketchesArgumentException("Non-compact MemorySegment images are not supported.");
    }

    final boolean empty = checkPreLongsFlagsCap(preambleLongs, flags, segCapBytes);
    checkFamilyID(familyID);
    final ItemsSketch<T> sk = getInstance(clazz, k, comparator); //checks k
    if (empty) { return sk; }

    //Not empty, must have valid preamble + min, max
    final long n = extractN(srcSeg);

    //can't check MemorySegment capacity here, not enough information
    final int extra = 2; //for min, max
    final int numSegItems = computeRetainedItems(k, n) + extra;

    //set class members
    sk.n_ = n;
    sk.combinedBufferItemCapacity_ = computeCombinedBufferItemCapacity(k, n);
    sk.baseBufferCount_ = computeBaseBufferItems(k, n);
    sk.bitPattern_ = computeBitPattern(k, n);
    sk.combinedBuffer_ = new Object[sk.combinedBufferItemCapacity_];

    final int srcSegItemsOffsetBytes = preambleLongs * Long.BYTES;
    final MemorySegment mReg =
        srcSeg.asSlice(srcSegItemsOffsetBytes, srcSeg.byteSize() - srcSegItemsOffsetBytes);
    final T[] itemsArray = serDe.deserializeFromMemorySegment(mReg, 0, numSegItems);
    sk.itemsArrayToCombinedBuffer(itemsArray);
    return sk;
  }

  /**
   * Returns a copy of the given sketch
   * @param <T> The sketch data type
   * @param sketch the given sketch
   * @return a copy of the given sketch
   */
  static <T> ItemsSketch<T> copy(final ItemsSketch<T> sketch) {
    final ItemsSketch<T> qsCopy = ItemsSketch.getInstance(sketch.clazz, sketch.k_, sketch.comparator_);
    qsCopy.n_ = sketch.n_;
    qsCopy.minItem_ = sketch.isEmpty() ? null : sketch.getMinItem();
    qsCopy.maxItem_ = sketch.isEmpty() ? null : sketch.getMaxItem();
    qsCopy.combinedBufferItemCapacity_ = sketch.getCombinedBufferAllocatedCount();
    qsCopy.baseBufferCount_ = sketch.getBaseBufferCount();
    qsCopy.bitPattern_ = sketch.getBitPattern();
    final Object[] combBuf = sketch.getCombinedBuffer();
    qsCopy.combinedBuffer_ = Arrays.copyOf(combBuf, combBuf.length);
    return qsCopy;
  }

  //END of Constructors

  @Override
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return classicQisSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public Class<T> getClassOfT() { return clazz; }

  @Override
  public Comparator<? super T> getComparator() {
    return comparator_;
  }

  @Override
  public T getMaxItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return maxItem_;
  }

  @Override
  public T getMinItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return minItem_;
  }

  @Override
  public GenericPartitionBoundaries<T> getPartitionBoundariesFromNumParts(
      final int numEquallySizedParts,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return classicQisSV.getPartitionBoundariesFromNumParts(numEquallySizedParts, searchCrit);
  }

  @Override
  public GenericPartitionBoundaries<T> getPartitionBoundariesFromPartSize(
      final long nominalPartSizeItems,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return classicQisSV.getPartitionBoundariesFromPartSize(nominalPartSizeItems, searchCrit);
  }

  @Override
  public double[] getPMF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
  if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return classicQisSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public T getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return classicQisSV.getQuantile(rank, searchCrit);
  }

  @Override
  public T getQuantileLowerBound(final double rank) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return getQuantile(max(0, rank - getNormalizedRankError(k_, false)));
  }

  @Override
  public T getQuantileUpperBound(final double rank) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return getQuantile(min(1.0, rank + getNormalizedRankError(k_, false)));
  }

  @Override
  public T[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return classicQisSV.getQuantiles(ranks, searchCrit);
  }

  @Override
  public double getRank(final T quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    return classicQisSV.getRank(quantile, searchCrit);
  }

  @Override
  public double getRankLowerBound(final double rank) {
    return max(0.0, rank - getNormalizedRankError(k_, false));
  }

  @Override
  public double getRankUpperBound(final double rank) {
    return min(1.0, rank + getNormalizedRankError(k_, false));
  }

  @Override
  public double[] getRanks(final T[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = classicQisSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  public QuantilesGenericSketchIterator<T> iterator() {
    return new ItemsSketchIterator<>(this, bitPattern_);
  }

  @Override
  public int getK() {
    return k_;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  public double getNormalizedRankError(final boolean pmf) {
    return getNormalizedRankError(k_, pmf);
  }

  /**
   * Gets the normalized rank error given k and pmf.
   * Static method version of the {@link #getNormalizedRankError(boolean)}.
   * @param k the configuration parameter
   * @param pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return if pmf is true, the normalized rank error for the getPMF() function.
   * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   */
  public static double getNormalizedRankError(final int k, final boolean pmf) {
    return ClassicUtil.getNormalizedRankError(k, pmf);
  }

  /**
   * Gets the approximate <em>k</em> to use given epsilon, the normalized rank error.
   * @param epsilon the normalized rank error between zero and one.
   * @param pmf if true, this function returns <em>k</em> assuming the input epsilon
   * is the desired "double-sided" epsilon for the getPMF() function. Otherwise, this function
   * returns <em>k</em> assuming the input epsilon is the desired "single-sided"
   * epsilon for all the other queries.
   * @return <i>k</i> given epsilon.
   */
  public static int getKFromEpsilon(final double epsilon, final boolean pmf) {
    return ClassicUtil.getKFromEpsilon(epsilon, pmf);
  }

  @Override
  public boolean isEmpty() {
   return getN() == 0;
  }

  @Override
  public boolean isEstimationMode() {
    return getN() >= (2L * k_);
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public void reset() {
    n_ = 0;
    combinedBufferItemCapacity_ = 2 * Math.min(MIN_K, k_); //the min is important
    combinedBuffer_ = new Object[combinedBufferItemCapacity_];
    baseBufferCount_ = 0;
    bitPattern_ = 0;
    minItem_ = null;
    maxItem_ = null;
    classicQisSV = null;
  }

  /**
   * Serialize this sketch to a byte array form.
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return byte array of this sketch
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe2<T> serDe) {
    return toByteArray(false, serDe);
  }

  /**
   * Serialize this sketch to a byte array form.
   * @param ordered if true the base buffer will be ordered (default == false).
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return this sketch in a byte array form.
   */
  public byte[] toByteArray(final boolean ordered, final ArrayOfItemsSerDe2<T> serDe) {
    return ItemsByteArrayImpl.toByteArray(this, ordered, serDe);
  }

  /**
   * Returns human readable summary information about this sketch.
   * Used for debugging.
   */
  @Override
  public String toString() {
    return toString(false, false);
  }

  /**
   * Returns human readable summary information about this sketch.
   * Used for debugging.
   * @param withLevels if true includes sketch levels array summary information
   * @param withLevelsAndItems if true include detail of levels array and items array together
   * @return human readable summary information about this sketch.
   */
  public String toString(final boolean withLevels, final boolean withLevelsAndItems) {
    return ItemsUtil.toString(withLevels, withLevelsAndItems, this);
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of an ItemsSketch.
   * Used for debugging.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of an ItemsSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.toString(byteArr, false);
  }

  /**
   * Returns a human readable string of the preamble of a MemorySegment image of an ItemsSketch.
   * Used for debugging.
   * @param seg the given MemorySegment
   * @return a human readable string of the preamble of a MemorySegment image of an ItemsSketch.
   */
  public static String toString(final MemorySegment seg) {
    return PreambleUtil.toString(seg, false);
  }

  /**
   * From an existing sketch, this creates a new sketch that can have a smaller K.
   * The original sketch is not modified.
   *
   * @param newK the new K that must be smaller than current K.
   * It is required that this.getK() = newK * 2^(nonnegative integer).
   * @return the new sketch.
   */
  public ItemsSketch<T> downSample(final int newK) {
    final ItemsSketch<T> newSketch = ItemsSketch.getInstance(clazz, newK, comparator_);
    ItemsMergeImpl.downSamplingMergeInto(this, newSketch);
    return newSketch;
  }

  @Override
  public int getNumRetained() {
    return computeRetainedItems(getK(), getN());
  }

  /**
   * Puts the current sketch into the given MemorySegment if there is sufficient space.
   * Otherwise, throws an error.
   *
   * @param dstSeg the given MemorySegment.
   * @param serDe an instance of ArrayOfItemsSerDe
   */
  public void putMemory(final MemorySegment dstSeg, final ArrayOfItemsSerDe2<T> serDe) {
    final byte[] byteArr = toByteArray(serDe);
    final long segCap = dstSeg.byteSize();
    if (segCap < byteArr.length) {
      throw new SketchesArgumentException(
          "Destination MemorySegment not large enough: " + segCap + " < " + byteArr.length);
    }
    MemorySegment.copy(byteArr, 0, dstSeg, JAVA_BYTE, 0, byteArr.length);
  }

  @Override
  public void update(final T item) {
    // this method only uses the base buffer part of the combined buffer

    if (item == null) { return; }
    if ((maxItem_ == null) || (comparator_.compare(item, maxItem_) > 0)) { maxItem_ = item; }
    if ((minItem_ == null) || (comparator_.compare(item, minItem_) < 0)) { minItem_ = item; }

    if ((baseBufferCount_ + 1) > combinedBufferItemCapacity_) {
      ItemsSketch.growBaseBuffer(this);
    }
    combinedBuffer_[baseBufferCount_++] = item;
    n_++;
    if (baseBufferCount_ == (2 * k_)) {
      ItemsUtil.processFullBaseBuffer(this);
    }
    classicQisSV = null;
  }

  // Restricted

  /**
   * Returns the base buffer count
   * @return the base buffer count
   */
  int getBaseBufferCount() {
    return baseBufferCount_;
  }

  /**
   * Returns the allocated count for the combined base buffer
   * @return the allocated count for the combined base buffer
   */
  int getCombinedBufferAllocatedCount() {
    return combinedBufferItemCapacity_;
  }

  /**
   * Returns the bit pattern for valid log levels
   * @return the bit pattern for valid log levels
   */
  long getBitPattern() {
    return bitPattern_;
  }

  /**
   * Returns the combined buffer reference
   * @return the combined buffer reference
   */
  Object[] getCombinedBuffer() {
    return combinedBuffer_;
  }

  /**
   * Loads the Combined Buffer, min and max from the given items array.
   * The Combined Buffer is always in non-compact form and must be pre-allocated.
   * @param itemsArray the given items array
   */
  private void itemsArrayToCombinedBuffer(final T[] itemsArray) {
    final int extra = 2; // space for min and max items

    //Load min, max
    minItem_ = itemsArray[0];
    maxItem_ = itemsArray[1];

    //Load base buffer
    System.arraycopy(itemsArray, extra, combinedBuffer_, 0, baseBufferCount_);

    //Load levels
    long bits = bitPattern_;
    if (bits > 0) {
      int index = extra + baseBufferCount_;
      for (int level = 0; bits != 0L; level++, bits >>>= 1) {
        if ((bits & 1L) > 0L) {
          System.arraycopy(itemsArray, index, combinedBuffer_, (2 + level) * k_, k_);
          index += k_;
        }
      }
    }
  }

  private static <T> void growBaseBuffer(final ItemsSketch<T> sketch) {
    final Object[] baseBuffer = sketch.getCombinedBuffer();
    final int oldSize = sketch.getCombinedBufferAllocatedCount();
    final int k = sketch.getK();
    assert oldSize < (2 * k);
    final int newSize = Math.max(Math.min(2 * k, 2 * oldSize), 1);
    sketch.combinedBufferItemCapacity_ = newSize;
    sketch.combinedBuffer_ = Arrays.copyOf(baseBuffer, newSize);
  }

  //************SORTED VIEW****************************

  @Override
  public ItemsSketchSortedView<T> getSortedView() {
    return refreshSortedView();
  }

  private ItemsSketchSortedView<T> refreshSortedView() {
    return (classicQisSV == null) ? (classicQisSV = getSV(this)) : classicQisSV;
  }

  @SuppressWarnings({"unchecked"})
  private static <T> ItemsSketchSortedView<T> getSV(final ItemsSketch<T> sk) {
    final long totalN = sk.getN();
    if (sk.isEmpty() || (totalN == 0)) { throw new SketchesArgumentException(EMPTY_MSG); }
    final int k = sk.getK();
    final int numQuantiles = sk.getNumRetained();
    final T[] svQuantiles = (T[]) Array.newInstance(sk.clazz, numQuantiles);
    final long[] svCumWeights = new long[numQuantiles];
    final Comparator<? super T> comparator = sk.comparator_;

    final T[] combinedBuffer = (T[]) sk.getCombinedBuffer();
    final int baseBufferCount = sk.getBaseBufferCount();

    // Populate from ItemsSketch:
    // copy over the "levels" and then the base buffer, all with appropriate weights
    populateFromItemsSketch(k, totalN, sk.getBitPattern(), combinedBuffer, baseBufferCount,
        numQuantiles, svQuantiles, svCumWeights, sk.getComparator());

    // Sort the first "numSamples" slots of the two arrays in tandem,
    // taking advantage of the already sorted blocks of length k
    ItemsMergeImpl.blockyTandemMergeSort(svQuantiles, svCumWeights, numQuantiles, k, comparator);

    if (convertToCumulative(svCumWeights) != totalN) {
      throw new SketchesStateException("Sorted View is misconfigured. TotalN does not match cumWeights.");
    }

    return new ItemsSketchSortedView<>(svQuantiles, svCumWeights, sk);

  }

  private static <T> void populateFromItemsSketch(
      final int k, final long totalN, final long bitPattern, final T[] combinedBuffer,
      final int baseBufferCount, final int numQuantiles, final T[] svQuantiles, final long[] svCumWeights,
      final Comparator<? super T> comparator) {

    long weight = 1;
    int index = 0;
    long bits = bitPattern;
    assert bits == (totalN / (2L * k)); // internal consistency check
    for (int lvl = 0; bits != 0L; lvl++, bits >>>= 1) {
      weight <<= 1; // X2
      if ((bits & 1L) > 0L) {
        final int offset = (2 + lvl) * k;
        for (int i = 0; i < k; i++) {
          svQuantiles[index] = combinedBuffer[i + offset];
          svCumWeights[index] = weight;
          index++;
        }
      }
    }

    weight = 1; //NOT a mistake! We just copied the highest level; now we need to copy the base buffer
    final int startOfBaseBufferBlock = index;

    // Copy BaseBuffer over, along with weight = 1
    for (int i = 0; i < baseBufferCount; i++) {
      svQuantiles[index] = combinedBuffer[i];
      svCumWeights[index] = weight;
      index++;
    }
    assert index == numQuantiles;

    // Must sort the items that came from the base buffer.
    // Don't need to sort the corresponding weights because they are all the same.
    Arrays.sort(svQuantiles, startOfBaseBufferBlock, numQuantiles, comparator);
  }

  /**
   * Convert the individual weights into cumulative weights.
   * An array of {1,1,1,1} becomes {1,2,3,4}
   * @param array of actual weights from the sketch, none of the weights may be zero
   * @return total weight
   */
  private static long convertToCumulative(final long[] array) {
    long subtotal = 0;
    for (int i = 0; i < array.length; i++) {
      final long newSubtotal = subtotal + array[i];
      subtotal = array[i] = newSubtotal;
    }
    return subtotal;
  }

}
