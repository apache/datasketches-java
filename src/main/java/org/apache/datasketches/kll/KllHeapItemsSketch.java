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

import static org.apache.datasketches.common.ByteArrayUtil.copyBytes;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfItemsSerDe2;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This class implements an on-heap items KllSketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
@SuppressWarnings("unchecked")
final class KllHeapItemsSketch<T> extends KllItemsSketch<T> {
  private final int k; // configured size of K.
  private final int m; // configured size of M.
  private long n;      // number of items input into this sketch.
  private int minK;    // dynamic minK for error estimation after merging with different k.
  private boolean isLevelZeroSorted;
  private T minItem;
  private T maxItem;
  private Object[] itemsArr;

  /**
   * New instance heap constructor.
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
   * The DEFAULT_M, which is 8 is recommended. Other sizes of <em>m</em> should be considered
   * experimental as they have not been as well characterized.
   * @param comparator user specified comparator of type T.
   * @param serDe serialization / deserialization class
   */
  KllHeapItemsSketch(final int k, final int m, final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe2<T> serDe) {
    super(UPDATABLE, comparator, serDe);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    levelsArr = new int[] {k, k};
    readOnly = false;
    this.k = k;
    this.m = m;
    n = 0;
    minK = k;
    isLevelZeroSorted = false;
    minItem = null;
    maxItem = null;
    itemsArr = new Object[k];
  }

  /**
   * Used for creating a temporary sketch for use with weighted updates.
   */
  KllHeapItemsSketch(final int k, final int m, final T item, final long weight, final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe2<T> serDe) {
    super(UPDATABLE, comparator, serDe);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    levelsArr = KllHelper.createLevelsArray(weight);
    readOnly = false;
    this.k = k;
    this.m = m;
    n = weight;
    minK = k;
    isLevelZeroSorted = false;
    minItem = item;
    maxItem = item;
    itemsArr = KllItemsHelper.createItemsArray(serDe.getClassOfT(), item, weight);
  }

  /**
   * The Heapify constructor
   * @param srcSeg the Source MemorySegment image that contains data.
   * @param comparator the comparator for this sketch and given MemorySegment.
   * @param serDe the serializer / deserializer for this sketch and the given MemorySegment.
   */
  KllHeapItemsSketch(
      final MemorySegment srcSeg,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe2<T> serDe) {
    super(SketchStructure.UPDATABLE, comparator, serDe);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(srcSeg, SketchType.ITEMS_SKETCH, serDe);
    k = segVal.k;
    m = segVal.m;
    levelsArr = segVal.levelsArr;
    readOnly = false;
    n = segVal.n;
    minK = segVal.minK;
    isLevelZeroSorted = segVal.level0SortedFlag;
    itemsArr = new Object[levelsArr[segVal.numLevels]]; //updatable size
    final SketchStructure segStruct = segVal.sketchStructure;
    if (segStruct == COMPACT_EMPTY) {
      minItem = null;
      maxItem = null;
      itemsArr = new Object[k];
    } else if (segStruct == COMPACT_SINGLE) {
      final int offset = N_LONG_ADR;
      final T item = serDe.deserializeFromMemorySegment(srcSeg, offset, 1)[0];
      minItem = item;
      maxItem = item;
      itemsArr[k - 1] = item;
    } else if (segStruct == COMPACT_FULL) {
      int offset = DATA_START_ADR + (segVal.numLevels * Integer.BYTES);
      minItem = serDe.deserializeFromMemorySegment(srcSeg, offset, 1)[0];
      offset += serDe.sizeOf(minItem);
      maxItem = serDe.deserializeFromMemorySegment(srcSeg, offset, 1)[0];
      offset += serDe.sizeOf(maxItem);
      final int numRetained = levelsArr[segVal.numLevels] - levelsArr[0];
      final Object[] retItems = serDe.deserializeFromMemorySegment(srcSeg, offset, numRetained);
      System.arraycopy(retItems, 0, itemsArr, levelsArr[0], numRetained);
    } else { //segStruct == UPDATABLE
      throw new SketchesArgumentException(UNSUPPORTED_MSG + "UPDATABLE");
    }
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "Null"; }
    return serDe.toString((T)(itemsArr[index]));
  }

  @Override
  public int getK() {
    return k;
  }

  @Override
  public T getMaxItem() {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    return maxItem;
  }

  @Override
  String getMaxItemAsString() {
    if (isEmpty()) { return "Null"; }
    return serDe.toString(maxItem);
  }

  @Override
  public T getMinItem() {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    return minItem;
  }

  @Override
  String getMinItemAsString() {
    if (isEmpty()) { return "Null"; }
    return serDe.toString(minItem);
  }

  @Override
  public long getN() {
    return n;
  }

  //restricted

  @Override
  int getM() {
    return m;
  }

  @Override
  int getMinK() {
    return minK;
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] minBytes = serDe.serializeToByteArray(minItem);
    final byte[] maxBytes = serDe.serializeToByteArray(maxItem);
    final byte[] minMaxBytes = new byte[minBytes.length + maxBytes.length];
    copyBytes(minBytes, 0, minMaxBytes, 0, minBytes.length);
    copyBytes(maxBytes, 0, minMaxBytes, minBytes.length, maxBytes.length);
    return minMaxBytes;
  }

  @Override
  int getMinMaxSizeBytes() {
    final int minBytes = serDe.sizeOf(minItem);
    final int maxBytes = serDe.sizeOf(maxItem);
    return minBytes + maxBytes;
  }

  @Override
  T[] getRetainedItemsArray() {
    final int numRet = getNumRetained();
    final T[] outArr = (T[]) Array.newInstance(serDe.getClassOfT(), numRet);
    System.arraycopy(itemsArr, levelsArr[0], outArr, 0 , numRet);
    return outArr;
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    final T[] retArr = getRetainedItemsArray();
    return serDe.serializeToByteArray(retArr);
  }

  @Override
  int getRetainedItemsSizeBytes() {
    return getRetainedItemsByteArr().length;
  }

  @Override
  T getSingleItem() {
    if (n != 1L) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    return (T) itemsArr[k - 1];
  }

  @Override
  byte[] getSingleItemByteArr() {
    return serDe.serializeToByteArray(getSingleItem());
  }

  @Override
  int getSingleItemSizeBytes() {
    return serDe.sizeOf(getSingleItem());
  }

  @Override
  T[] getTotalItemsArray() {
    if (n == 0) { return (T[]) Array.newInstance(serDe.getClassOfT(), k); }
    final T[] outArr = (T[]) Array.newInstance(serDe.getClassOfT(), itemsArr.length);
    System.arraycopy(itemsArr, 0, outArr, 0, itemsArr.length);
    return outArr;
  }

  @Override
  MemorySegment getMemorySegment() {
    return null;
  }

  @Override
  void incN(final int increment) { n += increment; }

  @Override
  boolean isLevelZeroSorted() {
    return isLevelZeroSorted;
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    isLevelZeroSorted = sorted;
  }

  @Override
  void setMinK(final int minK) {
    this.minK = minK;
  }

  @Override
  void setN(final long n) {
    this.n = n;
  }

  @Override
  void setItemsArray(final Object[] itemsArr) {
    this.itemsArr = itemsArr;
  }

  @Override
  void setItemsArrayAt(final int index, final Object item) {
    itemsArr[index] = item;
  }

  @Override
  void setMaxItem(final Object item) {
    maxItem = (T) item;
  }

  @Override
  void setMinItem(final Object item) {
    minItem = (T) item;
  }

  @Override
  public boolean hasMemorySegment() {
    return false;
  }

  @Override
  public boolean isOffHeap() {
    return false;
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return false;
  }

  @Override
  MemorySegmentRequest getMemorySegmentRequest() {
    return null;
  }

}
