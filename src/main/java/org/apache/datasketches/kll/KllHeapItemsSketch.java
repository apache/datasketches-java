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

import java.lang.reflect.Array;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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
   * Constructs a new empty instance of this sketch on the Java heap.
   */
  KllHeapItemsSketch(
      final int k,
      final int m,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(SketchStructure.UPDATABLE, comparator, serDe);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    this.levelsArr = new int[] {k, k};
    this.readOnly = false;
    this.k = k;
    this.m = m;
    this.n = 0;
    this.minK = k;
    this.isLevelZeroSorted = false;
    this.minItem = null;
    this.maxItem = null;
    this.itemsArr = new Object[k];
  }

  /**
   * The Heapify constructor, which constructs an image of this sketch from
   * a Memory (or WritableMemory) object that was created by this sketch
   * and has a type T consistent with the given comparator and serDe.
   * Once the data from the given Memory has been transferred into this heap sketch,
   * the reference to the Memory object is no longer retained.
   * @param srcMem the Source Memory image that contains data.
   * @param comparator the comparator for this sketch and given Memory.
   * @param serDe the serializer / deserializer for this sketch and the given Memory.
   */
  KllHeapItemsSketch(
      final Memory srcMem,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(SketchStructure.UPDATABLE, comparator, serDe);
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, SketchType.ITEMS_SKETCH, serDe);
    this.k = memVal.k;
    this.m = memVal.m;
    this.levelsArr = memVal.levelsArr;
    this.readOnly = false;
    this.n = memVal.n;
    this.minK = memVal.minK;
    this.isLevelZeroSorted = memVal.level0SortedFlag;
    this.itemsArr = new Object[levelsArr[memVal.numLevels]]; //updatable size
    final SketchStructure memStruct = memVal.sketchStructure;
    if (memStruct == COMPACT_EMPTY) {
      this.minItem = null;
      this.maxItem = null;
      this.itemsArr = new Object[k];
    } else if (memStruct == COMPACT_SINGLE) {
      final int offset = N_LONG_ADR;
      final T item = serDe.deserializeFromMemory(srcMem, offset, 1)[0];
      this.minItem = item;
      this.maxItem = item;
      itemsArr[k - 1] = item;
    } else if (memStruct == COMPACT_FULL) {
      int offset = DATA_START_ADR + memVal.numLevels * Integer.BYTES;
      this.minItem = serDe.deserializeFromMemory(srcMem, offset, 1)[0];
      offset += serDe.sizeOf(minItem);
      this.maxItem = serDe.deserializeFromMemory(srcMem, offset, 1)[0];
      offset += serDe.sizeOf(maxItem);
      final int numRetained = levelsArr[memVal.numLevels] - levelsArr[0];
      final Object[] retItems = serDe.deserializeFromMemory(srcMem, offset, numRetained);
      System.arraycopy(retItems, 0, itemsArr, levelsArr[0], numRetained);
    } else { //memStruct == UPDATABLE
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
    final T item = (T) itemsArr[k - 1];
    return item;
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
  WritableMemory getWritableMemory() {
    return null;
  }

  @Override
  void incN(final int increment) {
    n += increment;
  }

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
    this.itemsArr[index] = item;
  }

  @Override
  void setMaxItem(final Object item) {
    this.maxItem = (T) item;
  }

  @Override
  void setMinItem(final Object item) {
    this.minItem = (T) item;
  }

}
