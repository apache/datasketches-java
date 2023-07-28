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
import static org.apache.datasketches.kll.KllSketch.Error.NOT_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

@SuppressWarnings("unchecked")
final class KllHeapItemsSketch<T> extends KllItemsSketch<T> {
  private final int k; // configured size of K.
  private final int m; // configured size of M.
  private long n;      // number of items input into this sketch.
  private int minK;    // dynamic minK for error estimation after merging with different k.
  private boolean isLevelZeroSorted;
  private Object minItem;
  private Object maxItem;
  private Object[] itemsArr;

  /**
   * Internal Constructor
   */
  KllHeapItemsSketch(
      final int k,
      final int m,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(comparator, serDe);
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

  @Override
  public int getK() {
    return k;
  }

  @Override
  public T getMaxItem() {
    return (T)maxItem;
  }

  @Override
  public T getMinItem() {
    return (T)minItem;
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
  MemoryRequestServer getMemoryRequestServer() {
    // TODO move to Direct eventually
    return null;
  }

  @Override
  int getMinK() {
    return minK;
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] minBytes = serDe.serializeToByteArray((T)minItem);
    final byte[] maxBytes = serDe.serializeToByteArray((T)maxItem);
    final byte[] minMaxBytes = new byte[minBytes.length + maxBytes.length];
    copyBytes(minBytes, 0, minMaxBytes, 0, minBytes.length);
    copyBytes(maxBytes, 0, minMaxBytes, minBytes.length, maxBytes.length);
    return minMaxBytes;
  }

  @Override
  int getMinMaxSizeBytes() {
    final int minBytes = serDe.sizeOf((T)minItem);
    final int maxBytes = serDe.sizeOf((T)maxItem);
    return minBytes + maxBytes;
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    return serDe.serializeToByteArray(getRetainedItemsArray());
  }

  @Override
  int getRetainedItemsSizeBytes() {
    return getRetainedItemsByteArr().length;
  }

  @Override
  T getSingleItem() {
    if (n != 1L) { kllSketchThrow(NOT_SINGLE_ITEM); return null; }
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
    return (T[])itemsArr;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    return serDe.serializeToByteArray(getTotalItemsArray());
  }

  @Override
  int getTotalItemsNumBytes() {
    return serDe.sizeOf(getTotalItemsArray());
  }

  @Override
  WritableMemory getWritableMemory() {
    //TODO move to Direct eventually
    return null;
  }

  @Override
  void incN() {
    n++;
  }

  @Override
  void incNumLevels() {
    //the heap sketch computes num levels from the array itself, so this is not used on-heap.
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
  void setNumLevels(final int numLevels) {
    //the heap sketch computes num levels from the array itself, so this is not used on-heap.
  }

  @Override
  void setWritablMemory(final WritableMemory wmem) {
    //TODO move to Direct eventually
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
    this.maxItem = item;
  }

  @Override
  void setMinItem(final Object item) {
    this.minItem = item;
  }

  @Override
  T[] getRetainedItemsArray() {
    final int srcIndex = levelsArr[0];
    final int numItems = levelsArr[getNumRetained()] - levelsArr[0];
    final T[] ret = copyRangeOfObjectArray(itemsArr, srcIndex, numItems);
    return ret;
  }

}
