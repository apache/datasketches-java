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
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySerVer;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.LONGS_SKETCH;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an off-heap, updatable KllLongsSketch using WritableMemory.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
class KllDirectLongsSketch extends KllLongsSketch {
  private WritableMemory writableMemory;
  private MemoryRequestServer memoryRequestServer;

  /**
   * Constructs from Memory or WritableMemory already initialized with a sketch image and validated.
   * @param writableMemory the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  KllDirectLongsSketch(
      final SketchStructure sketchStructure,
      final WritableMemory writableMemory,
      final MemoryRequestServer memReqSvr,
      final KllMemoryValidate memVal) {
    super(sketchStructure);
    this.writableMemory = writableMemory;
    this.memoryRequestServer = memReqSvr;
    readOnly = (writableMemory != null && writableMemory.isReadOnly()) || sketchStructure != UPDATABLE;
    levelsArr = memVal.levelsArr; //always converted to writable form.
  }

  /**
   * Create a new updatable, direct instance of this sketch.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param m parameter that controls the minimum level width in items.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new instance of this sketch
   */
  static KllDirectLongsSketch newDirectUpdatableInstance(
      final int k,
      final int m,
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    setMemoryPreInts(dstMem, UPDATABLE.getPreInts());
    setMemorySerVer(dstMem, UPDATABLE.getSerVer());
    setMemoryFamilyID(dstMem, Family.KLL.getID());
    setMemoryK(dstMem, k);
    setMemoryM(dstMem, m);
    setMemoryN(dstMem, 0);
    setMemoryMinK(dstMem, k);
    setMemoryNumLevels(dstMem, 1);
    int offset = DATA_START_ADR;
    //new Levels array
    dstMem.putIntArray(offset, new int[] {k, k}, 0, 2);
    offset += 2 * Integer.BYTES;
    //new min/max array
    dstMem.putLongArray(offset, new long[] {Long.MAX_VALUE, Long.MIN_VALUE}, 0, 2);
    offset += 2 * ITEM_BYTES;
    //new empty items array
    dstMem.putLongArray(offset, new long[k], 0, k);

    final KllMemoryValidate memVal = new KllMemoryValidate(dstMem, LONGS_SKETCH, null);
    final WritableMemory wMem = dstMem;
    return new KllDirectLongsSketch(UPDATABLE, wMem, memReqSvr, memVal);
  }

  //End of Constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "Null"; }
    return Long.toString(getLongItemsArray()[index]);
  }

  @Override
  public int getK() {
    return getMemoryK(writableMemory);
  }

  //MinMax Methods

  @Override
  public long getMaxItem() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    if (sketchStructure == COMPACT_SINGLE) { return getLongSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    return writableMemory.getLong(offset);
  }

  @Override
  long getMaxItemInternal() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { return Long.MAX_VALUE; }
    if (sketchStructure == COMPACT_SINGLE) { return getLongSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    return writableMemory.getLong(offset);
  }

  @Override
  String getMaxItemAsString() {
    final long maxItem = getMaxItemInternal();
    return Long.toString(maxItem);
  }

  @Override
  public long getMinItem() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    if (sketchStructure == COMPACT_SINGLE) { return getLongSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    return writableMemory.getLong(offset);
  }

  @Override
  long getMinItemInternal() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { return Long.MAX_VALUE; }
    if (sketchStructure == COMPACT_SINGLE) { return getLongSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    return writableMemory.getLong(offset);
  }

  @Override
  String getMinItemAsString() {
    final long minItem = getMinItemInternal();
    return Long.toString(minItem);
  }

  @Override
  void setMaxItem(final long item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    writableMemory.putLong(offset, item);
  }

  @Override
  void setMinItem(final long item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    writableMemory.putLong(offset, item);
  }

  //END MinMax Methods

  @Override
  public long getN() {
    if (sketchStructure == COMPACT_EMPTY) { return 0; }
    else if (sketchStructure == COMPACT_SINGLE) { return 1; }
    else { return getMemoryN(writableMemory); }
  }

  //other restricted

  @Override //returns updatable, expanded array including free space at bottom
  long[] getLongItemsArray() {
    final int k = getK();
    if (sketchStructure == COMPACT_EMPTY) { return new long[k]; }
    if (sketchStructure == COMPACT_SINGLE) {
      final long[] itemsArr = new long[k];
      itemsArr[k - 1] = getLongSingleItem();
      return itemsArr;
    }
    final int capacityItems = KllHelper.computeTotalItemCapacity(k, getM(), getNumLevels());
    final long[] longItemsArr = new long[capacityItems];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    final int shift = (sketchStructure == COMPACT_FULL) ? levelsArr[0] : 0;
    final int numItems = (sketchStructure == COMPACT_FULL) ? getNumRetained() : capacityItems;
    writableMemory.getLongArray(offset, longItemsArr, shift, numItems);
    return longItemsArr;
  }

  @Override //returns compact items array of retained items, no free space.
  long[] getLongRetainedItemsArray() {
    if (sketchStructure == COMPACT_EMPTY) { return new long[0]; }
    if (sketchStructure == COMPACT_SINGLE) { return new long[] { getLongSingleItem() }; }
    final int numRetained = getNumRetained();
    final long[] longItemsArr = new long[numRetained];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES
        + (sketchStructure == COMPACT_FULL ? 0 : levelsArr[0] * ITEM_BYTES);
    writableMemory.getLongArray(offset, longItemsArr, 0, numRetained);
    return longItemsArr;
  }

  @Override
  long getLongSingleItem() {
    if (!isSingleItem()) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    if (sketchStructure == COMPACT_SINGLE) {
      return writableMemory.getLong(DATA_START_ADR_SINGLE_ITEM);
    }
    final int offset;
    if (sketchStructure == COMPACT_FULL) {
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    } else { //sketchStructure == UPDATABLE
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (2 + getK() - 1) * ITEM_BYTES;
    }
    return writableMemory.getLong(offset);
  }

  @Override
  int getM() {
    return getMemoryM(writableMemory);
  }

  @Override
  MemoryRequestServer getMemoryRequestServer() { return memoryRequestServer; }

  @Override
  int getMinK() {
    if (sketchStructure == COMPACT_FULL || sketchStructure == UPDATABLE) { return getMemoryMinK(writableMemory); }
    return getK();
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * ITEM_BYTES];
    if (sketchStructure == COMPACT_EMPTY) {
      ByteArrayUtil.putLongLE(bytesOut, 0, Long.MAX_VALUE);
      ByteArrayUtil.putLongLE(bytesOut, ITEM_BYTES, Long.MIN_VALUE);
      return bytesOut;
    }
    final int offset;
    if (sketchStructure == COMPACT_SINGLE) {
      offset = DATA_START_ADR_SINGLE_ITEM;
      writableMemory.getByteArray(offset, bytesOut, 0, ITEM_BYTES);
      copyBytes(bytesOut, 0, bytesOut, ITEM_BYTES, ITEM_BYTES);
      return bytesOut;
    }
    //sketchStructure == UPDATABLE OR COMPACT_FULL
    offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    writableMemory.getByteArray(offset, bytesOut, 0, ITEM_BYTES);
    writableMemory.getByteArray(offset + ITEM_BYTES, bytesOut, ITEM_BYTES, ITEM_BYTES);
    return bytesOut;
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    if (sketchStructure == COMPACT_EMPTY) { return new byte[0]; }
    final long[] lngArr = getLongRetainedItemsArray();
    final byte[] lngByteArr = new byte[lngArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(lngByteArr);
    wmem2.putLongArray(0, lngArr, 0, lngArr.length);
    return lngByteArr;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final long[] lngArr = getLongItemsArray();
    final byte[] lngByteArr = new byte[lngArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(lngByteArr);
    wmem2.putLongArray(0, lngArr, 0, lngArr.length);
    return lngByteArr;
  }

  @Override
  WritableMemory getWritableMemory() {
    return writableMemory;
  }

  @Override
  void incN(final int increment) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryN(writableMemory, getMemoryN(writableMemory) + increment);
  }

  @Override
  void incNumLevels() {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    int numLevels = getMemoryNumLevels(writableMemory);
    setMemoryNumLevels(writableMemory, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemoryLevelZeroSortedFlag(writableMemory);
  }

  @Override
  void setLongItemsArray(final long[] longItems) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    writableMemory.putLongArray(offset, longItems, 0, longItems.length);
  }

  @Override
  void setLongItemsArrayAt(final int index, final long item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset =
        DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (index + 2) * ITEM_BYTES;
    writableMemory.putLong(offset, item);
  }

  @Override
  void setLongItemsArrayAt(final int index, final long[] items, final int srcOffset, final int length) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (index + 2) * ITEM_BYTES;
    writableMemory.putLongArray(offset, items, srcOffset, length);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryLevelZeroSortedFlag(writableMemory, sorted);
  }

  @Override
  void setMinK(final int minK) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryMinK(writableMemory, minK);
  }

  @Override
  void setN(final long n) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryN(writableMemory, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryNumLevels(writableMemory, numLevels);
  }

  @Override
  void setWritableMemory(final WritableMemory writableMemory) {
    this.writableMemory = writableMemory;
  }

  final static class KllDirectCompactLongsSketch extends KllDirectLongsSketch {

    KllDirectCompactLongsSketch(
        final SketchStructure sketchStructure,
        final Memory srcMem,
        final KllMemoryValidate memVal) {
      super(sketchStructure, (WritableMemory) srcMem, null, memVal);
    }
  }

}
