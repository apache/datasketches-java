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
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an off-heap, updatable KllDoublesSketch using WritableMemory.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
class KllDirectDoublesSketch extends KllDoublesSketch {
  private WritableMemory wmem;
  private MemoryRequestServer memReqSvr;

  /**
   * Constructs from Memory or WritableMemory already initialized with a sketch image and validated.
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  KllDirectDoublesSketch(
      final SketchStructure sketchStructure,
      final WritableMemory wmem,
      final MemoryRequestServer memReqSvr,
      final KllMemoryValidate memVal) {
    super(sketchStructure);
    this.wmem = wmem;
    this.memReqSvr = memReqSvr;
    readOnly = (wmem != null && wmem.isReadOnly()) || sketchStructure != UPDATABLE;
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
  static KllDirectDoublesSketch newDirectUpdatableInstance(
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
    dstMem.putDoubleArray(offset, new double[] {Double.NaN, Double.NaN}, 0, 2);
    offset += 2 * ITEM_BYTES;
    //new empty items array
    dstMem.putDoubleArray(offset, new double[k], 0, k);

    final KllMemoryValidate memVal = new KllMemoryValidate(dstMem, DOUBLES_SKETCH, null);
    final WritableMemory wMem = dstMem;
    return new KllDirectDoublesSketch(UPDATABLE, wMem, memReqSvr, memVal);
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "NaN"; }
    return Double.toString(getDoubleItemsArray()[index]);
  }

  @Override
  public int getK() {
    return getMemoryK(wmem);
  }

  //MinMax Methods

  @Override
  public double getMaxItem() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    return wmem.getDouble(offset);
  }

  @Override
  double getMaxItemInternal() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { return Double.NaN; }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    return wmem.getDouble(offset);
  }

  @Override
  String getMaxItemAsString() {
    final double maxItem = getMaxItemInternal();
    return Double.toString(maxItem);
  }

  @Override
  public double getMinItem() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    return wmem.getDouble(offset);
  }

  @Override
  double getMinItemInternal() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { return Double.NaN; }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    return wmem.getDouble(offset);
  }

  @Override
  String getMinItemAsString() {
    final double minItem = getMinItemInternal();
    return Double.toString(minItem);
  }

  @Override
  void setMaxItem(final double item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    wmem.putDouble(offset, item);
  }

  @Override
  void setMinItem(final double item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    wmem.putDouble(offset, item);
  }

  //END MinMax Methods

  @Override
  public long getN() {
    if (sketchStructure == COMPACT_EMPTY) { return 0; }
    else if (sketchStructure == COMPACT_SINGLE) { return 1; }
    else { return getMemoryN(wmem); }
  }

  //other restricted

  @Override //returns updatable, expanded array including free space at bottom
  double[] getDoubleItemsArray() {
    final int k = getK();
    if (sketchStructure == COMPACT_EMPTY) { return new double[k]; }
    if (sketchStructure == COMPACT_SINGLE) {
      final double[] itemsArr = new double[k];
      itemsArr[k - 1] = getDoubleSingleItem();
      return itemsArr;
    }
    final int capacityItems = KllHelper.computeTotalItemCapacity(k, getM(), getNumLevels());
    final double[] doubleItemsArr = new double[capacityItems];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    final int shift = (sketchStructure == COMPACT_FULL) ? levelsArr[0] : 0;
    final int numItems = (sketchStructure == COMPACT_FULL) ? getNumRetained() : capacityItems;
    wmem.getDoubleArray(offset, doubleItemsArr, shift, numItems);
    return doubleItemsArr;
  }

  @Override //returns compact items array of retained items, no free space.
  double[] getDoubleRetainedItemsArray() {
    if (sketchStructure == COMPACT_EMPTY) { return new double[0]; }
    if (sketchStructure == COMPACT_SINGLE) { return new double[] { getDoubleSingleItem() }; }
    final int numRetained = getNumRetained();
    final double[] doubleItemsArr = new double[numRetained];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES
        + (sketchStructure == COMPACT_FULL ? 0 : levelsArr[0] * ITEM_BYTES);
    wmem.getDoubleArray(offset, doubleItemsArr, 0, numRetained);
    return doubleItemsArr;
  }

  @Override
  double getDoubleSingleItem() {
    if (!isSingleItem()) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    if (sketchStructure == COMPACT_SINGLE) {
      return wmem.getDouble(DATA_START_ADR_SINGLE_ITEM);
    }
    final int offset;
    if (sketchStructure == COMPACT_FULL) {
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    } else { //sketchStructure == UPDATABLE
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (2 + getK() - 1) * ITEM_BYTES;
    }
    return wmem.getDouble(offset);
  }

  @Override
  int getM() {
    return getMemoryM(wmem);
  }

  @Override
  MemoryRequestServer getMemoryRequestServer() { return memReqSvr; }

  @Override
  int getMinK() {
    if (sketchStructure == COMPACT_FULL || sketchStructure == UPDATABLE) { return getMemoryMinK(wmem); }
    return getK();
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * ITEM_BYTES];
    if (sketchStructure == COMPACT_EMPTY) {
      ByteArrayUtil.putDoubleLE(bytesOut, 0, Double.NaN);
      ByteArrayUtil.putDoubleLE(bytesOut, ITEM_BYTES, Double.NaN);
      return bytesOut;
    }
    final int offset;
    if (sketchStructure == COMPACT_SINGLE) {
      offset = DATA_START_ADR_SINGLE_ITEM;
      wmem.getByteArray(offset, bytesOut, 0, ITEM_BYTES);
      copyBytes(bytesOut, 0, bytesOut, ITEM_BYTES, ITEM_BYTES);
      return bytesOut;
    }
    //sketchStructure == UPDATABLE OR COMPACT_FULL
    offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    wmem.getByteArray(offset, bytesOut, 0, ITEM_BYTES);
    wmem.getByteArray(offset + ITEM_BYTES, bytesOut, ITEM_BYTES, ITEM_BYTES);
    return bytesOut;
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    if (sketchStructure == COMPACT_EMPTY) { return new byte[0]; }
    final double[] dblArr = getDoubleRetainedItemsArray();
    final byte[] dblByteArr = new byte[dblArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(dblByteArr);
    wmem2.putDoubleArray(0, dblArr, 0, dblArr.length);
    return dblByteArr;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final double[] dblArr = getDoubleItemsArray();
    final byte[] dblByteArr = new byte[dblArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(dblByteArr);
    wmem2.putDoubleArray(0, dblArr, 0, dblArr.length);
    return dblByteArr;
  }

  @Override
  WritableMemory getWritableMemory() {
    return wmem;
  }

  @Override
  void incN(final int increment) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryN(wmem, getMemoryN(wmem) + increment);
  }

  @Override
  void incNumLevels() {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    int numLevels = getMemoryNumLevels(wmem);
    setMemoryNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemoryLevelZeroSortedFlag(wmem);
  }

  @Override
  void setDoubleItemsArray(final double[] doubleItems) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    wmem.putDoubleArray(offset, doubleItems, 0, doubleItems.length);
  }

  @Override
  void setDoubleItemsArrayAt(final int index, final double item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset =
        DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (index + 2) * ITEM_BYTES;
    wmem.putDouble(offset, item);
  }

  @Override
  void setDoubleItemsArrayAt(final int index, final double[] items, final int srcOffset, final int length) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (index + 2) * ITEM_BYTES;
    wmem.putDoubleArray(offset, items, srcOffset, length);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMinK(final int minK) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryMinK(wmem, minK);
  }

  @Override
  void setN(final long n) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryN(wmem, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryNumLevels(wmem, numLevels);
  }

  @Override
  void setWritableMemory(final WritableMemory wmem) {
    this.wmem = wmem;
  }

  final static class KllDirectCompactDoublesSketch extends KllDirectDoublesSketch {

    KllDirectCompactDoublesSketch(
        final SketchStructure sketchStructure,
        final Memory srcMem,
        final KllMemoryValidate memVal) {
      super(sketchStructure, (WritableMemory) srcMem, null, memVal);
    }
  }

}
