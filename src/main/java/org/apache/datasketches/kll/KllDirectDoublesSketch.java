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
import static org.apache.datasketches.kll.KllSketch.Error.EMPTY;
import static org.apache.datasketches.kll.KllSketch.Error.NOT_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an off-heap, updatable doubles KllSketch using WritableMemory.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
class KllDirectDoublesSketch extends KllDoublesSketch {

  /**
   * The constructor with WritableMemory that can be off-heap.
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  KllDirectDoublesSketch(
      final SketchStructure sketchStructure,
      final WritableMemory wmem,
      final MemoryRequestServer memReqSvr,
      final KllMemoryValidate memVal) {
    super(sketchStructure, wmem, memReqSvr);
    levelsArr = memVal.levelsArr; //always converted to writable form.
  }

  /**
   * Create a new instance of this sketch.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param m parameter that controls the minimum level width in items.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new instance of this sketch
   */
  static KllDirectDoublesSketch newDirectInstance(
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

  @Override
  public int getK() {
    return getMemoryK(wmem);
  }

  @Override
  public double getMaxItem() {
    int levelsArrBytes = 0;
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { kllSketchThrow(EMPTY); }
    else if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    else if (sketchStructure == COMPACT_FULL) {
      levelsArrBytes = getLevelsArrBytes(COMPACT_FULL);
    } else { //UPDATABLE
      levelsArrBytes = getLevelsArrBytes(UPDATABLE);
    }
    final int offset =  DATA_START_ADR + levelsArrBytes + ITEM_BYTES;
    return wmem.getDouble(offset);
  }

  @Override
  public double getMinItem() {
    int levelsArrBytes = 0;
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { kllSketchThrow(EMPTY); }
    else if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    else if (sketchStructure == COMPACT_FULL) {
      levelsArrBytes = getLevelsArrBytes(COMPACT_FULL);
    } else { //UPDATABLE
      levelsArrBytes = getLevelsArrBytes(UPDATABLE);
    }
    final int offset =  DATA_START_ADR + levelsArrBytes;
    return wmem.getDouble(offset);
  }

  @Override
  public long getN() {
    if (sketchStructure == COMPACT_EMPTY) { return 0; }
    else if (sketchStructure == COMPACT_SINGLE) { return 1; }
    else { return getMemoryN(wmem); }
  }

  //restricted

  @Override //returns updatable, expanded array including empty/garbage space at bottom
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
    final int offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure) + 2 * ITEM_BYTES;
    final int shift = (sketchStructure == COMPACT_FULL) ? levelsArr[0] : 0;
    final int numItems = (sketchStructure == COMPACT_FULL) ? getNumRetained() : capacityItems;
    wmem.getDoubleArray(offset, doubleItemsArr, shift, numItems);
    return doubleItemsArr;
  }

  @Override //returns compact items array of retained items, no empty/garbage.
  double[] getDoubleRetainedItemsArray() {
    if (sketchStructure == COMPACT_EMPTY) { return new double[0]; }
    if (sketchStructure == COMPACT_SINGLE) { return new double[] { getDoubleSingleItem() }; }
    final int numRetained = getNumRetained();
    final double[] doubleItemsArr = new double[numRetained];
    final int offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure) + 2 * ITEM_BYTES
        + (sketchStructure == COMPACT_FULL ? 0 : levelsArr[0] * ITEM_BYTES);
    wmem.getDoubleArray(offset, doubleItemsArr, 0, numRetained);
    return doubleItemsArr;
  }

  @Override
  double getDoubleSingleItem() {
    if (!isSingleItem()) { kllSketchThrow(NOT_SINGLE_ITEM); }
    if (sketchStructure == COMPACT_SINGLE) {
      return wmem.getDouble(DATA_START_ADR_SINGLE_ITEM);
    }
    final int offset;
    if (sketchStructure == COMPACT_FULL) {
      offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure) + 2 * ITEM_BYTES;
    } else { //sketchStructure == UPDATABLE
      offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure) + (2 + getK() - 1) * ITEM_BYTES;
    }
    return wmem.getDouble(offset);
  }

  @Override
  int getM() {
    return getMemoryM(wmem);
  }

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
    offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure);
    wmem.getByteArray(offset, bytesOut, 0, ITEM_BYTES);
    wmem.getByteArray(offset + ITEM_BYTES, bytesOut, ITEM_BYTES, ITEM_BYTES);
    return bytesOut;
  }

  @Override
  byte[] getRetainedDataByteArr() {
    if (sketchStructure == COMPACT_EMPTY) { return new byte[0]; }
    final double[] dblArr = getDoubleRetainedItemsArray();
    final byte[] dblByteArr = new byte[dblArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(dblByteArr);
    wmem2.putDoubleArray(0, dblArr, 0, dblArr.length);
    return dblByteArr;
  }

  @Override
  byte[] getTotalItemDataByteArr() {
    final double[] dblArr = getDoubleItemsArray();
    final byte[] dblByteArr = new byte[dblArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(dblByteArr);
    wmem2.putDoubleArray(0, dblArr, 0, dblArr.length);
    return dblByteArr;
  }

  @Override
  int getTotalItemDataBytes() { //TODO MOVE THIS UP
    final int capacityItems = levelsArr[getNumLevels()];
    if (sketchType == FLOATS_SKETCH || sketchType == DOUBLES_SKETCH) {
      return capacityItems * ITEM_BYTES;
    }
    else {
      return 0; //ITEMS_SKETCH //TODO
    }
  }

  @Override
  void incN() {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    long n = getMemoryN(wmem);
    setMemoryN(wmem, ++n);
  }

  @Override
  void incNumLevels() {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    int numLevels = getMemoryNumLevels(wmem);
    setMemoryNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemoryLevelZeroSortedFlag(wmem);
  }

  @Override
  void setDoubleItemsArray(final double[] doubleItems) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure) + 2 * ITEM_BYTES;
    wmem.putDoubleArray(offset, doubleItems, 0, doubleItems.length);
  }

  @Override
  void setDoubleItemsArrayAt(final int index, final double item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset =
        DATA_START_ADR + getLevelsArrBytes(sketchStructure) + (index + 2) * ITEM_BYTES;
    wmem.putDouble(offset, item);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    setMemoryLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMaxItem(final double item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure) + ITEM_BYTES;
    wmem.putDouble(offset, item);
  }

  @Override
  void setMinItem(final double item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset = DATA_START_ADR + getLevelsArrBytes(sketchStructure);
    wmem.putDouble(offset, item);
  }

  @Override
  void setMinK(final int minK) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    setMemoryMinK(wmem, minK);
  }

  @Override
  void setN(final long n) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    setMemoryN(wmem, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    setMemoryNumLevels(wmem, numLevels);
  }

}
