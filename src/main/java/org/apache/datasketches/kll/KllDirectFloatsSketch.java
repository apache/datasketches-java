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
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an off-heap, updatable KllFloatsSketch using WritableMemory.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
class KllDirectFloatsSketch extends KllFloatsSketch {
  private WritableMemory wmem;
  private MemoryRequestServer memReqSvr;

  /**
   * Constructs from Memory or WritableMemory already initialized with a sketch image and validated.
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  KllDirectFloatsSketch(
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
  static KllDirectFloatsSketch newDirectUpdatableInstance(
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
    dstMem.putFloatArray(offset, new float[] {Float.NaN, Float.NaN}, 0, 2);
    offset += 2 * ITEM_BYTES;
    //new empty items array
    dstMem.putFloatArray(offset, new float[k], 0, k);

    final KllMemoryValidate memVal = new KllMemoryValidate(dstMem, FLOATS_SKETCH, null);
    final WritableMemory wMem = dstMem;
    return new KllDirectFloatsSketch(UPDATABLE, wMem, memReqSvr, memVal);
  }

  //END of Constructors

  @Override
  public int getK() {
    return getMemoryK(wmem);
  }

  @Override
  public float getMaxItem() {
    int levelsArrBytes = 0;
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    else if (sketchStructure == COMPACT_SINGLE) { return getFloatSingleItem(); }
    else if (sketchStructure == COMPACT_FULL) {
      levelsArrBytes = getLevelsArrSizeBytes(COMPACT_FULL);
    } else { //UPDATABLE
      levelsArrBytes = getLevelsArrSizeBytes(UPDATABLE);
    }
    final int offset =  DATA_START_ADR + levelsArrBytes + ITEM_BYTES;
    return wmem.getFloat(offset);
  }

  @Override
  public float getMinItem() {
    int levelsArrBytes = 0;
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    else if (sketchStructure == COMPACT_SINGLE) { return getFloatSingleItem(); }
    else if (sketchStructure == COMPACT_FULL) {
      levelsArrBytes = getLevelsArrSizeBytes(COMPACT_FULL);
    } else { //UPDATABLE
      levelsArrBytes = getLevelsArrSizeBytes(UPDATABLE);
    }
    final int offset =  DATA_START_ADR + levelsArrBytes;
    return wmem.getFloat(offset);
  }

  @Override
  public long getN() {
    if (sketchStructure == COMPACT_EMPTY) { return 0; }
    else if (sketchStructure == COMPACT_SINGLE) { return 1; }
    else { return getMemoryN(wmem); }
  }

  //restricted

  @Override //returns updatable, expanded array including empty/garbage space at bottom
  float[] getFloatItemsArray() {
    final int k = getK();
    if (sketchStructure == COMPACT_EMPTY) { return new float[k]; }
    if (sketchStructure == COMPACT_SINGLE) {
      final float[] itemsArr = new float[k];
      itemsArr[k - 1] = getFloatSingleItem();
      return itemsArr;
    }
    final int capacityItems = KllHelper.computeTotalItemCapacity(k, getM(), getNumLevels());
    final float[] floatItemsArr = new float[capacityItems];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    final int shift = (sketchStructure == COMPACT_FULL) ? levelsArr[0] : 0;
    final int numItems = (sketchStructure == COMPACT_FULL) ? getNumRetained() : capacityItems;
    wmem.getFloatArray(offset, floatItemsArr, shift, numItems);
    return floatItemsArr;
  }

  @Override //returns compact items array of retained items, no empty/garbage.
  float[] getFloatRetainedItemsArray() {
    if (sketchStructure == COMPACT_EMPTY) { return new float[0]; }
    if (sketchStructure == COMPACT_SINGLE) { return new float[] { getFloatSingleItem() }; }
    final int numRetained = getNumRetained();
    final float[] floatItemsArr = new float[numRetained];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES
        + (sketchStructure == COMPACT_FULL ? 0 : levelsArr[0] * ITEM_BYTES);
    wmem.getFloatArray(offset, floatItemsArr, 0, numRetained);
    return floatItemsArr;
  }

  @Override
  float getFloatSingleItem() {
    if (!isSingleItem()) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    if (sketchStructure == COMPACT_SINGLE) {
      return wmem.getFloat(DATA_START_ADR_SINGLE_ITEM);
    }
    final int offset;
    if (sketchStructure == COMPACT_FULL) {
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    } else { //sketchStructure == UPDATABLE
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (2 + getK() - 1) * ITEM_BYTES;
    }
    return wmem.getFloat(offset);
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
      ByteArrayUtil.putFloatLE(bytesOut, 0, Float.NaN);
      ByteArrayUtil.putFloatLE(bytesOut, ITEM_BYTES, Float.NaN);
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
    final float[] fltArr = getFloatRetainedItemsArray();
    final byte[] fltByteArr = new byte[fltArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(fltByteArr);
    wmem2.putFloatArray(0, fltArr, 0, fltArr.length);
    return fltByteArr;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final float[] fltArr = getFloatItemsArray();
    final byte[] fltByteArr = new byte[fltArr.length * ITEM_BYTES];
    final WritableMemory wmem2 = WritableMemory.writableWrap(fltByteArr);
    wmem2.putFloatArray(0, fltArr, 0, fltArr.length);
    return fltByteArr;
  }

  @Override
  WritableMemory getWritableMemory() {
    return wmem;
  }

  @Override
  void incN() {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    long n = getMemoryN(wmem);
    setMemoryN(wmem, ++n);
  }

  @Override
  void incNBy(int increment) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    long n = getMemoryN(wmem);
    setMemoryN(wmem, n + increment);
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
  void setFloatItemsArray(final float[] floatItems) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + 2 * ITEM_BYTES;
    wmem.putFloatArray(offset, floatItems, 0, floatItems.length);
  }

  @Override
  void setFloatItemsArrayAt(final int index, final float item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset =
        DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (index + 2) * ITEM_BYTES;
    wmem.putFloat(offset, item);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemoryLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMaxItem(final float item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    wmem.putFloat(offset, item);
  }

  @Override
  void setMinItem(final float item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    wmem.putFloat(offset, item);
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

  final static class KllDirectCompactFloatsSketch extends KllDirectFloatsSketch {

    KllDirectCompactFloatsSketch(
        final SketchStructure sketchStructure,
        final Memory srcMem,
        final KllMemoryValidate memVal) {
      super(sketchStructure, (WritableMemory) srcMem, null, memVal);
    }
  }

}
