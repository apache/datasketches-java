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
import static org.apache.datasketches.common.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySerVer;
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
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an off-heap, updatable floats KllSketch using WritableMemory.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
class KllDirectFloatsSketch extends KllFloatsSketch {

  /**
   * The constructor with WritableMemory that can be off-heap.
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  KllDirectFloatsSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr, final KllMemoryValidate memVal) {
    super(wmem, memReqSvr);
    levelsArr = memVal.levelsArr; //converted to writable form if required.
  }

  /**
   * Create a new instance of this sketch.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param m parameter that controls the minimum level width in items.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new instance of this sketch
   */
  static KllDirectFloatsSketch newDirectInstance(final int k, final int m, final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    setMemoryPreInts(dstMem, PREAMBLE_INTS_FULL);
    setMemorySerVer(dstMem, SERIAL_VERSION_UPDATABLE);
    setMemoryFamilyID(dstMem, Family.KLL.getID());
    setMemoryK(dstMem, k);
    setMemoryM(dstMem, m);
    setMemoryN(dstMem, 0);
    setMemoryMinK(dstMem, k);
    setMemoryNumLevels(dstMem, 1);
    int offset = DATA_START_ADR;
    dstMem.putIntArray(offset, new int[] {k, k}, 0, 2);
    offset += 2 * Integer.BYTES;
    dstMem.putFloatArray(offset, new float[] {Float.NaN, Float.NaN}, 0, 2);
    offset += 2 * ITEM_BYTES;
    dstMem.putFloatArray(offset, new float[k], 0, k);
    final KllMemoryValidate memVal = new KllMemoryValidate(dstMem, FLOATS_SKETCH, null);
    return new KllDirectFloatsSketch(dstMem, memReqSvr, memVal);
  }

  private int levelsArrBytes() {
    return Integer.BYTES * (serialVersionUpdatable ? getLevelsArray().length : getLevelsArray().length - 1);
  }

  @Override
  public int getK() {
    return getMemoryK(wmem);
  }

  @Override
  public float getMaxItem() {
    if (isEmpty()) { kllSketchThrow(EMPTY); }
    if (isSingleItem()) { return getFloatSingleItem(); }
    final int offset =  DATA_START_ADR + levelsArrBytes() + ITEM_BYTES;
    return wmem.getFloat(offset);
  }

  @Override
  public float getMinItem() {
    if (isEmpty()) { kllSketchThrow(EMPTY); }
    if (isSingleItem()) { return getFloatSingleItem(); }
    final int offset =  DATA_START_ADR + levelsArrBytes();
    return wmem.getFloat(offset);
  }

  @Override
  public long getN() {
    if (getMemoryPreInts(wmem) == PREAMBLE_INTS_FULL) { return getMemoryN(wmem); }
    else if (getMemorySerVer(wmem) == SERIAL_VERSION_SINGLE) { return 1; }
    else { return 0; }
  }

  //restricted

  @Override //returns updatable, expanded array including empty space at bottom
  float[] getFloatItemsArray() {
    final int k = getK();
    if (isEmpty()) { return new float[k]; }
    if (isSingleItem()) {
      final float[] itemsArr = new float[k];
      itemsArr[k - 1] = getFloatSingleItem();
      return itemsArr;
    }
    final int capacityItems = KllHelper.computeTotalItemCapacity(k, getM(), getNumLevels());
    final float[] floatItemsArr = new float[capacityItems];
    final int offset = DATA_START_ADR + levelsArrBytes() + 2 * ITEM_BYTES;
    final int shift = serialVersionUpdatable ? 0 : levelsArr[0];
    wmem.getFloatArray(offset, floatItemsArr, shift, capacityItems - shift);
    return floatItemsArr;
  }

  @Override //returns items array of retained items.
  float[] getFloatRetainedItemsArray() {
    if (isEmpty()) { return new float[0]; }
    if (isSingleItem()) { return new float[] { getFloatSingleItem() }; }
    final int numRetained = getNumRetained();
    final float[] out = new float[numRetained];
    final int offset = DATA_START_ADR + levelsArrBytes() + 2 * ITEM_BYTES;
    wmem.getFloatArray(offset, out, 0, numRetained);
    return out;
  }

  @Override
  float getFloatSingleItem() {
    if (!isSingleItem()) { kllSketchThrow(NOT_SINGLE_ITEM); }
    if (getMemoryPreInts(wmem) == PREAMBLE_INTS_FULL) {
      final int k = getK();
      final int offset = DATA_START_ADR + levelsArrBytes() + (2 + k - 1) * ITEM_BYTES; //2 for min/max
      return wmem.getFloat(offset);
    }
    return wmem.getFloat(DATA_START_ADR_SINGLE_ITEM);
  }

  @Override
  int getM() {
    return getMemoryM(wmem);
  }

  @Override
  int getMinK() {
    if (getMemoryPreInts(wmem) == PREAMBLE_INTS_FULL) {
      return getMemoryMinK(wmem);
    } else { return getK(); }
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * ITEM_BYTES];
    if (getMemoryPreInts(wmem) == PREAMBLE_INTS_FULL) {
      int offset = DATA_START_ADR + getLevelsArray().length * Integer.BYTES;
      wmem.getByteArray(offset, bytesOut, 0, ITEM_BYTES);
      offset += ITEM_BYTES;
      wmem.getByteArray(offset, bytesOut, ITEM_BYTES, ITEM_BYTES);
    } else if (getMemorySerVer(wmem) == SERIAL_VERSION_SINGLE) {
      final int offset = DATA_START_ADR_SINGLE_ITEM;
      wmem.getByteArray(offset, bytesOut, 0, ITEM_BYTES);
      copyBytes(bytesOut, 0, bytesOut, ITEM_BYTES, ITEM_BYTES);
    } else { //empty
      ByteArrayUtil.putFloatLE(bytesOut, 0, Float.NaN);
      ByteArrayUtil.putFloatLE(bytesOut, ITEM_BYTES, Float.NaN);
    }
    return bytesOut;
  }

  @Override
  byte[] getRetainedDataByteArr() {
    if (isEmpty()) { return new byte[0]; }
    final byte[] bytesOut;
    if (isSingleItem()) {
      bytesOut = new byte[ITEM_BYTES];
      putFloatLE(bytesOut, 0, getFloatSingleItem());
      return bytesOut;
    }
    final int retained = getNumRetained();
    final int bytes = retained * ITEM_BYTES;
    bytesOut = new byte[bytes];
    final WritableMemory wmem = WritableMemory.writableWrap(bytesOut);
    wmem.putFloatArray(0, getFloatItemsArray(), levelsArr[0], retained);
    return bytesOut;
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
  void setFloatItemsArray(final float[] floatItems) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset = DATA_START_ADR + levelsArrBytes() + 2 * ITEM_BYTES;
    wmem.putFloatArray(offset, floatItems, 0, floatItems.length);
  }

  @Override
  void setFloatItemsArrayAt(final int index, final float item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset =
        DATA_START_ADR + levelsArrBytes() + (index + 2) * ITEM_BYTES;
    wmem.putFloat(offset, item);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    setMemoryLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMaxItem(final float item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset = DATA_START_ADR + levelsArrBytes() + ITEM_BYTES;
    wmem.putFloat(offset, item);
  }

  @Override
  void setMinItem(final float item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int offset = DATA_START_ADR + levelsArrBytes();
    wmem.putFloat(offset, item);
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
