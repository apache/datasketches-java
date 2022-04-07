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

import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.UPDATABLE_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySerVer;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_IMMUTABLE;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

//intentional extra line
/**
 * This class implements an off-heap floats KllSketch via a WritableMemory instance of the sketch.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
final class KllDirectFloatsSketch extends KllFloatsSketch {
  final boolean updatableMemory;
  WritableMemory levelsArrUpdatable;
  WritableMemory minMaxArrUpdatable;
  WritableMemory itemsArrUpdatable;

  /**
   * The actual constructor
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @param memVal the MemoryValadate object
   */
  KllDirectFloatsSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr,
   final KllMemoryValidate memVal) {
   super(SketchType.FLOATS_SKETCH, wmem, memReqSvr);
   updatableMemory = memVal.updatableMemory && memReqSvr != null;
   levelsArrUpdatable = memVal.levelsArrUpdatable;
   minMaxArrUpdatable = memVal.minMaxArrUpdatable;
   itemsArrUpdatable = memVal.itemsArrUpdatable;
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
    setMemoryFlags(dstMem, UPDATABLE_BIT_MASK);
    setMemoryK(dstMem, k);
    setMemoryM(dstMem, m);
    setMemoryN(dstMem, 0);
    setMemoryMinK(dstMem, k);
    setMemoryNumLevels(dstMem, 1);
    int offset = DATA_START_ADR;
    dstMem.putIntArray(offset, new int[] {k, k}, 0, 2);
    offset += 2 * Integer.BYTES;
    dstMem.putFloatArray(offset, new float[] {Float.NaN, Float.NaN}, 0, 2);
    offset += 2 * Float.BYTES;
    dstMem.putFloatArray(offset, new float[k], 0, k);
    final KllMemoryValidate memVal = new KllMemoryValidate(dstMem);
    return new KllDirectFloatsSketch(dstMem, memReqSvr, memVal);
  }

  @Override
  public int getK() {
    return getMemoryK(wmem);
  }

  @Override
  public long getN() {
    return getMemoryN(wmem);
  }

  @Override
  public void reset() {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelsArray(new int[] {k, k});
    setLevelZeroSorted(false);
    final int newLevelsArrLen = 2 * Integer.BYTES;
    final int newItemsArrLen = k;
    KllHelper.memorySpaceMgmt(this, newLevelsArrLen, newItemsArrLen);
    levelsArrUpdatable.putIntArray(0L, new int[] {k, k}, 0, 2);
    if (sketchType == SketchType.DOUBLES_SKETCH) {
      minMaxArrUpdatable.putDoubleArray(0L, new double[] {Double.NaN, Double.NaN}, 0, 2);
      itemsArrUpdatable.putDoubleArray(0L, new double[k], 0, k);
    } else {
      minMaxArrUpdatable.putFloatArray(0L, new float[] {Float.NaN, Float.NaN}, 0, 2);
      itemsArrUpdatable.putFloatArray(0L, new float[k], 0, k);
    }
  }

  @Override
  public byte[] toUpdatableByteArray() {
    final int bytes = (int) wmem.getCapacity();
    final byte[] byteArr = new byte[bytes];
    wmem.getByteArray(0, byteArr, 0, bytes);
    return byteArr;
  }

  @Override
  float[] getFloatItemsArray() {
    final int items = getItemsArrLengthItems();
    final float[] itemsArr = new float[items];
    itemsArrUpdatable.getFloatArray(0, itemsArr, 0, items);
    return itemsArr;
  }

  @Override
  float getFloatItemsArrayAt(final int index) {
    return itemsArrUpdatable.getFloat((long)index * Float.BYTES);
  }

  int getItemsArrLengthItems() {
    return getLevelsArray()[getNumLevels()];
  }

  @Override
  int[] getLevelsArray() {
    final int numInts = getNumLevels() + 1;
    final int[] myLevelsArr = new int[numInts];
    levelsArrUpdatable.getIntArray(0, myLevelsArr, 0, numInts);
    return myLevelsArr;
  }

  @Override
  int getLevelsArrayAt(final int index) {
    return levelsArrUpdatable.getInt((long)index * Integer.BYTES);
  }

  @Override
  int getM() {
    return getMemoryM(wmem);
  }

  @Override
  float getMaxFloatValue() {
    return minMaxArrUpdatable.getFloat(Float.BYTES);
  }

  @Override
  float getMinFloatValue() {
    return minMaxArrUpdatable.getFloat(0);
  }

  @Override
  int getMinK() {
    return getMemoryMinK(wmem);
  }

  @Override
  int getNumLevels() {
    return getMemoryNumLevels(wmem);
  }

  @Override
  void incN() {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    long n = getMemoryN(wmem);
    setMemoryN(wmem, ++n);
  }

  @Override
  void incNumLevels() {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    int numLevels = getMemoryNumLevels(wmem);
    setMemoryNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemoryLevelZeroSortedFlag(wmem);
  }

  @Override
  void setFloatItemsArray(final float[] floatItems) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    itemsArrUpdatable.putFloatArray(0, floatItems, 0, floatItems.length);
  }

  @Override
  void setFloatItemsArrayAt(final int index, final float value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    itemsArrUpdatable.putFloat((long)index * Float.BYTES, value);
  }

  @Override
  void setItemsArrayUpdatable(final WritableMemory itemsMem) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    itemsArrUpdatable = itemsMem;
  }

  @Override
  void setLevelsArray(final int[] levelsArr) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    levelsArrUpdatable.putIntArray(0, levelsArr, 0, levelsArr.length);
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    levelsArrUpdatable.putInt((long)index * Integer.BYTES, value);
  }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    final int offset = index * Integer.BYTES;
    final int curV = levelsArrUpdatable.getInt(offset);
    levelsArrUpdatable.putInt(offset, curV - minusEq);
  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    final int offset = index * Integer.BYTES;
    final int curV = levelsArrUpdatable.getInt(offset);
    levelsArrUpdatable.putInt(offset, curV + plusEq);
  }

  @Override
  void setLevelsArrayUpdatable(final WritableMemory levelsMem) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    levelsArrUpdatable = levelsMem;
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMaxFloatValue(final float value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    minMaxArrUpdatable.putFloat(Float.BYTES, value);
  }

  @Override
  void setMinFloatValue(final float value) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    minMaxArrUpdatable.putFloat(0, value);
  }

  @Override
  void setMinK(final int minK) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryMinK(wmem, minK);
  }

  @Override
  void setMinMaxArrayUpdatable(final WritableMemory minMaxMem) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    minMaxArrUpdatable = minMaxMem;
  }

  @Override
  void setN(final long n) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryN(wmem, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (!updatableMemory) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    setMemoryNumLevels(wmem, numLevels);
  }

}
