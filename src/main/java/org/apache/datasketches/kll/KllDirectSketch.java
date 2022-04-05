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

import static org.apache.datasketches.kll.KllPreambleUtil.extractMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractM;
import static org.apache.datasketches.kll.KllPreambleUtil.extractN;
import static org.apache.datasketches.kll.KllPreambleUtil.extractNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.insertMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.insertN;
import static org.apache.datasketches.kll.KllPreambleUtil.insertNumLevels;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_IMMUTABLE;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements all the methods for the Direct (off-heap) sketches that are independent
 * of the sketch type (float or double).
 */
abstract class KllDirectSketch extends KllSketch {
  final boolean updatable = true;
  WritableMemory levelsArrUpdatable;
  WritableMemory minMaxArrUpdatable;
  WritableMemory itemsArrUpdatable;

  /**
   * For the direct sketches it is important that the methods implemented here are designed to work dynamically
   * as the sketch grows off-heap.
   * @param sketchType either DOUBLE_SKETCH or FLOAT_SKETCH
   * @param wmem the current WritableMemory
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   */
  KllDirectSketch(final SketchType sketchType, final WritableMemory wmem, final MemoryRequestServer memReqSvr,
      final KllMemoryValidate memVal) {
    super(sketchType, wmem, memReqSvr);
    levelsArrUpdatable = memVal.levelsArrUpdatable;
    minMaxArrUpdatable = memVal.minMaxArrUpdatable;
    itemsArrUpdatable = memVal.itemsArrUpdatable;
  }

  @Override
  public int getK() {
    return extractK(wmem);
  }

  @Override
  int getM() {
    return extractM(wmem);
  }

  @Override
  public long getN() {
    return extractN(wmem);
  }

  @Override
  public void reset() {
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelsArray(new int[] {k, k});
    setLevelZeroSorted(false);
    final int newLevelsArrLen = 2 * Integer.BYTES;
    final int newItemsArrLen = k;
    KllSketch.memorySpaceMgmt(this, newLevelsArrLen, newItemsArrLen);
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
  int getMinK() {
    return extractMinK(wmem);
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
  int getNumLevels() {
    return extractNumLevels(wmem);
  }

  @Override
  void incN() {
    if (!updatable) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    long n = extractN(wmem);
    insertN(wmem, ++n);
  }

  @Override
  void incNumLevels() {
    if (!updatable) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    int numLevels = extractNumLevels(wmem);
    insertNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return extractLevelZeroSortedFlag(wmem);
  }

  @Override
  void setMinK(final int minK) {
    if (!updatable) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    insertMinK(wmem, minK);
  }

  @Override
  void setItemsArrayUpdatable(final WritableMemory itemsMem) {
    itemsArrUpdatable = itemsMem;
  }

  @Override
  void setLevelsArray(final int[] levelsArr) {
    if (!updatable) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    levelsArrUpdatable.putIntArray(0, levelsArr, 0, levelsArr.length);
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) {
    levelsArrUpdatable.putInt((long)index * Integer.BYTES, value);
  }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    final int offset = index * Integer.BYTES;
    final int curV = levelsArrUpdatable.getInt(offset);
    levelsArrUpdatable.putInt(offset, curV - minusEq);
  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    final int offset = index * Integer.BYTES;
    final int curV = levelsArrUpdatable.getInt(offset);
    levelsArrUpdatable.putInt(offset, curV + plusEq);
  }

  @Override
  void setLevelsArrayUpdatable(final WritableMemory levelsMem) {
    levelsArrUpdatable = levelsMem;
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (!updatable) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    insertLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMinMaxArrayUpdatable(final WritableMemory minMaxMem) {
    minMaxArrUpdatable = minMaxMem;
  }

  @Override
  void setN(final long n) {
    if (!updatable) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    insertN(wmem, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (!updatable) { kllSketchThrow(TGT_IS_IMMUTABLE); }
    insertNumLevels(wmem, numLevels);
  }

}
