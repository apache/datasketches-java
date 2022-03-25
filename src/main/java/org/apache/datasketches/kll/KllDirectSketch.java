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

import static org.apache.datasketches.kll.KllPreambleUtil.extractDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractN;
import static org.apache.datasketches.kll.KllPreambleUtil.extractNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.insertDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.insertN;
import static org.apache.datasketches.kll.KllPreambleUtil.insertNumLevels;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import org.apache.datasketches.kll.KllPreambleUtil.Layout;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;


abstract class KllDirectSketch extends KllSketch {
  //All these members are constant for the life of this object. If the WritableMemory changes, it will require
  //rebuilding this class
  final Layout layout;
  final boolean updatable;
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
  KllDirectSketch(final SketchType sketchType, final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
    super(sketchType, wmem, memReqSvr);
    final MemoryValidate memVal = new MemoryValidate(wmem);
    layout = memVal.layout;
    updatable = memVal.updatable;
    if (!updatable) { kllSketchThrow(31); }
    levelsArrUpdatable = memVal.levelsArrUpdatable;
    minMaxArrUpdatable = memVal.minMaxArrUpdatable;
    itemsArrUpdatable = memVal.itemsArrUpdatable;
  }

  @Override
  public int getK() {
    return extractK(wmem);
  }

  @Override
  double getMaxDoubleValue() {
    return minMaxArrUpdatable.getDouble(Double.BYTES);
  }

  @Override
  float getMaxFloatValue() {
    return minMaxArrUpdatable.getFloat(Float.BYTES);
  }

  @Override
  double getMinDoubleValue() {
    return minMaxArrUpdatable.getDouble(0);
  }

  @Override
  float getMinFloatValue() {
    return minMaxArrUpdatable.getFloat(0);
  }

  @Override
  public long getN() {
    return extractN(wmem);
  }

  @Override
  double[] getDoubleItemsArray() {
    if (sketchType == FLOATS_SKETCH) { return null; }
    final int items = getItemsArrLengthItems();
    final double[] itemsArr = new double[items];
    itemsArrUpdatable.getDoubleArray(0, itemsArr, 0, items);
    return itemsArr;
  }

  @Override
  double getDoubleItemsArrayAt(final int index) {
    if (sketchType == FLOATS_SKETCH) { return Double.NaN; }
    return itemsArrUpdatable.getDouble(index * Double.BYTES);
  }

  @Override
  int getDyMinK() {
    return extractDyMinK(wmem);
  }

  @Override
  float[] getFloatItemsArray() {
    if (sketchType == DOUBLES_SKETCH) { return null; }
    final int items = getItemsArrLengthItems();
    final float[] itemsArr = new float[items];
    itemsArrUpdatable.getFloatArray(0, itemsArr, 0, items);
    return itemsArr;
  }

  @Override
  float getFloatItemsArrayAt(final int index) {
    if (sketchType == DOUBLES_SKETCH) { return Float.NaN; }
    return itemsArrUpdatable.getFloat(index * Float.BYTES);
  }

  int getItemsArrLengthItems() {
    return getLevelsArray()[getNumLevels()];
  }

  @Override
  String getLayout() { return layout.toString(); }

  @Override
  int[] getLevelsArray() {
    final int numInts = getNumLevels() + 1;
    final int[] myLevelsArr = new int[numInts];
    levelsArrUpdatable.getIntArray(0, myLevelsArr, 0, numInts);
    return myLevelsArr;
  }

  @Override
  int getLevelsArrayAt(final int index) {
    return levelsArrUpdatable.getInt(index * Integer.BYTES);
  }

  @Override
  int getNumLevels() {
    return extractNumLevels(wmem);
  }

  @Override
  void incN() {
    if (!updatable) { kllSketchThrow(30); }
    long n = extractN(wmem);
    insertN(wmem, ++n);
  }

  @Override
  void incNumLevels() {
    if (!updatable) { kllSketchThrow(30); }
    int numLevels = extractNumLevels(wmem);
    insertNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return extractLevelZeroSortedFlag(wmem);
  }

  @Override
  void setDoubleItemsArray(final double[] doubleItems) {
    if (!updatable) { kllSketchThrow(30); }
    itemsArrUpdatable.putDoubleArray(0, doubleItems, 0, doubleItems.length);
  }

  @Override
  void setDoubleItemsArrayAt(final int index, final double value) {
    itemsArrUpdatable.putDouble(index * Double.BYTES, value);
  }

  @Override
  void setDyMinK(final int dyMinK) {
    if (!updatable) { kllSketchThrow(30); }
    insertDyMinK(wmem, dyMinK);
  }

  @Override
  void setFloatItemsArray(final float[] floatItems) {
    if (!updatable) { kllSketchThrow(30); }
    itemsArrUpdatable.putFloatArray(0, floatItems, 0, floatItems.length);
  }

  @Override
  void setFloatItemsArrayAt(final int index, final float value) {
    itemsArrUpdatable.putFloat(index * Float.BYTES, value);
  }

  @Override
  void setItemsArrayUpdatable(final WritableMemory itemsMem) {
    itemsArrUpdatable = itemsMem;
  }

  @Override
  void setLevelsArray(final int[] levelsArr) {
    if (!updatable) { kllSketchThrow(30); }
    levelsArrUpdatable.putIntArray(0, levelsArr, 0, levelsArr.length);
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) {
    levelsArrUpdatable.putInt(index * Integer.BYTES, value);
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
    if (!updatable) { kllSketchThrow(30); }
    insertLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setMaxDoubleValue(final double value) {
    if (!updatable) { kllSketchThrow(30); }
    minMaxArrUpdatable.putDouble(Double.BYTES, value);
  }

  @Override
  void setMaxFloatValue(final float value) {
    if (!updatable) { kllSketchThrow(30); }
    minMaxArrUpdatable.putFloat(Float.BYTES, value);
  }

  @Override
  void setMinDoubleValue(final double value) {
    if (!updatable) { kllSketchThrow(30); }
    minMaxArrUpdatable.putDouble(0, value);
  }

  @Override
  void setMinFloatValue(final float value) {
    if (!updatable) { kllSketchThrow(30); }
    minMaxArrUpdatable.putFloat(0, value);
  }

  @Override
  void setMinMaxArrayUpdatable(final WritableMemory minMaxMem) {
    minMaxArrUpdatable = minMaxMem;
  }

  @Override
  void setN(final long n) {
    if (!updatable) { kllSketchThrow(30); }
    insertN(wmem, n);
  }


  @Override
  void setNumLevels(final int numLevels) {
    if (!updatable) { kllSketchThrow(30); }
    insertNumLevels(wmem, numLevels);
  }

  @Override
  public byte[] toUpdatableByteArray() {
    final int bytes = (int) wmem.getCapacity();
    final byte[] byteArr = new byte[bytes];
    wmem.getByteArray(0, byteArr, 0, bytes);
    return byteArr;
  }

}
