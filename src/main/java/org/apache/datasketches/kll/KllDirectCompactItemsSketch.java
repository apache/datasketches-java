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
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;

import java.lang.reflect.Array;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an off-heap, read-only KllItemsSketch using WritableMemory.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
@SuppressWarnings("unchecked")
final class KllDirectCompactItemsSketch<T> extends KllItemsSketch<T> {
  private Memory mem;

  /**
   * Internal implementation of the wrapped Memory KllSketch.
   * @param memVal the MemoryValadate object
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   */
  KllDirectCompactItemsSketch( //called below and KllItemsSketch
      final KllMemoryValidate memVal,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(memVal.sketchStructure, comparator, serDe);
    this.mem = memVal.srcMem;
    readOnly = true;
    levelsArr = memVal.levelsArr; //always converted to writable form.
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "Null"; }
    return serDe.toString(getTotalItemsArray()[index]);
  }

  @Override
  public int getK() {
    return getMemoryK(mem);
  }

  //MinMax Methods

  @Override
  public T getMaxItem() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) {
      throw new SketchesArgumentException(EMPTY_MSG);
    }
    if (sketchStructure == COMPACT_SINGLE) {
      return serDe.deserializeFromMemory(mem, DATA_START_ADR_SINGLE_ITEM, 1)[0];
    }
    //sketchStructure == COMPACT_FULL
    final int baseOffset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    final int offset = baseOffset + serDe.sizeOf(mem, baseOffset, 1); //size of minItem

    return serDe.deserializeFromMemory(mem, offset, 1)[0];
  }

  @Override
  String getMaxItemAsString() {
    if (isEmpty()) { return "Null"; }
    return serDe.toString(getMaxItem());
  }

  @Override
  public T getMinItem() {
    if (sketchStructure == COMPACT_EMPTY || isEmpty()) {
      throw new SketchesArgumentException(EMPTY_MSG);
    }
    if (sketchStructure == COMPACT_SINGLE) {
      return serDe.deserializeFromMemory(mem, DATA_START_ADR_SINGLE_ITEM, 1)[0];
    }
    //sketchStructure == COMPACT_FULL
    final int offset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    return serDe.deserializeFromMemory(mem, offset, 1)[0];
  }

  @Override
  String getMinItemAsString() {
    if (isEmpty()) { return "Null"; }
    return serDe.toString(getMinItem());
  }

  @Override
  public long getN() {
    if (sketchStructure == COMPACT_EMPTY) { return 0; }
    if (sketchStructure == COMPACT_SINGLE) { return 1; }
    return getMemoryN(mem);
  }

  //restricted

  private int getCompactDataOffset() { //Sketch cannot be empty
    return sketchStructure == COMPACT_SINGLE
        ? DATA_START_ADR_SINGLE_ITEM
        : DATA_START_ADR + getNumLevels() * Integer.BYTES + getMinMaxSizeBytes();
  }

  @Override
  int getM() {
    return getMemoryM(mem);
  }

  @Override
  int getMinK() {
    if (sketchStructure == COMPACT_EMPTY || sketchStructure == COMPACT_SINGLE) { return getMemoryK(mem); }
    return getMemoryMinK(mem);
  }

  @Override
  byte[] getMinMaxByteArr() { //this is only used by COMPACT_FULL
    final int offset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    final int bytesMinMax = serDe.sizeOf(mem, offset, 2);
    final byte[] byteArr = new byte[bytesMinMax];
    mem.getByteArray(offset, byteArr, 0, bytesMinMax);
    return byteArr;
  }

  @Override
  int getMinMaxSizeBytes() { //this is only used by COMPACT_FULL
    final int offset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    if (serDe instanceof ArrayOfBooleansSerDe) { return 2; }
    return serDe.sizeOf(mem, offset, 2);
  }

  @Override
  T[] getRetainedItemsArray() {
    final int numRet = getNumRetained();
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) {
      return (T[]) Array.newInstance(serDe.getClassOfT(), numRet);
    }
    final int offset = getCompactDataOffset(); //both single & full
    return serDe.deserializeFromMemory(mem, offset, numRet);
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return new byte[0]; }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(mem, offset, getNumRetained());
    final byte[] byteArr = new byte[bytes];
    mem.getByteArray(offset, byteArr, 0, bytes);
    return byteArr;
  }

  @Override
  int getRetainedItemsSizeBytes() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return 0; }
    final int offset = getCompactDataOffset(); //both single & full
    return serDe.sizeOf(mem, offset, getNumRetained());
  }

  @Override
  T getSingleItem() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    return (serDe.deserializeFromMemory(mem, offset, 1)[0]);
  }

  @Override
  byte[] getSingleItemByteArr() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(mem, offset, 1);
    final byte[] byteArr = new byte[bytes];
    mem.getByteArray(offset, byteArr, 0, bytes);
    return byteArr;
  }

  @Override
  int getSingleItemSizeBytes() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(mem, offset, 1);
    return bytes;
  }

  @Override
  T[] getTotalItemsArray() {
    final int k = getK();
    if (getN() == 0) { return (T[]) Array.newInstance(serDe.getClassOfT(), k); }
    if (getN() == 1) {
      final T[] itemsArr = (T[]) Array.newInstance(serDe.getClassOfT(), k);
      itemsArr[k - 1] = getSingleItem();
      return itemsArr;
    }
    final int offset = getCompactDataOffset();
    final int numRetItems = getNumRetained();
    final int numCapItems = levelsArr[getNumLevels()];
    final T[] retItems = serDe.deserializeFromMemory(mem, offset, numRetItems);
    final T[] capItems = (T[]) Array.newInstance(serDe.getClassOfT(), numCapItems);
    System.arraycopy(retItems, 0, capItems, levelsArr[0], numRetItems);
    return capItems;
  }

  @Override
  int getTotalItemsNumBytesInternal()
  {
    return getRetainedItemsSizeBytes();
  }

  @Override
  WritableMemory getWritableMemory() {
    return (WritableMemory)mem;
  }

  @Override
  void incN(final int increment) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemoryLevelZeroSortedFlag(mem);
  }

  @Override
  void setItemsArray(final Object[] ItemsArr) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setItemsArrayAt(final int index, final Object item) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setMaxItem(final Object item) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setMinItem(final Object item) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setMinK(final int minK) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setN(final long n) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

}
