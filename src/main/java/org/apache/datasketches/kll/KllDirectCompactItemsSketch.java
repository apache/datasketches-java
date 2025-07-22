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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentN;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfBooleansSerDe2;
import org.apache.datasketches.common.ArrayOfItemsSerDe2;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This class implements an off-heap, read-only KllItemsSketch using MemorySegment.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
@SuppressWarnings("unchecked")
final class KllDirectCompactItemsSketch<T> extends KllItemsSketch<T> {
  private final MemorySegment seg;

  /**
   * Internal implementation of the wrapped MemorySegment KllSketch.
   * @param segVal the MemoryValadate object
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   */
  KllDirectCompactItemsSketch( //called below and KllItemsSketch
      final KllMemorySegmentValidate segVal,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe2<T> serDe) {
    super(segVal.sketchStructure, comparator, serDe);
    seg = segVal.srcSeg;
    readOnly = true;
    levelsArr = segVal.levelsArr; //always converted to writable form.
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "Null"; }
    return serDe.toString(getTotalItemsArray()[index]);
  }

  @Override
  public int getK() {
    return getMemorySegmentK(seg);
  }

  //MinMax Methods

  @Override
  public T getMaxItem() {
    if ((sketchStructure == COMPACT_EMPTY) || isEmpty()) {
      throw new SketchesArgumentException(EMPTY_MSG);
    }
    if (sketchStructure == COMPACT_SINGLE) {
      return serDe.deserializeFromMemorySegment(seg, DATA_START_ADR_SINGLE_ITEM, 1)[0];
    }
    //sketchStructure == COMPACT_FULL
    final int baseOffset = DATA_START_ADR + (getNumLevels() * Integer.BYTES);
    final int offset = baseOffset + serDe.sizeOf(seg, baseOffset, 1); //size of minItem

    return serDe.deserializeFromMemorySegment(seg, offset, 1)[0];
  }

  @Override
  String getMaxItemAsString() {
    if (isEmpty()) { return "Null"; }
    return serDe.toString(getMaxItem());
  }

  @Override
  public T getMinItem() {
    if ((sketchStructure == COMPACT_EMPTY) || isEmpty()) {
      throw new SketchesArgumentException(EMPTY_MSG);
    }
    if (sketchStructure == COMPACT_SINGLE) {
      return serDe.deserializeFromMemorySegment(seg, DATA_START_ADR_SINGLE_ITEM, 1)[0];
    }
    //sketchStructure == COMPACT_FULL
    final int offset = DATA_START_ADR + (getNumLevels() * Integer.BYTES);
    return serDe.deserializeFromMemorySegment(seg, offset, 1)[0];
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
    return getMemorySegmentN(seg);
  }

  //restricted

  private int getCompactDataOffset() { //Sketch cannot be empty
    return sketchStructure == COMPACT_SINGLE
        ? DATA_START_ADR_SINGLE_ITEM
        : DATA_START_ADR + (getNumLevels() * Integer.BYTES) + getMinMaxSizeBytes();
  }

  @Override
  int getM() {
    return getMemorySegmentM(seg);
  }

  @Override
  int getMinK() {
    if ((sketchStructure == COMPACT_EMPTY) || (sketchStructure == COMPACT_SINGLE)) { return getMemorySegmentK(seg); }
    return getMemorySegmentMinK(seg);
  }

  @Override
  byte[] getMinMaxByteArr() { //this is only used by COMPACT_FULL
    final int offset = DATA_START_ADR + (getNumLevels() * Integer.BYTES);
    final int bytesMinMax = serDe.sizeOf(seg, offset, 2);
    final byte[] byteArr = new byte[bytesMinMax];
    MemorySegment.copy(seg, JAVA_BYTE, offset, byteArr, 0, bytesMinMax);
    return byteArr;
  }

  @Override
  int getMinMaxSizeBytes() { //this is only used by COMPACT_FULL
    final int offset = DATA_START_ADR + (getNumLevels() * Integer.BYTES);
    if (serDe instanceof ArrayOfBooleansSerDe2) { return 2; }
    return serDe.sizeOf(seg, offset, 2);
  }

  @Override
  T[] getRetainedItemsArray() {
    final int numRet = getNumRetained();
    if ((sketchStructure == COMPACT_EMPTY) || (getN() == 0)) {
      return (T[]) Array.newInstance(serDe.getClassOfT(), numRet);
    }
    final int offset = getCompactDataOffset(); //both single & full
    return serDe.deserializeFromMemorySegment(seg, offset, numRet);
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    if ((sketchStructure == COMPACT_EMPTY) || (getN() == 0)) { return new byte[0]; }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(seg, offset, getNumRetained());
    final byte[] byteArr = new byte[bytes];
    MemorySegment.copy(seg, JAVA_BYTE, offset, byteArr, 0, bytes);
    return byteArr;
  }

  @Override
  int getRetainedItemsSizeBytes() {
    if ((sketchStructure == COMPACT_EMPTY) || (getN() == 0)) { return 0; }
    final int offset = getCompactDataOffset(); //both single & full
    return serDe.sizeOf(seg, offset, getNumRetained());
  }

  @Override
  T getSingleItem() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    return (serDe.deserializeFromMemorySegment(seg, offset, 1)[0]);
  }

  @Override
  byte[] getSingleItemByteArr() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(seg, offset, 1);
    final byte[] byteArr = new byte[bytes];
    MemorySegment.copy(seg, JAVA_BYTE, offset, byteArr, 0, bytes);
    return byteArr;
  }

  @Override
  int getSingleItemSizeBytes() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    return serDe.sizeOf(seg, offset, 1);
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
    final T[] retItems = serDe.deserializeFromMemorySegment(seg, offset, numRetItems);
    final T[] capItems = (T[]) Array.newInstance(serDe.getClassOfT(), numCapItems);
    System.arraycopy(retItems, 0, capItems, levelsArr[0], numRetItems);
    return capItems;
  }

  @Override
  MemorySegment getMemorySegment() {
    return seg;
  }

  @Override
  void incN(final int increment) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemorySegmentLevelZeroSortedFlag(seg);
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

  @Override
  public boolean hasMemorySegment() {
    return (seg != null) && seg.scope().isAlive();
  }

  @Override
  public boolean isOffHeap() {
    return hasMemorySegment() && seg.isNative();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(seg, that);
  }

  @Override
  MemorySegmentRequest getMemorySegmentRequest() {
    return null;
  }

}
