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

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.HllUtil.EMPTY;
import static org.apache.datasketches.hll.HllUtil.RESIZE_DENOM;
import static org.apache.datasketches.hll.HllUtil.RESIZE_NUMER;
import static org.apache.datasketches.hll.PreambleUtil.extractInt;
import static org.apache.datasketches.hll.PreambleUtil.extractLgArr;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class HeapAuxHashMap implements AuxHashMap {
  private final int lgConfigK; //required for #slot bits
  private int lgAuxArrInts;
  private int auxCount;
  private int[] auxIntArr; //used by Hll4Array

  /**
   * Standard constructor
   * @param lgAuxArrInts the log size of the aux integer array
   * @param lgConfigK must be 7 to 21
   */
  HeapAuxHashMap(final int lgAuxArrInts, final int lgConfigK) {
    this.lgConfigK = lgConfigK;
    this.lgAuxArrInts = lgAuxArrInts;
    auxIntArr = new int[1 << lgAuxArrInts];
  }

  /**
   * Copy constructor
   * @param that another AuxHashMap
   */
  HeapAuxHashMap(final HeapAuxHashMap that) {
    lgConfigK = that.lgConfigK;
    lgAuxArrInts = that.lgAuxArrInts;
    auxCount = that.auxCount;
    auxIntArr = that.auxIntArr.clone();
  }

  static HeapAuxHashMap heapify(final MemorySegment seg, final long offset, final int lgConfigK,
      final int auxCount, final boolean srcCompact) {
    final int lgAuxArrInts;
    final HeapAuxHashMap auxMap;
    if (srcCompact) { //early versions did not use LgArr byte field
      lgAuxArrInts = PreambleUtil.computeLgArr(seg, auxCount, lgConfigK);
    } else { //updatable
      lgAuxArrInts = extractLgArr(seg);
    }
    auxMap = new HeapAuxHashMap(lgAuxArrInts, lgConfigK);

    final int configKmask = (1 << lgConfigK) - 1;

    if (srcCompact) {
      for (int i = 0; i < auxCount; i++) {
        final int pair = extractInt(seg, offset + (i << 2));
        final int slotNo = HllUtil.getPairLow26(pair) & configKmask;
        final int value = HllUtil.getPairValue(pair);
        auxMap.mustAdd(slotNo, value); //increments count
      }
    } else { //updatable
      final int auxArrInts = 1 << lgAuxArrInts;
      for (int i = 0; i < auxArrInts; i++) {
        final int pair = extractInt(seg, offset + (i << 2));
        if (pair == EMPTY) { continue; }
        final int slotNo = HllUtil.getPairLow26(pair) & configKmask;
        final int value = HllUtil.getPairValue(pair);
        auxMap.mustAdd(slotNo, value); //increments count
      }
    }
    return auxMap;
  }

  @Override
  public HeapAuxHashMap copy() {
    return new HeapAuxHashMap(this);
  }

  @Override
  public int getAuxCount() {
    return auxCount;
  }

  @Override
  public int[] getAuxIntArr() {
    return auxIntArr;
  }

  @Override
  public int getCompactSizeBytes() {
    return auxCount << 2;
  }

  @Override
  public PairIterator getIterator() {
    return new IntArrayPairIterator(auxIntArr, lgConfigK);
  }

  @Override
  public int getLgAuxArrInts() {
    return lgAuxArrInts;
  }

  @Override
  public int getUpdatableSizeBytes() {
    return 4 << lgAuxArrInts;
  }

  @Override
  public boolean hasMemorySegment() {
    return false;
  }

  @Override
  public boolean isOffHeap() {
    return false;
  }

  @Override
  public boolean isSameResource(final MemorySegment seg) {
    return false;
  }

  //In C: two-registers.c Line 300.
  @Override
  public void mustAdd(final int slotNo, final int value) {
    final int index = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
    final int pair = HllUtil.pair(slotNo, value);
    if (index >= 0) {
      final String pairStr = HllUtil.pairString(pair);
      throw new SketchesStateException("Found a slotNo that should not be there: " + pairStr);
    }
      //Found empty entry
      auxIntArr[~index] = pair;
      auxCount++;
      checkGrow();
  }

  //In C: two-registers.c Line 205
  @Override
  public int mustFindValueFor(final int slotNo) {
    final int index = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
    if (index >= 0) {
      return HllUtil.getPairValue(auxIntArr[index]);
    }
    throw new SketchesStateException("SlotNo not found: " + slotNo);
  }

  //In C: two-registers.c Line 321.
  @Override
  public void mustReplace(final int slotNo, final int value) {
    final int idx = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
    if (idx >= 0) {
      auxIntArr[idx] = HllUtil.pair(slotNo, value); //replace
      return;
    }
    final String pairStr = HllUtil.pairString(HllUtil.pair(slotNo, value));
    throw new SketchesStateException("Pair not found: " + pairStr);
  }

  //Searches the Aux arr hash table for an empty or a matching slotNo depending on the context.
  //If entire entry is empty, returns one's complement of index = found empty.
  //If entry contains given slotNo, returns its index = found slotNo.
  //Continues searching.
  //If the probe comes back to original index, throws an exception.
  private static int find(final int[] auxArr, final int lgAuxArrInts, final int lgConfigK,
      final int slotNo) {
    assert lgAuxArrInts < lgConfigK;
    final int auxArrMask = (1 << lgAuxArrInts) - 1;
    final int configKmask = (1 << lgConfigK) - 1;
    int probe = slotNo & auxArrMask;
    final int loopIndex = probe;
    do {
      final int arrVal = auxArr[probe];
      if (arrVal == EMPTY) { //Compares on entire entry
        return ~probe; //empty
      }
      else if (slotNo == (arrVal & configKmask)) { //Compares only on slotNo
        return probe; //found given slotNo, return probe = index into aux array
      }
      final int stride = (slotNo >>> lgAuxArrInts) | 1;
      probe = (probe + stride) & auxArrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  private void checkGrow() {
    if ((RESIZE_DENOM * auxCount) > (RESIZE_NUMER * auxIntArr.length)) {
      growAuxSpace();
    }
  }

  private void growAuxSpace() {
    final int[] oldArray = auxIntArr;
    final int configKmask = (1 << lgConfigK) - 1;
    auxIntArr = new int[1 << ++lgAuxArrInts];
    for (int i = 0; i < oldArray.length; i++) {
      final int fetched = oldArray[i];
      if (fetched != EMPTY) {
        //find empty in new array
        final int idx = find(auxIntArr, lgAuxArrInts, lgConfigK, fetched & configKmask);
        auxIntArr[~idx] = fetched;
      }
    }
  }

}
