/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.hll.AuxHashMap.pair;
import static com.yahoo.sketches.hll.AuxHashMap.pairString;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertAuxCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;

import com.yahoo.memory.MemoryRequestServer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
class DirectAuxHashMap extends DirectHll4Array implements AuxHashMap {
  private final int auxArrOffset;
  private long unsafeAuxArrOffset;

  DirectAuxHashMap(final WritableMemory wmem) {
    super(wmem);
    auxArrOffset = HLL_BYTE_ARRAY_START + (1 << (lgConfigK - 1));
    unsafeAuxArrOffset = memAdd + auxArrOffset;
  }

  @Override
  public DirectAuxHashMap copy() { //a no-op
    return null;
  }

  @Override
  public int getAuxCount() {
    return extractAuxCount(memObj, memAdd);
  }

  @Override
  public int[] getAuxIntArr() {
    return null;
  }

  @Override
  public int getCompactedSizeBytes() {
    return getAuxCount() << 2;
  }

  @Override
  public PairIterator getIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getLgAuxArrInts() {
    return extractLgArr(memObj, memAdd);
  }

  @Override
  public int getUpdatableSizeBytes() {
    return 4 << getLgAuxArrInts();
  }

  @Override
  public void mustAdd(final int slotNo, final int value) {
    final int index = find(slotNo);
    final int pair = pair(slotNo, value);
    if (index >= 0) {
      final String pairStr = pairString(pair);
      throw new SketchesStateException("Found a slotNo that should not be there: " + pairStr);
    }
    //Found empty entry
    unsafe.putInt(memObj, unsafeAuxArrOffset + (~index << 2), pair);
    int auxCount = extractAuxCount(memObj, memAdd);
    insertAuxCount(memObj, memAdd, ++auxCount);
    checkGrow(auxCount);
  }

  @Override
  public int mustFindValueFor(final int slotNo) {
    final int index = find(slotNo);
    if (index >= 0) {
      final int pair = unsafe.getInt(memObj, unsafeAuxArrOffset + (index << 2));
      return BaseHllSketch.getValue(pair);
    }
    throw new SketchesStateException("SlotNo not found: " + slotNo);
  }

  @Override
  public void mustReplace(final int slotNo, final int value) {
    final int index = find(slotNo);
    if (index >= 0) {
      unsafe.putInt(memObj, unsafeAuxArrOffset + (index << 2), pair(slotNo, value));
      return;
    }
    final String pairStr = pairString(pair(slotNo, value));
    throw new SketchesStateException("Pair not found: " + pairStr);
  }

  private final int find(final int slotNo) {
    final int lgAuxArrInts = extractLgArr(memObj, memAdd);
    assert lgAuxArrInts < lgConfigK;
    final int auxInts = 1 << lgAuxArrInts;
    final int auxArrMask = auxInts - 1;
    final int configKmask = (1 << lgConfigK) - 1;
    int probe = slotNo & auxArrMask;
    final int loopIndex = probe;
    do {
      final int arrVal = unsafe.getInt(memObj, unsafeAuxArrOffset + (probe << 2));
      if (arrVal == EMPTY) {
        return ~probe; //empty
      }
      else if (slotNo == (arrVal & configKmask)) { //found given slotNo
        return probe; //return aux array index
      }
      final int stride = (slotNo >>> lgAuxArrInts) | 1;
      probe = (probe + stride) & auxArrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  final void checkGrow(final int auxCount) {
    int lgAuxArrInts = extractLgArr(memObj, memAdd);
    if ((RESIZE_DENOM * auxCount) > (RESIZE_NUMER * (1 << lgAuxArrInts))) {
      insertLgArr(memObj, memAdd, ++lgAuxArrInts);
      final long requestBytes = auxArrOffset + (4 << lgAuxArrInts);
      final long oldCapBytes = wmem.getCapacity();
      if (requestBytes > oldCapBytes) {
        final MemoryRequestServer svr = wmem.getMemoryRequestServer();
        final WritableMemory newWmem = svr.request(requestBytes);
        wmem.copyTo(0, newWmem, 0, oldCapBytes); //also copies old auxArr
        super.updateMemory(newWmem);
        unsafeAuxArrOffset = memAdd + auxArrOffset; //memAdd may be different!
      }
      growAuxSpace();
    }
  }

  //lgArr has been incremented and there is sufficient space.
  final void growAuxSpace() {
    final int auxArrInts = 1 << extractLgArr(memObj, memAdd);
    final int[] oldArray = new int[auxArrInts];
    wmem.getIntArray(auxArrOffset, oldArray, 0, auxArrInts);
    final int configKmask = (1 << lgConfigK) - 1;
    wmem.clear(auxArrOffset, auxArrInts << 2);
    for (int i = 0; i < auxArrInts; i++) {
      final int fetched = oldArray[i];
      if (fetched != EMPTY) {
        //find empty in new array
        final int index = find(fetched & configKmask);
        unsafe.putInt(memObj, unsafeAuxArrOffset + (~index << 2), fetched);
      }
    }
  }

}
