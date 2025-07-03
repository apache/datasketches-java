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

package org.apache.datasketches.hll2;

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.Util.clear;
import static org.apache.datasketches.hll2.HllUtil.EMPTY;
import static org.apache.datasketches.hll2.HllUtil.RESIZE_DENOM;
import static org.apache.datasketches.hll2.HllUtil.RESIZE_NUMER;
import static org.apache.datasketches.hll2.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll2.PreambleUtil.extractAuxCount;
import static org.apache.datasketches.hll2.PreambleUtil.extractLgArr;
import static org.apache.datasketches.hll2.PreambleUtil.insertAuxCount;
import static org.apache.datasketches.hll2.PreambleUtil.insertLgArr;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;

/**
 * @author Lee Rhodes
 */
final class DirectAuxHashMap implements AuxHashMap {
  private final DirectHllArray host; //hosts the MemorySegment and read-only MemorySegment
  private final boolean readOnly;

  DirectAuxHashMap(final DirectHllArray host, final boolean initialize) {
    this.host = host;
    readOnly = (host.wseg == null);
    final int initLgArrInts = HllUtil.LG_AUX_ARR_INTS[host.lgConfigK];

    if (initialize) { //must be writable
      if (readOnly) { noWriteAccess(); }
      insertLgArr(host.wseg, initLgArrInts);
      clear(host.wseg, host.auxStart, 4 << initLgArrInts);
    } else if (extractLgArr(host.seg) < initLgArrInts) {
      if (readOnly) {
        throw new SketchesArgumentException(
            "Possible MemorySegment image corruption, incorrect LgArr field in preamble.");
      }
      //insert the correct LgArr value
      final int lgArr =
          PreambleUtil.computeLgArr(host.wseg, host.auxHashMap.getAuxCount(), host.lgConfigK);
      insertLgArr(host.wseg, lgArr);
    }
  }

  @Override
  public DirectAuxHashMap copy() { //a no-op
    return null;
  }

  @Override
  public int getAuxCount() {
    return extractAuxCount(host.seg);
  }

  @Override
  public int[] getAuxIntArr() {
    return null;
  }

  @Override
  public int getCompactSizeBytes() {
    return getAuxCount() << 2;
  }

  @Override
  public PairIterator getIterator() {
    return new IntMemorySegmentPairIterator(
        host.seg, host.auxStart, 1 << getLgAuxArrInts(), host.lgConfigK);
  }

  @Override
  public int getLgAuxArrInts() {
    return extractLgArr(host.seg);
  }

  @Override
  public int getUpdatableSizeBytes() {
    return 4 << getLgAuxArrInts();
  }

  @Override
  public boolean hasMemorySegment() {
    return host.hasMemorySegment();
  }

  @Override
  public boolean isOffHeap() {
    return host.isOffHeap();
  }

  @Override
  public boolean isSameResource(final MemorySegment seg) {
    return host.isSameResource(seg);
  }

  @Override
  public void mustAdd(final int slotNo, final int value) {
    if (readOnly) { noWriteAccess(); }
    final int index = find(host, slotNo);
    final int pair = HllUtil.pair(slotNo, value);
    if (index >= 0) {
      final String pairStr = HllUtil.pairString(pair);
      throw new SketchesStateException("Found a slotNo that should not be there: " + pairStr);
    }
    //Found empty entry
    host.wseg.set(JAVA_INT_UNALIGNED, host.auxStart + (~index << 2), pair);
    int auxCount = extractAuxCount(host.seg);
    insertAuxCount(host.wseg, ++auxCount);
    final int lgAuxArrInts = extractLgArr(host.seg);
    if ((RESIZE_DENOM * auxCount) > (RESIZE_NUMER * (1 << lgAuxArrInts))) {
      grow(host, lgAuxArrInts);
    }
  }

  @Override
  public int mustFindValueFor(final int slotNo) {
    final int index = find(host, slotNo);
    if (index >= 0) {
      final int pair = host.seg.get(JAVA_INT_UNALIGNED, host.auxStart + (index << 2));
      return HllUtil.getPairValue(pair);
    }
    throw new SketchesStateException("SlotNo not found: " + slotNo);
  }

  @Override
  public void mustReplace(final int slotNo, final int value) {
    if (readOnly) { noWriteAccess(); }
    final int index = find(host, slotNo);
    if (index >= 0) {
      host.wseg.set(JAVA_INT_UNALIGNED, host.auxStart + (index << 2), HllUtil.pair(slotNo, value));
      return;
    }
    final String pairStr = HllUtil.pairString(HllUtil.pair(slotNo, value));
    throw new SketchesStateException("Pair not found: " + pairStr);
  }

  //Searches the Aux arr hash table (embedded in MemorySegment) for an empty or a matching slotNo
  //  depending on the context.
  //If entire entry is empty, returns one's complement of index = found empty.
  //If entry contains given slotNo, returns its index = found slotNo.
  //Continues searching.
  //If the probe comes back to original index, throws an exception.
  private static int find(final DirectHllArray host, final int slotNo) {
    final int lgAuxArrInts = extractLgArr(host.seg);
    assert lgAuxArrInts < host.lgConfigK : lgAuxArrInts;
    final int auxInts = 1 << lgAuxArrInts;
    final int auxArrMask = auxInts - 1;
    final int configKmask = (1 << host.lgConfigK) - 1;
    int probe = slotNo & auxArrMask;
    final int loopIndex = probe;
    do {
      final int arrVal = host.seg.get(JAVA_INT_UNALIGNED, host.auxStart + (probe << 2));
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

  private static void grow(final DirectHllArray host, final int oldLgAuxArrInts) {
    if (host.wseg == null) { noWriteAccess(); }
    final int oldAuxArrInts = 1 << oldLgAuxArrInts;
    final int[] oldIntArray = new int[oldAuxArrInts]; //buffer old aux data
    MemorySegment.copy(host.wseg, JAVA_INT_UNALIGNED, host.auxStart, oldIntArray, 0, oldAuxArrInts);

    insertLgArr(host.wseg, oldLgAuxArrInts + 1); //update LgArr field

    final long newAuxBytes = oldAuxArrInts << 3;
    final long requestBytes = host.auxStart + newAuxBytes;
    final long oldCapBytes = host.wseg.byteSize();

    if (requestBytes > oldCapBytes) {
      final MemorySegment newWseg = MemorySegment.ofArray(new byte[(int)requestBytes]);
      MemorySegment.copy(host.wseg, 0, newWseg, 0, host.auxStart);

      clear(newWseg, host.auxStart, newAuxBytes); //clear space for new aux data
      host.updateMemorySegment(newWseg);
    }
    //rehash into larger aux array
    final int configKmask = (1 << host.lgConfigK) - 1;

    for (int i = 0; i < oldAuxArrInts; i++) {
      final int fetched = oldIntArray[i];
      if (fetched != EMPTY) {
        //find empty in new array
        final int index = find(host, fetched & configKmask);
        host.wseg.set(JAVA_INT_UNALIGNED, host.auxStart + (~index << 2), fetched);
      }
    }
  }

}
