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
import static org.apache.datasketches.hll.HllUtil.noWriteAccess;
import static org.apache.datasketches.hll.PreambleUtil.extractAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.extractLgArr;
import static org.apache.datasketches.hll.PreambleUtil.insertAuxCount;
import static org.apache.datasketches.hll.PreambleUtil.insertLgArr;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class DirectAuxHashMap implements AuxHashMap {
  private final DirectHllArray host; //hosts the WritableMemory and read-only Memory
  private final boolean readOnly;

  DirectAuxHashMap(final DirectHllArray host, final boolean initialize) {
    this.host = host;
    readOnly = (host.wmem == null);
    final int initLgArrInts = HllUtil.LG_AUX_ARR_INTS[host.lgConfigK];

    if (initialize) { //must be writable
      if (readOnly) { noWriteAccess(); }
      insertLgArr(host.wmem, initLgArrInts);
      host.wmem.clear(host.auxStart, 4 << initLgArrInts);
    } else {
      if (extractLgArr(host.mem) < initLgArrInts) {
        if (readOnly) {
          throw new SketchesArgumentException(
              "Possible Memory image corruption, incorrect LgArr field in preamble.");
        }
        //insert the correct LgArr value
        final int lgArr =
            PreambleUtil.computeLgArr(host.wmem, host.auxHashMap.getAuxCount(), host.lgConfigK);
        insertLgArr(host.wmem, lgArr);
      }
    }
  }

  @Override
  public DirectAuxHashMap copy() { //a no-op
    return null;
  }

  @Override
  public int getAuxCount() {
    return extractAuxCount(host.mem);
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
    return new IntMemoryPairIterator(
        host.mem, host.auxStart, 1 << getLgAuxArrInts(), host.lgConfigK);
  }

  @Override
  public int getLgAuxArrInts() {
    return extractLgArr(host.mem);
  }

  @Override
  public int getUpdatableSizeBytes() {
    return 4 << getLgAuxArrInts();
  }

  @Override
  public boolean isMemory() {
    return true;
  }

  @Override
  public boolean isOffHeap() {
    return host.isOffHeap();
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
    host.wmem.putInt(host.auxStart + (~index << 2), pair);
    int auxCount = extractAuxCount(host.mem);
    insertAuxCount(host.wmem, ++auxCount);
    final int lgAuxArrInts = extractLgArr(host.mem);
    if ((RESIZE_DENOM * auxCount) > (RESIZE_NUMER * (1 << lgAuxArrInts))) {
      grow(host, lgAuxArrInts);
    }
  }

  @Override
  public int mustFindValueFor(final int slotNo) {
    final int index = find(host, slotNo);
    if (index >= 0) {
      final int pair = host.mem.getInt(host.auxStart + (index << 2));
      return HllUtil.getPairValue(pair);
    }
    throw new SketchesStateException("SlotNo not found: " + slotNo);
  }

  @Override
  public void mustReplace(final int slotNo, final int value) {
    if (readOnly) { noWriteAccess(); }
    final int index = find(host, slotNo);
    if (index >= 0) {
      host.wmem.putInt(host.auxStart + (index << 2), HllUtil.pair(slotNo, value));
      return;
    }
    final String pairStr = HllUtil.pairString(HllUtil.pair(slotNo, value));
    throw new SketchesStateException("Pair not found: " + pairStr);
  }

  //Searches the Aux arr hash table (embedded in Memory) for an empty or a matching slotNo
  //  depending on the context.
  //If entire entry is empty, returns one's complement of index = found empty.
  //If entry contains given slotNo, returns its index = found slotNo.
  //Continues searching.
  //If the probe comes back to original index, throws an exception.
  private static final int find(final DirectHllArray host, final int slotNo) {
    final int lgAuxArrInts = extractLgArr(host.mem);
    assert lgAuxArrInts < host.lgConfigK : lgAuxArrInts;
    final int auxInts = 1 << lgAuxArrInts;
    final int auxArrMask = auxInts - 1;
    final int configKmask = (1 << host.lgConfigK) - 1;
    int probe = slotNo & auxArrMask;
    final int loopIndex = probe;
    do {
      final int arrVal = host.mem.getInt(host.auxStart + (probe << 2));
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

  private static final void grow(final DirectHllArray host, final int oldLgAuxArrInts) {
    if (host.wmem == null) { noWriteAccess(); }
    final int oldAuxArrInts = 1 << oldLgAuxArrInts;
    final int[] oldIntArray = new int[oldAuxArrInts]; //buffer old aux data
    host.wmem.getIntArray(host.auxStart, oldIntArray, 0, oldAuxArrInts);

    insertLgArr(host.wmem, oldLgAuxArrInts + 1); //update LgArr field

    final long newAuxBytes = oldAuxArrInts << 3;
    final long requestBytes = host.auxStart + newAuxBytes;
    final long oldCapBytes = host.wmem.getCapacity();

    if (requestBytes > oldCapBytes) {
      final MemoryRequestServer svr = host.wmem.getMemoryRequestServer();
      final WritableMemory newWmem = svr.request(requestBytes);
      host.wmem.copyTo(0, newWmem, 0, host.auxStart);
      newWmem.clear(host.auxStart, newAuxBytes); //clear space for new aux data
      svr.requestClose(host.wmem, newWmem); //old host.wmem is now invalid
      host.updateMemory(newWmem);
    }
    //rehash into larger aux array
    final int configKmask = (1 << host.lgConfigK) - 1;

    for (int i = 0; i < oldAuxArrInts; i++) {
      final int fetched = oldIntArray[i];
      if (fetched != EMPTY) {
        //find empty in new array
        final int index = find(host, fetched & configKmask);
        host.wmem.putInt(host.auxStart + (~index << 2), fetched);
      }
    }
  }

}
