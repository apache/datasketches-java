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

package org.apache.datasketches.quantiles2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.quantiles2.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles2.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles2.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.quantiles2.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.quantiles2.PreambleUtil.insertFlags;
import static org.apache.datasketches.quantiles2.PreambleUtil.insertK;
import static org.apache.datasketches.quantiles2.PreambleUtil.insertN;
import static org.apache.datasketches.quantiles2.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.quantiles2.PreambleUtil.insertSerVer;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.datasketches.common.ArrayOfItemsSerDe2;
import org.apache.datasketches.common.Family;

/**
 * The items to byte array algorithms.
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 */
final class ItemsByteArrayImpl {

  private ItemsByteArrayImpl() {}

  static <T> byte[] toByteArray(final ItemsSketch<T> sketch, final boolean ordered, final ArrayOfItemsSerDe2<T> serDe) {
    final boolean empty = sketch.isEmpty();

    final int flags = (empty ? EMPTY_FLAG_MASK : 0)
        | (ordered ? ORDERED_FLAG_MASK : 0)
        | COMPACT_FLAG_MASK; //always compact

    if (empty) {
      final byte[] outByteArr = new byte[Long.BYTES];
      final MemorySegment segOut = MemorySegment.ofArray(outByteArr);
      final int preLongs = 1;
      insertPre0(segOut, preLongs, flags, sketch.getK());
      return outByteArr;
    }

    //not empty
    final T[] dataArr = combinedBufferToItemsArray(sketch, ordered); //includes min and max

    final int preLongs = 2;
    final byte[] itemsByteArr = serDe.serializeToByteArray(dataArr);
    final int numOutBytes = (preLongs << 3) + itemsByteArr.length;
    final byte[] outByteArr = new byte[numOutBytes];
    final MemorySegment segOut = MemorySegment.ofArray(outByteArr);

    //insert preamble
    insertPre0(segOut, preLongs, flags, sketch.getK());
    insertN(segOut, sketch.getN());

    //insert data
    MemorySegment.copy(itemsByteArr, 0, segOut, JAVA_BYTE, preLongs << 3, itemsByteArr.length);
    return outByteArr;
  }

  /**
   * Returns an array of items in compact form, including min and max extracted from the
   * Combined Buffer.
   * @param <T> the data type
   * @param sketch a type of ItemsSketch
   * @param ordered true if the desired form of the resulting array has the base buffer sorted.
   * @return an array of items, including min and max extracted from the Combined Buffer.
   */
  @SuppressWarnings("unchecked")
  private static <T> T[] combinedBufferToItemsArray(final ItemsSketch<T> sketch,
      final boolean ordered) {
    final int extra = 2; // extra space for min and max items
    final int outArrCap = sketch.getNumRetained();
    final T minItem = sketch.getMinItem();
    final T[] outArr = (T[]) Array.newInstance(minItem.getClass(), outArrCap + extra);

    //Load min, max
    outArr[0] = minItem;
    outArr[1] = sketch.getMaxItem();
    final int baseBufferCount = sketch.getBaseBufferCount();
    final Object[] combinedBuffer = sketch.getCombinedBuffer();

    //Load base buffer
    System.arraycopy(combinedBuffer, 0, outArr, extra, baseBufferCount);

    //Load levels
    long bitPattern = sketch.getBitPattern();
    if (bitPattern > 0) {
      final int k = sketch.getK();
      int index = extra + baseBufferCount;
      for (int level = 0; bitPattern != 0L; level++, bitPattern >>>= 1) {
        if ((bitPattern & 1L) > 0L) {
          System.arraycopy(combinedBuffer, (2 + level) * k, outArr, index, k);
          index += k;
        }
      }
    }
    if (ordered) {
      Arrays.sort(outArr, extra, baseBufferCount + extra, sketch.getComparator());
    }
    return outArr;
  }

  private static void insertPre0(final MemorySegment wseg,
      final int preLongs, final int flags, final int k) {
    insertPreLongs(wseg, preLongs);
    insertSerVer(wseg, ItemsUtil.ITEMS_SER_VER);
    insertFamilyID(wseg, Family.QUANTILES.getID());
    insertFlags(wseg, flags);
    insertK(wseg, k);
  }

}
