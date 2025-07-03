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
import static org.apache.datasketches.hll2.HllUtil.EMPTY;

import java.lang.foreign.MemorySegment;

/**
 * Iterates within a given Memory extracting integer pairs.
 *
 * @author Lee Rhodes
 */
class IntMemorySegmentPairIterator extends PairIterator {
  private final MemorySegment seg;
  private final long offsetBytes;
  private final int arrLen;
  private final int slotMask;
  private int index;
  private int pair;

  //Used by DirectAuxHashMap, DirectCouponList
  IntMemorySegmentPairIterator(
      final MemorySegment seg, final long offsetBytes, final int arrayLength, final int lgConfigK) {
    this.seg = seg;
    this.offsetBytes = offsetBytes;
    this.arrLen = arrayLength;
    slotMask = (1 << lgConfigK) - 1;
    index = -1;
  }

  IntMemorySegmentPairIterator(final byte[] byteArr, final long offsetBytes, final int lengthPairs,
      final int lgConfigK) {
    this(MemorySegment.ofArray(byteArr), offsetBytes, lengthPairs, lgConfigK);
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public int getKey() {
    return HllUtil.getPairLow26(pair);
  }

  @Override
  public int getPair() {
    return pair;
  }

  @Override
  public int getSlot() {
    return getKey() & slotMask;
  }

  @Override
  public int getValue() {
    return HllUtil.getPairValue(pair);
  }

  @Override
  public boolean nextAll() {
    if (++index < arrLen) {
      pair = pair();
      return true;
    }
    return false;
  }

  @Override
  public boolean nextValid() {
    while (++index < arrLen) {
      final int pair = pair();
      if (pair != EMPTY) {
        this.pair = pair;
        return true;
      }
    }
    return false;
  }

  int pair() {
    return seg.get(JAVA_INT_UNALIGNED, offsetBytes + (index << 2));
  }

}
