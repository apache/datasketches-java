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

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;

import com.yahoo.memory.Memory;

/**
 * Iterates within a given Memory extracting integer pairs.
 *
 * @author Lee Rhodes
 */
class IntMemoryPairIterator implements PairIterator {
  final Memory mem;
  final long offsetBytes;
  final int lengthPairs;
  final int slotMask;
  int index;
  int pair;

  IntMemoryPairIterator(final Memory mem, final long offsetBytes, final int lengthPairs,
      final int lgConfigK) {
    this.mem = mem;
    this.offsetBytes = offsetBytes;
    this.lengthPairs = lengthPairs;
    slotMask = (1 << lgConfigK) - 1;
    index = -1;
  }

  IntMemoryPairIterator(final byte[] byteArr, final long offsetBytes, final int lengthPairs,
      final int lgConfigK) {
    this(Memory.wrap(byteArr), offsetBytes, lengthPairs, lgConfigK);
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public int getKey() {
    return HllUtil.getLow26(pair);
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
    return HllUtil.getValue(pair);
  }

  @Override
  public boolean nextAll() {
    if (++index < lengthPairs) {
      pair = pair();
      return true;
    }
    return false;
  }

  @Override
  public boolean nextValid() {
    while (++index < lengthPairs) {
      final int pair = pair();
      if (pair != EMPTY) {
        this.pair = pair;
        return true;
      }
    }
    return false;
  }

  int pair() {
    return mem.getInt(offsetBytes + (index << 2));
  }

}
