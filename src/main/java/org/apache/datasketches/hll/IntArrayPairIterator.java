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

/**
 * Iterates over an on-heap integer array extracting pairs.
 *
 * @author Lee Rhodes
 */
class IntArrayPairIterator implements PairIterator {
  private final int[] array;
  private final int slotMask;
  private final int lengthPairs;
  private int index;
  private int pair;

  IntArrayPairIterator(final int[] array, final int lgConfigK) {
    this.array = array;
    slotMask = (1 << lgConfigK) - 1;
    lengthPairs = array.length;
    index = - 1;
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
      pair = array[index];
      return true;
    }
    return false;
  }

  @Override
  public boolean nextValid() {
    while (++index < lengthPairs) {
      final int pair = array[index];
      if (pair != EMPTY) {
        this.pair = pair;
        return true;
      }
    }
    return false;
  }

}
