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
import static org.apache.datasketches.hll.HllUtil.pair;

/**
 * Iterates over an on-heap HLL byte array producing pairs of index, value.
 *
 * @author Lee Rhodes
 */
abstract class HllPairIterator extends PairIterator {
  final int lengthPairs;
  int index;
  int value;

  //Used by Direct<4,6,8>Array, Heap<4,6,8>Array
  HllPairIterator(final int lengthPairs) {
    this.lengthPairs = lengthPairs;
    index = - 1;
  }

  @Override
  public String getHeader() {
    return String.format("%10s%6s", "Slot", "Value");
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public int getKey() {
    return index;
  }

  @Override
  public int getPair() {
    return pair(index, value);
  }

  @Override
  public int getSlot() {
    return index;
  }

  @Override
  public String getString() {
    final int slot = getSlot();
    final int value = getValue();
    return String.format("%10d%6d", slot, value);
  }

  @Override
  public int getValue() {
    return value;
  }

  @Override
  public boolean nextAll() {
    if (++index < lengthPairs) {
      value = value();
      return true;
    }
    return false;
  }

  @Override
  public boolean nextValid() {
    while (++index < lengthPairs) {
      value = value();
      if (value != EMPTY) {
        return true;
      }
    }
    return false;
  }

  abstract int value();

}
