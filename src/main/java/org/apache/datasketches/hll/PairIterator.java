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

/**
 * @author Lee Rhodes
 */
abstract class PairIterator {

  /**
   * Gets the header string for a list of pairs
   * @return the header string for a list of pairs
   */
  String getHeader() {
    return String.format("%10s%10s%10s%6s", "Index", "Key", "Slot", "Value");
  }

  /**
   * In LIST and SET modes, this gets the iterating index into the integer array of HLL key/value
   * pairs.
   * In HLL mode, this is the iterating index into the hypothetical array of HLL values, which may
   * be physically contructed differently based on the compaction scheme (HLL_4, HLL_6, HLL_8).
   * @return the index.
   */
  abstract int getIndex();

  /**
   * Gets the key, the low 26 bits of an pair, and can be up to 26 bits in length.
   * @return the key
   */
  abstract int getKey();

  /**
   * Gets the key, value pair as a single int where the key is the lower 26 bits
   * and the value is in the upper 6 bits.
   * @return the key, value pair.
   */
  abstract int getPair();

  /**
   * Gets the target or actual HLL slot number, which is derived from the key and LgConfigK.
   * The slot number is the index into a hypothetical array of length K and has LgConfigK bits.
   * If in LIST or SET mode this is the index into the hypothetical target HLL array of size K.
   * In HLL mode, this will be the actual index into the hypothetical target HLL array of size K.
   * @return the target or actual HLL slot number.
   */
  abstract int getSlot();

  /**
   * Gets the current pair as a string
   * @return the current pair as a string
   */
  String getString() {
    final int index = getIndex();
    final int key = getKey();
    final int slot = getSlot();
    final int value = getValue();
    return String.format("%10d%10d%10d%6d", index, key, slot, value);
  }

  /**
   * Gets the HLL value of a particular slot or pair.
   * @return the HLL value of a particular slot or pair.
   */
  abstract int getValue();

  /**
   * Returns true at the next pair in sequence.
   * If false, the iteration is done.
   * @return true at the next pair in sequence.
   */
  abstract boolean nextAll();

  /**
   * Returns true at the next pair where getKey() and getValue() are valid.
   * If false, the iteration is done.
   * @return true at the next pair where getKey() and getValue() are valid.
   */
  abstract boolean nextValid();

}
