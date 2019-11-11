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
   * Gets the index into the array
   * @return the index into the array
   */
  abstract int getIndex();

  /**
   * Gets the key
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
   * Gets the target or actual HLL slot number, which is derived from the key.
   * If in LIST or SET mode this will be the target slot number.
   * In HLL mode, this will be the actual slot number and equal to the key.
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
   * Gets the value
   * @return the value
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
