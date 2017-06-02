/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * @author Lee Rhodes
 */
interface PairIterator {

  /**
   * Returns true at the next pair where getKey() and getValue() are valid.
   * If false, the iteration is done.
   * @return true at the next pair where getKey() and getValue() are valid.
   */
  boolean nextValid();

  /**
   * Returns true at the next pair in sequence.
   * If false, the iteration is done.
   * @return true at the next pair in sequence.
   */
  boolean nextAll();

  /**
   * Gets the key, value pair as a single int where the key is the lower 26 bits
   * and the value is in the upper 6 bits.
   * @return the key, value pair.
   */
  int getPair();

  /**
   * Gets the key
   * @return the key
   */
  int getKey();

  /**
   * Gets the value
   * @return the value
   */
  int getValue();

  /**
   * Gets the index
   * @return the index
   */
  int getIndex();

  /**
   * Gets the header string for a list of pairs
   * @return the header string for a list of pairs
   */
  default String getHeader() {
    return String.format("%10s%10s%6s", "Index", "Key", "Value");
  }

  /**
   * Gets the current pair as a string
   * @return the current pair as a string
   */
  default String getString() {
    final int index = getIndex();
    final int key = getKey();
    final int value = getValue();
    return String.format("%10d%10d%6d", index, key, value);
  }

}
