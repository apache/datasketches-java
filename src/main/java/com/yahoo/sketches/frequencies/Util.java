/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

final class Util {

  private Util() {}

  /**
   * The following constant controls the size of the initial data structure for the 
   * frequencies sketches and its value is somewhat arbitrary.
   */
  static final int LG_MIN_MAP_SIZE = 3;

  /**
   * This constant is large enough so that computing the median of SAMPLE_SIZE
   * randomly selected entries from a list of numbers and outputting
   * the empirical median will give a constant-factor approximation to the 
   * true median with high probability.
   */
  static final int SAMPLE_SIZE = 1024;

  /**
   * @param key to be hashed
   * @return an index into the hash table This hash function is taken from the internals of 
   * Austin Appleby's MurmurHash3 algorithm. It is also used by the Trove for Java libraries.
   */
  static long hash(long key) {
    key ^= key >>> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >>> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >>> 33;
    return key;
  }

}
