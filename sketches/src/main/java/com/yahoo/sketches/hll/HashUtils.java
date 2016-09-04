/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * @author Kevin Lang
 */
final class HashUtils {
  static int NOT_A_PAIR = -1;

  private static final int KEY_BITS = 25;
  private static final int KEY_MASK = (1 << KEY_BITS) - 1;

  private static final int VAL_BITS = 6;
  private static final int VAL_MASK = (1 << VAL_BITS) - 1;

  private static final int[] MAX_HASH_SIZE = new int[] {
      4, 16, 16, 16,
      16, 16, 16, 32,
      32, 32, 64, 64,
      128, 128, 256, 512,
      1024, 2048, 4096, 8192,
      16384, 32768, 65536, 131072,
      262144, 524288, 1048576
  };

  private HashUtils() {}

  static int getMaxHashSize(int index) {
    return MAX_HASH_SIZE[index];
  }

  static int keyOfPair(int pair) {
    return pair & KEY_MASK;
  }

  static byte valOfPair(int pair) {
    return (byte) ((pair >> KEY_BITS) & VAL_MASK);
  }

  static int pairOfKeyAndVal(int key, byte value) {
    return ((value & VAL_MASK) << KEY_BITS) | (key & KEY_MASK);
  }
}
