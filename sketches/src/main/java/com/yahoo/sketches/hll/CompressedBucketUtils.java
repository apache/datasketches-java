/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * @author Kevin Lang
 */
final class CompressedBucketUtils {
  private static final int LO_NIBBLE_MASK = 0x0f;
  private static final int HI_NIBBLE_MASK = 0xf0;

  private CompressedBucketUtils() {}

  static byte getNibble(final byte[] buckets, final int index) {
    final byte theByte = buckets[index >> 1];
    //if index is even grab the hi nibble, else the lo nibble
    return (byte) (((index & 1) == 0 ? theByte >> 4 : theByte) & LO_NIBBLE_MASK);
  }

  static void setNibble(final byte[] buckets, final int index, final byte newValue) {
    final int byteno = index >> 1;
    final byte oldValue = buckets[byteno];
    if ((index & 1) == 0) { //if even replace the hi nibble
      buckets[byteno] = (byte) (((newValue << 4) & HI_NIBBLE_MASK) | (oldValue & LO_NIBBLE_MASK));
    }
    else { //odd: replace the lo nibble
      buckets[byteno] = (byte) ((oldValue & HI_NIBBLE_MASK) | (newValue & LO_NIBBLE_MASK));
    }
  }

  static void updateNibble(final byte[] buckets, final int index, final byte newNibble,
      final Fields.UpdateCallback callback) {
    if (newNibble < 0) {
      return;
    }

    final int byteno = index >> 1;
    final byte oldValue = buckets[byteno];
    final int oldLowNibble = oldValue & LO_NIBBLE_MASK;
    final int oldHighNibble = oldValue & HI_NIBBLE_MASK;

    if ((index & 1) == 0) {
      final int newHighNibble = newNibble << 4;
      if (oldHighNibble < newHighNibble) {
        buckets[byteno] = (byte) (newHighNibble | oldLowNibble);
        final int oldNib = oldHighNibble >> 4;
        callback.bucketUpdated(index, (byte) oldNib, newNibble);
      }
    }
    else {
      final int newLoNibble = newNibble & LO_NIBBLE_MASK;
      if (oldLowNibble < newLoNibble) {
        buckets[byteno] = (byte) (oldHighNibble | newLoNibble);
        callback.bucketUpdated(index, (byte) oldLowNibble, newNibble);
      }
    }
  }

  static BucketIterator getBucketIterator(final byte[] buckets, final int currMin,
      final OnHeapHash exceptions) {
    final BucketIterator exceptionsIter = exceptions.getBucketIterator();
    final BucketIterator nibblesIter = new BucketIterator() {
      private int i = -1;
      private int size = buckets.length << 1; // 2 nibbles / byte

      private byte nibble;

      @Override
      public boolean next() {
        ++i;
        while (i < size) {
          nibble = CompressedBucketUtils.getNibble(buckets, i);
          if ((nibble > 0) && (nibble < 0x0f)) {
            break;
          }
          ++i;
        }
        return i < size;
      }

      @Override
      public boolean nextAll() {
        nibble = CompressedBucketUtils.getNibble(buckets, i);
        return ++i < size;
      }

      @Override
      public int getKey() {
        return i;
      }

      @Override
      public byte getValue() {
        return (byte) (currMin + nibble);
      }
    };

    return new CompositeBucketIterator(nibblesIter, exceptionsIter);
  }
}
