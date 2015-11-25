/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

/**
 * @author Kevin Lang
 */
class CompressedBucketUtils {
  private static final int LO_NIBBLE_MASK = 0x0f;
  private static final int HI_NIBBLE_MASK = 0xf0;

  static byte getNibble(byte[] buckets, int index) {
    byte theByte = buckets[index >> 1];
    return (byte) (((index & 1) == 0 ? theByte >> 4 : theByte) & LO_NIBBLE_MASK);
  }

  static void setNibble(byte[] buckets, int index, byte newValue) {
    int byteno = index >> 1;
    byte oldValue = buckets[byteno];
    if ((index & 1) == 0) {
      buckets[byteno] = (byte) (((newValue << 4) & HI_NIBBLE_MASK) | (oldValue & LO_NIBBLE_MASK));
    } 
    else {
      buckets[byteno] = (byte) ((oldValue & HI_NIBBLE_MASK) | (newValue & LO_NIBBLE_MASK));
    }
  }

  static void updateNibble(byte[] buckets, int index, byte newNibble, Fields.UpdateCallback callback) {
    if (newNibble < 0) {
      return;
    }

    int byteno = index >> 1;
    byte oldValue = buckets[byteno];
    int oldLowNibble = oldValue & LO_NIBBLE_MASK;
    int oldHighNibble = oldValue & HI_NIBBLE_MASK;

    if ((index & 1) == 0) {
      int newHighNibble = newNibble << 4;
      if (oldHighNibble < newHighNibble) {
        buckets[byteno] = (byte) (newHighNibble | oldLowNibble);
        int oldNib = oldHighNibble >> 4;
        callback.bucketUpdated(index, (byte) oldNib, newNibble);
      }
    } 
    else {
      int newLoNibble = newNibble & LO_NIBBLE_MASK;
      if (oldLowNibble < newLoNibble) {
        buckets[byteno] = (byte) (oldHighNibble | newLoNibble);
        callback.bucketUpdated(index, (byte) oldLowNibble, newNibble);
      }
    }
  }

  static BucketIterator getBucketIterator(final byte[] buckets, final int currMin, OnHeapHash exceptions) {
    BucketIterator exceptionsIter = exceptions.getBucketIterator();
    BucketIterator nibblesIter = new BucketIterator() {
      private int i = -1;
      private int size = buckets.length << 1;

      private byte nibble;

      @Override
      public boolean next() {
        ++i;
        while (i < size) {
          nibble = CompressedBucketUtils.getNibble(buckets, i);
          if (nibble > 0 && nibble < 0x0f) {
            break;
          }
          ++i;
        }
        return i < size;
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
