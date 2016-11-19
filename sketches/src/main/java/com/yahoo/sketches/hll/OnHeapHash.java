/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Kevin Lang
 */
final class OnHeapHash {
  private int[] fields_;
  private int mask;
  private int numElements;

  OnHeapHash(final int startSize) {
    resetFields(startSize);
  }

  void resetFields(final int size) {
    this.fields_ = new int[size];
    Arrays.fill(this.fields_, -1);
    this.mask = fields_.length - 1;
    this.numElements = 0;
  }

  int[] getFields() {
    return fields_;
  }

  public int getNumElements() {
    return numElements;
  }

  void updateBucket(final int key, final byte val, final Fields.UpdateCallback callback) {
    updateBucket(key, val, HashUtils.pairOfKeyAndVal(key, val), callback);
  }

  private int updateBucket(final int key, final byte val, final int newField,
      final Fields.UpdateCallback callback) {
    int probe = key & mask;
    int field = fields_[probe];
    while (field != HashUtils.NOT_A_PAIR && key != HashUtils.keyOfPair(field)) {
      probe = (probe + 1) & mask;
      field = fields_[probe];
    }

    if (field == HashUtils.NOT_A_PAIR) {
      fields_[probe] = newField;
      callback.bucketUpdated(key, (byte) 0, val);
      ++numElements;
    }

    final byte oldVal = HashUtils.valOfPair(field);
    if (oldVal < val) {
      fields_[probe] = newField;
      callback.bucketUpdated(key, oldVal, val);
      ++numElements;
    }

    return numElements;
  }

  int intoByteArray(final byte[] array, int offset) {
    final int numBytesNeeded = numBytesToSerialize();
    if (array.length - offset < numBytesNeeded) {
      throw new SketchesArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    final Memory mem = new NativeMemory(array);

    for (int field : fields_) {
      mem.putInt(offset, field);
      offset += 4;
    }

    return offset;
  }

  int numBytesToSerialize() {
    return (fields_.length << 2);
  }

  BucketIterator getBucketIterator() {
    return new BucketIterator() {
      private int i = -1;

      @Override
      public boolean next() {
        ++i;
        while (i < fields_.length && fields_[i] == HashUtils.NOT_A_PAIR) {
          ++i;
        }
        return i < fields_.length;
      }

      @Override
      public int getKey() {
        return HashUtils.keyOfPair(fields_[i]);
      }

      @Override
      public byte getValue() {
        return HashUtils.valOfPair(fields_[i]);
      }
    };
  }

  void boostrap(final int[] fields) {
    for (int field : fields) {
      if (field != HashUtils.NOT_A_PAIR) {
        updateBucket(HashUtils.keyOfPair(field), HashUtils.valOfPair(field), field, Fields.NOOP_CB);
      }
    }
  }
}
