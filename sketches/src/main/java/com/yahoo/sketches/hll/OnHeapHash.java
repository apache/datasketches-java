/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 */
final class OnHeapHash {
  private int[] fields;
  private int mask;
  private int numElements;

  private OnHeapHash(final int[] fields, final int numElements) { //called by fromBytes()
    this.fields = fields;
    this.numElements = numElements;
    mask = fields.length - 1;
  }

  OnHeapHash(final int startSize) {
    resetFields(startSize);
  }

  static OnHeapHash fromBytes(final byte[] bytes, final int offset, final int endOffset) {
    final int[] fields = new int[(endOffset - offset) / 4];
    int numElements = 0;

    final Memory mem = new NativeMemory(bytes);
    for (int i = 0; i < fields.length; ++i) {
      fields[i] = mem.getInt(offset + (i << 2));
      if (fields[i] != -1) {
        ++numElements;
      }
    }

    return new OnHeapHash(fields, numElements);
  }

  void resetFields(final int size) {
    fields = new int[size];
    Arrays.fill(fields, -1); //
    mask = fields.length - 1;
    numElements = 0;
  }

  int[] getFields() {
    return fields;
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
    int field = fields[probe];
    while ((field != HashUtils.NOT_A_PAIR) && (key != HashUtils.keyOfPair(field))) {
      probe = (probe + 1) & mask;
      field = fields[probe];
    }

    if (field == HashUtils.NOT_A_PAIR) {
      fields[probe] = newField;
      callback.bucketUpdated(key, (byte) 0, val);
      ++numElements;
    }

    final byte oldVal = HashUtils.valOfPair(field);
    if (oldVal < val) {
      fields[probe] = newField;
      callback.bucketUpdated(key, oldVal, val);
      ++numElements;
    }

    return numElements;
  }

  int intoByteArray(final byte[] array, int offset) {
    final int numBytesNeeded = numBytesToSerialize();
    if ((array.length - offset) < numBytesNeeded) {
      throw new SketchesArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    final Memory mem = new NativeMemory(array);

    for (int field : fields) {
      mem.putInt(offset, field);
      offset += 4;
    }

    return offset;
  }

  int numBytesToSerialize() {
    return (fields.length << 2);
  }

  BucketIterator getBucketIterator() {
    return new BucketIterator() {
      private int i = -1;

      @Override
      public boolean next() {
        ++i;
        while ((i < fields.length) && (fields[i] == HashUtils.NOT_A_PAIR)) {
          ++i;
        }
        return i < fields.length;
      }

      @Override
      public boolean nextAll() {
        return ++i < fields.length;
      }

      @Override
      public int getKey() {
        if (fields[i] == -1) { return -1; }
        return HashUtils.keyOfPair(fields[i]);
      }

      @Override
      public byte getValue() {
        return HashUtils.valOfPair(fields[i]);
      }
    };
  }

  void boostrap(final int[] myFields) {
    for (int field : myFields) {
      if (field != HashUtils.NOT_A_PAIR) {
        updateBucket(HashUtils.keyOfPair(field), HashUtils.valOfPair(field), field, Fields.NOOP_CB);
      }
    }
  }
}
