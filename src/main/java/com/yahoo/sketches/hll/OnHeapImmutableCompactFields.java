/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author Kevin Lang
 */
class OnHeapImmutableCompactFields implements Fields {
  public static OnHeapImmutableCompactFields fromFields(Fields fields) {
    List<Integer> vals = new ArrayList<>();

    BucketIterator iter = fields.getBucketIterator();
    while (iter.next()) {
      vals.add(HashUtils.pairOfKeyAndVal(iter.getKey(), iter.getValue()));
    }
    Collections.sort(
        vals,
        new Comparator<Integer>() {
          @Override
          public int compare(Integer o1, Integer o2) {
            return HashUtils.valOfPair(o2) - HashUtils.valOfPair(o1);
          }
        }
    );

    int[] theFields = new int[vals.size()];
    int count = 0;
    for (Integer val : vals) {
      theFields[count++] = val;
    }

    return new OnHeapImmutableCompactFields(fields.getPreamble(), theFields);
  }

  private final Preamble preamble;
  private final int[] fields;

  OnHeapImmutableCompactFields(Preamble preamble, int[] fields) {
    this.preamble = preamble;
    this.fields = fields;
  }

  @Override
  public Preamble getPreamble() {
    return preamble;
  }

  @Override
  public Fields updateBucket(int i, byte val, UpdateCallback cb) {
    throw new UnsupportedOperationException("Cannot mutate a compact sketch");
  }

  @Override
  public int intoByteArray(byte[] array, int offset) {
    int numBytesNeeded = numBytesToSerialize();
    if (array.length - offset < numBytesNeeded) {
      throw new SketchesArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    Memory mem = new NativeMemory(array);
    mem.putByte(offset++, Fields.SORTED_SPARSE_VERSION);

    for (int field : fields) {
      mem.putInt(offset, field);
      offset += 4;
    }
    return offset;
  }

  @Override
  public int numBytesToSerialize() {
    return 1 + (fields.length << 2);
  }

  @Override
  public Fields toCompact() {
    return this;
  }

  @Override
  public BucketIterator getBucketIterator() {
    return new BucketIterator() {
      int i = -1;

      @Override
      public boolean next() {
        return ++i < fields.length;
      }

      @Override
      public int getKey() {
        return HashUtils.keyOfPair(fields[i]);
      }

      @Override
      public byte getValue() {
        return HashUtils.valOfPair(fields[i]);
      }
    };
  }

  @Override
  public Fields unionInto(Fields recipient, UpdateCallback cb) {
    return recipient.unionBucketIterator(getBucketIterator(), cb);
  }

  @Override
  public Fields unionBucketIterator(BucketIterator iter, UpdateCallback cb) {
    throw new UnsupportedOperationException("Cannot mutate a compact sketch");
  }

  @Override
  public Fields unionCompressedAndExceptions(
      byte[] compressed, int minVal, OnHeapHash exceptions, UpdateCallback cb) {
    throw new UnsupportedOperationException("Cannot mutate a compact sketch");
  }
}
