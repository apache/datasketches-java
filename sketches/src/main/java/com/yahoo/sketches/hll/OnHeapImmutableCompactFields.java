/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 */
final class OnHeapImmutableCompactFields implements Fields {
  private final Preamble preamble;
  private final int[] fields;
  private static final byte VERSION_ID = Fields.Version.SORTED_SPARSE_VERSION.getId();

  OnHeapImmutableCompactFields(final Preamble preamble, final int[] fields) {
    this.preamble = preamble;
    this.fields = fields;
  }

  public static OnHeapImmutableCompactFields fromBytes(
      final Preamble preamble,
      final byte[] bytes,
      final int startOffset,
      final int endOffset) {
    if (bytes[startOffset] != VERSION_ID) {
      throw new IllegalArgumentException(
        String.format(
          "Can only deserialize the sorted, sparse representation[%d] got [%d]",
          VERSION_ID,
          bytes[startOffset]
        )
      );
    }

    final WritableMemory mem = WritableMemory.wrap(bytes);
    final int[] fields = new int[(endOffset - startOffset) / 4];
    final int dataOffset = startOffset + 1;

    for (int i = 0; i < fields.length; ++i) {
      fields[i] = mem.getInt(dataOffset + (i << 2));
    }
    return new OnHeapImmutableCompactFields(preamble, fields);
  }

  public static OnHeapImmutableCompactFields fromFields(final Fields fields) {
    final List<Integer> vals = new ArrayList<>();

    final BucketIterator iter = fields.getBucketIterator();
    while (iter.next()) {
      vals.add(HashUtils.pairOfKeyAndVal(iter.getKey(), iter.getValue()));
    }
    Collections.sort(
      vals,
      new Comparator<Integer>() {
        @Override
        public int compare(final Integer o1, final Integer o2) {
          return HashUtils.valOfPair(o2) - HashUtils.valOfPair(o1);
        }
      }
    );

    final int[] theFields = new int[vals.size()];
    int count = 0;
    for (Integer val : vals) {
      theFields[count++] = val;
    }
    return new OnHeapImmutableCompactFields(fields.getPreamble(), theFields);
  }

  @Override
  public Version getFieldsVersion() {
    return Fields.Version.SORTED_SPARSE_VERSION;
  }

  @Override
  public Preamble getPreamble() {
    return preamble;
  }

  @Override
  public Fields updateBucket(final int i, final byte val, final UpdateCallback cb) {
    throw new UnsupportedOperationException("Cannot mutate a compact sketch");
  }

  @Override
  public int intoByteArray(final byte[] array, int offset) {
    final int numBytesNeeded = numBytesToSerialize();
    if ((array.length - offset) < numBytesNeeded) {
      throw new SketchesArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    final WritableMemory mem = WritableMemory.wrap(array);
    mem.putByte(offset++, VERSION_ID);

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
        return HashUtils.keyOfPair(fields[i]);
      }

      @Override
      public byte getValue() {
        return HashUtils.valOfPair(fields[i]);
      }
    };
  }

  @Override
  public Fields unionInto(final Fields recipient, final UpdateCallback cb) {
    return recipient.unionBucketIterator(getBucketIterator(), cb);
  }

  @Override
  public Fields unionBucketIterator(final BucketIterator iter, final UpdateCallback cb) {
    throw new UnsupportedOperationException("Cannot mutate a compact sketch");
  }

  @Override
  public Fields unionCompressedAndExceptions(final byte[] compressed, final int minVal,
      final OnHeapHash exceptions, final UpdateCallback cb) {
    throw new UnsupportedOperationException("Cannot mutate a compact sketch");
  }

}
