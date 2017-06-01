/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 */
final class OnHeapCompressedFields implements Fields {
  private static final int LO_NIBBLE_MASK = 0x0f;
  private static final int HI_NIBBLE_MASK = 0xf0;
  private static final byte VERSION_ID = Fields.Version.COMPRESSED_DENSE_VERSION.getId();

  private final Preamble preamble;
  private final byte[] buckets;

  private OnHeapHash exceptions;
  private byte currMin = 0;
  private int exceptionGrowthBound;
  private int numAtCurrMin;

  public OnHeapCompressedFields(final Preamble preamble) {
    this.preamble = preamble;
    buckets = new byte[preamble.getConfigK() >>> 1];
    exceptions = new OnHeapHash(16);
    exceptionGrowthBound = 3 * (exceptions.getFields().length >>> 2);
    numAtCurrMin = preamble.getConfigK();
  }

  private OnHeapCompressedFields(
      final Preamble preamble,
      final byte[] buckets,
      final OnHeapHash exceptions,
      final byte currMin,
      final int numAtCurrMin) {
    this.preamble = preamble;
    this.buckets = buckets;
    this.exceptions = exceptions;
    this.currMin = currMin;
    exceptionGrowthBound = 3 * (exceptions.getFields().length >>> 2);
    this.numAtCurrMin = numAtCurrMin;
  }

  public static OnHeapCompressedFields fromBytes(
      final Preamble preamble,
      final byte[] bytes,
      int offset,
      final int endOffset) {
    if (bytes[offset] != VERSION_ID) {
      throw new IllegalArgumentException(
        String.format(
          "Can only deserialize the compressed dense representation[%d] got [%d]",
          VERSION_ID,
          bytes[offset]
        )
      );
    }

    final byte currMin = bytes[offset + 1];
    offset += 2;

    final NativeMemory mem = new NativeMemory(bytes);
    final int numAtCurrMin = mem.getInt(offset + 2);
    offset += 4;

    final byte[] buckets = new byte[mem.getInt(offset)];
    offset += 4;

    mem.getByteArray(offset, buckets, 0, buckets.length);
    offset += buckets.length;

    return new OnHeapCompressedFields(
      preamble,
      buckets,
      OnHeapHash.fromBytes(bytes, offset, endOffset),
      currMin,
      numAtCurrMin
    );
  }

  @Override
  public Version getFieldsVersion() {
    return Fields.Version.COMPRESSED_DENSE_VERSION;
  }

  @Override
  public Preamble getPreamble() {
    return preamble;
  }

  @Override
  public Fields updateBucket(final int index, final byte val, final UpdateCallback callback) {
    final int valDiff = val - currMin;
    if (valDiff <= 0) {
      return this;
    }
    if (valDiff > 14) {
      final byte theOldVal = CompressedBucketUtils.getNibble(buckets, index);
      CompressedBucketUtils.setNibble(buckets, index, (byte) 0xf);
      exceptions.updateBucket(
          index,
          val,
          (bucket, oldVal, newVal) -> callback.bucketUpdated(
              bucket, theOldVal == 0xf ? oldVal : (byte) (theOldVal + currMin), newVal)
      );

      adjustNumAtCurrMin(theOldVal);

      if (exceptions.getNumElements() >= exceptionGrowthBound) {
        final int[] fields = exceptions.getFields();
        exceptionGrowthBound = 3 * (fields.length >>> 2);
        exceptions.resetFields(fields.length << 1);
        exceptions.boostrap(fields);
      }
    } else {
      CompressedBucketUtils.updateNibble(
          buckets, index,
          (byte) (val - currMin),
          (bucket, oldVal, newVal) -> {
            oldVal = (byte) (oldVal + currMin);
            final byte actualNewVal = (byte) (newVal + currMin);
            adjustNumAtCurrMin(oldVal);
            callback.bucketUpdated(bucket, oldVal, actualNewVal);
          }
      );
    }
    return this;
  }

  private void adjustNumAtCurrMin(final byte oldVal) {
    if (oldVal == 0) {
      --numAtCurrMin;

      if (numAtCurrMin == 0) {
        while (numAtCurrMin == 0) {
          ++currMin;

          for (int i = 0; i < buckets.length; ++i) {
            final byte bucket = buckets[i];

            final int newLowNib = (bucket & LO_NIBBLE_MASK) - 1;
            final int newHighNib = (bucket & HI_NIBBLE_MASK) - 0x10;

            if (newLowNib == 0) {
              ++numAtCurrMin;
            }
            if (newHighNib == 0) {
              ++numAtCurrMin;
            }
            buckets[i] = (byte) (newHighNib | newLowNib);
          }
        }

        final OnHeapHash oldExceptions = exceptions;
        exceptions = new OnHeapHash(oldExceptions.getFields().length);
        final BucketIterator bucketIter = oldExceptions.getBucketIterator();
        while (bucketIter.next()) {
          updateBucket(bucketIter.getKey(), bucketIter.getValue(), NOOP_CB);
        }
      }
    }
  }

  @Override
  public int intoByteArray(final byte[] array, int offset) {
    if ((array.length - offset) < 6) {
      throw new SketchesArgumentException(
        String.format("array too small[%,d][%,d], need at least 6 bytes", array.length, offset)
      );
    }

    array[offset++] = VERSION_ID;
    array[offset++] = currMin;
    final NativeMemory mem = new NativeMemory(array);
    offset = Serde.putInt(mem, offset, numAtCurrMin);
    offset = Serde.putInt(mem, offset, buckets.length);
    for (byte bucket : buckets) {
      array[offset++] = bucket;
    }
    return exceptions.intoByteArray(array, offset);
  }

  @Override
  public int numBytesToSerialize() {
    return 1 // version
           + 1 // currMin
           + 4 // numAtCurrMin
           + 4 // buckets.length
           + buckets.length
           + exceptions.numBytesToSerialize();
  }

  @Override
  public Fields toCompact() {
    return this;
  }

  @Override
  public BucketIterator getBucketIterator() {
    return CompressedBucketUtils.getBucketIterator(buckets, currMin, exceptions);
  }

  @Override
  public Fields unionInto(final Fields recipient, final UpdateCallback cb) {
    return recipient.unionCompressedAndExceptions(buckets, currMin, exceptions, cb);
  }

  @Override
  public Fields unionBucketIterator(final BucketIterator iter, final UpdateCallback callback) {
    return HllUtils.unionBucketIterator(this, iter, callback);
  }

  @Override
  public Fields unionCompressedAndExceptions(
      final byte[] compressed, final int minVal, final OnHeapHash myExceptions,
      final UpdateCallback cb ) {
    return unionBucketIterator(
        CompressedBucketUtils.getBucketIterator(compressed, minVal, myExceptions), cb);
  }

}
