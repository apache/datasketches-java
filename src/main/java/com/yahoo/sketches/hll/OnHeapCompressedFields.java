/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Kevin Lang
 */
class OnHeapCompressedFields implements Fields {
  private static final int LO_NIBBLE_MASK = 0x0f;
  private static final int HI_NIBBLE_MASK = 0xf0;

  private final Preamble preamble;
  private final byte[] buckets;

  private volatile OnHeapHash exceptions_;
  private volatile byte currMin = 0;
  private volatile byte currMax = 14;
  private volatile int exceptionGrowthBound;
  private volatile int numAtCurrMin;

  public OnHeapCompressedFields(Preamble preamble) {
    this.preamble = preamble;
    buckets = new byte[preamble.getConfigK() >>> 1];
    exceptions_ = new OnHeapHash(16);

    this.exceptionGrowthBound = 3 * (exceptions_.getFields().length >>> 2);
    this.numAtCurrMin = preamble.getConfigK();
  }

  @Override
  public Preamble getPreamble() {
    return preamble;
  }

  @Override
  public Fields updateBucket(int index, byte val, final UpdateCallback callback) {
    if (val > currMax) {
      final byte theOldVal = CompressedBucketUtils.getNibble(buckets, index);
      CompressedBucketUtils.setNibble(buckets, index, (byte) 0xf);
      exceptions_.updateBucket(
          index, val, new UpdateCallback() {
            @Override
            public void bucketUpdated(int bucket, byte oldVal, byte newVal) {
              callback.bucketUpdated(bucket, theOldVal == 0xf ? oldVal : (byte) (theOldVal + currMin), newVal);
            }
          }
      );

      adjustNumAtCurrMin(theOldVal);

      if (exceptions_.getNumElements() >= exceptionGrowthBound) {
        int[] fields = exceptions_.getFields();
        this.exceptionGrowthBound = 3 * (fields.length >>> 2);
        exceptions_.resetFields(fields.length << 1);
        exceptions_.boostrap(fields);
      }
    } else {
      CompressedBucketUtils.updateNibble(
          buckets, index, (byte) (val - currMin), new UpdateCallback() {
            @Override
            public void bucketUpdated(int bucket, byte oldVal, byte newVal) {
              oldVal = (byte) (oldVal + currMin);
              byte actualNewVal = (byte) (newVal + currMin);
              adjustNumAtCurrMin(oldVal);
              callback.bucketUpdated(bucket, oldVal, actualNewVal);
            }
          }
      );
    }
    return this;
  }

  private void adjustNumAtCurrMin(byte oldVal) {
    if (oldVal == 0) {
      --numAtCurrMin;

      if (numAtCurrMin == 0) {
        while (numAtCurrMin == 0) {
          ++currMin;
          ++currMax;

          for (int i = 0; i < buckets.length; ++i) {
            byte bucket = buckets[i];

            int newLowNib = (bucket & LO_NIBBLE_MASK) - 1;
            int newHighNib = (bucket & HI_NIBBLE_MASK) - 0x10;

            if (newLowNib == 0) {
              ++numAtCurrMin;
            }
            if (newHighNib == 0) {
              ++numAtCurrMin;
            }
            buckets[i] = (byte) (newHighNib | newLowNib);
          }
        }

        OnHeapHash oldExceptions = exceptions_;
        exceptions_ = new OnHeapHash(oldExceptions.getFields().length);
        BucketIterator bucketIter = oldExceptions.getBucketIterator();
        while (bucketIter.next()) {
          updateBucket(bucketIter.getKey(), bucketIter.getValue(), NOOP_CB);
        }
      }
    }
  }

  @Override
  public int intoByteArray(byte[] array, int offset) {
    if (array.length - offset < 6) {
      throw new IllegalArgumentException(
          String.format("array too small[%,d][%,d], need at least 6 bytes", array.length, offset)
      );
    }

    array[offset++] = Fields.COMPRESSED_DENSE_VERSION;
    array[offset++] = currMin;
    new NativeMemory(array).putInt(offset, numAtCurrMin);
    offset += 4;
    for (byte bucket : buckets) {
      array[offset++] = bucket;
    }
    return exceptions_.intoByteArray(array, offset);
  }

  @Override
  public int numBytesToSerialize() {
    return 1 + 5 + buckets.length + exceptions_.numBytesToSerialize();
  }

  @Override
  public Fields toCompact() {
    return this;
  }

  @Override
  public BucketIterator getBucketIterator() {
    return CompressedBucketUtils.getBucketIterator(buckets, currMin, exceptions_);
  }

  @Override
  public Fields unionInto(Fields recipient, UpdateCallback cb) {
    return recipient.unionCompressedAndExceptions(buckets, currMin, exceptions_, cb);
  }

  @Override
  public Fields unionBucketIterator(BucketIterator iter, UpdateCallback callback) {
    return HllUtils.unionBucketIterator(this, iter, callback);
  }

  @Override
  public Fields unionCompressedAndExceptions(
      byte[] compressed, int minVal, OnHeapHash exceptions, UpdateCallback cb) {
    return unionBucketIterator(
        CompressedBucketUtils.getBucketIterator(compressed, minVal, exceptions), cb);
  }
}
