/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Kevin Lang
 */
final class OnHeapCompressedFields implements Fields {
  private static final int LO_NIBBLE_MASK = 0x0f;
  private static final int HI_NIBBLE_MASK = 0xf0;

  private final Preamble preamble;
  private final byte[] buckets;

  private volatile OnHeapHash exceptions_;
  private volatile byte currMin = 0;
  private volatile byte currMax = 14;
  private volatile int exceptionGrowthBound;
  private int numAtCurrMin;

  public OnHeapCompressedFields(final Preamble preamble) {
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
  public Fields updateBucket(final int index, final byte val, final UpdateCallback callback) {
    if (val <= currMin) { return this; }
    if (val > currMax) {
      final byte theOldVal = CompressedBucketUtils.getNibble(buckets, index);
      CompressedBucketUtils.setNibble(buckets, index, (byte) 0xf);
      exceptions_.updateBucket(
          index, val, new UpdateCallback() {
            @Override
            public void bucketUpdated(final int bucket, final byte oldVal, final byte newVal) {
              callback.bucketUpdated(
                  bucket, theOldVal == 0xf ? oldVal : (byte) (theOldVal + currMin), newVal);
            }
          }
      );

      adjustNumAtCurrMin(theOldVal);

      if (exceptions_.getNumElements() >= exceptionGrowthBound) {
        final int[] fields = exceptions_.getFields();
        this.exceptionGrowthBound = 3 * (fields.length >>> 2);
        exceptions_.resetFields(fields.length << 1);
        exceptions_.boostrap(fields);
      }
    } else {
      CompressedBucketUtils.updateNibble(
          buckets, index, (byte) (val - currMin), new UpdateCallback() {

            @Override
            public void bucketUpdated(final int bucket, byte oldVal, final byte newVal) {
              oldVal = (byte) (oldVal + currMin);
              final byte actualNewVal = (byte) (newVal + currMin);
              adjustNumAtCurrMin(oldVal);
              callback.bucketUpdated(bucket, oldVal, actualNewVal);
            }
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
          ++currMax;

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

        final OnHeapHash oldExceptions = exceptions_;
        exceptions_ = new OnHeapHash(oldExceptions.getFields().length);
        final BucketIterator bucketIter = oldExceptions.getBucketIterator();
        while (bucketIter.next()) {
          updateBucket(bucketIter.getKey(), bucketIter.getValue(), NOOP_CB);
        }
      }
    }
  }

  @Override
  public int intoByteArray(final byte[] array, int offset) {
    if (array.length - offset < 6) {
      throw new SketchesArgumentException(
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
  public Fields unionInto(final Fields recipient, final UpdateCallback cb) {
    return recipient.unionCompressedAndExceptions(buckets, currMin, exceptions_, cb);
  }

  @Override
  public Fields unionBucketIterator(final BucketIterator iter, final UpdateCallback callback) {
    return HllUtils.unionBucketIterator(this, iter, callback);
  }

  @Override
  public Fields unionCompressedAndExceptions(
      final byte[] compressed, final int minVal, final OnHeapHash exceptions, final UpdateCallback cb) {
    return unionBucketIterator(
        CompressedBucketUtils.getBucketIterator(compressed, minVal, exceptions), cb);
  }
}
