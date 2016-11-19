/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Kevin Lang
 */
final class OnHeapFields implements Fields {
  private final Preamble preamble;
  private final byte[] buckets;

  public OnHeapFields(final Preamble preamble) {
    this.preamble = preamble;
    buckets = new byte[preamble.getConfigK()];
  }

  @Override
  public Preamble getPreamble() {
    return preamble;
  }

  @Override
  public Fields updateBucket(final int index, final byte val, final UpdateCallback callback) {
    if (val > buckets[index]) {
      callback.bucketUpdated(index, buckets[index], val);
      buckets[index] = val;
    }
    return this;
  }

  @Override
  public int intoByteArray(final byte[] array, int offset) {
    final int numBytesNeeded = numBytesToSerialize();
    if (array.length - offset < numBytesNeeded) {
      throw new SketchesArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    array[offset++] = Fields.NAIVE_DENSE_VERSION;
    for (byte bucket : buckets) {
      array[offset++] = bucket;
    }
    return offset;
  }

  @Override
  public int numBytesToSerialize() {
    return 1 + buckets.length;
  }

  @Override
  public Fields toCompact() {
    return this;
  }

  @Override
  public BucketIterator getBucketIterator() {
    return new BucketIterator() {
      private int i = -1;

      @Override
      public boolean next() {
        ++i;
        while (i < buckets.length && buckets[i] == 0) {
          ++i;
        }
        return i < buckets.length;
      }

      @Override
      public int getKey() {
        return i;
      }

      @Override
      public byte getValue() {
        return buckets[i];
      }
    };
  }

  @Override
  public Fields unionInto(final Fields recipient, final UpdateCallback cb) {
    return recipient.unionBucketIterator(getBucketIterator(), cb);
  }

  @Override
  public Fields unionBucketIterator(final BucketIterator iter, final UpdateCallback callback) {
    return HllUtils.unionBucketIterator(this, iter, callback);
  }

  @Override
  public Fields unionCompressedAndExceptions(
      final byte[] compressed, final int minVal, final OnHeapHash exceptions, final UpdateCallback cb) {
    return unionBucketIterator(CompressedBucketUtils.getBucketIterator(compressed, minVal, exceptions), cb);
  }
}
