/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;


final class OnHeapHashFields implements Fields {
  private final Preamble preamble;
  private final FieldsFactory denseFactory;
  private final int switchToDenseSize;
  private final OnHeapHash hasher;
  private int growthBound;
  private static final byte VERSION_ID = Fields.Version.HASH_SPARSE_VERSION.getId();

  private OnHeapHashFields( //for deserialization
      final Preamble preamble,
      final FieldsFactory denseFactory,
      final int switchToDenseSize,
      final OnHeapHash hasher,
      final int growthBound
  ) {
    this.preamble = preamble;
    this.denseFactory = denseFactory;
    this.switchToDenseSize = switchToDenseSize;
    this.hasher = hasher;
    this.growthBound = growthBound;
  }

  public OnHeapHashFields(
      final Preamble preamble,
      final int startSize,
      final int switchToDenseSize,
      final FieldsFactory denseFactory) {
    this.preamble = preamble;
    this.denseFactory = denseFactory;
    hasher = new OnHeapHash(startSize);
    this.switchToDenseSize = switchToDenseSize;

    growthBound = 3 * (startSize >>> 2); //.75 fill ratio * size
  }

  public static OnHeapHashFields fromBytes(
      final Preamble preamble,
      final byte[] bytes,
      final int offset,
      final int numBytes) {
    if (bytes[offset] != VERSION_ID) {
      throw new IllegalArgumentException(
        String.format(
          "Can only deserialize the hash table sparse representation[%d] got [%d]",
          VERSION_ID,
          bytes[offset]
        )
      );
    }

    int bytesRead = 1;
    final NativeMemory mem = new NativeMemory(bytes);

    final int switchToDenseSize = mem.getInt(offset + bytesRead);
    bytesRead += 4;

    final int growthBound = mem.getInt(offset + bytesRead);
    bytesRead += 4;

    final int numBytesForFieldsFactory = mem.getInt(offset + bytesRead);
    bytesRead += 4;

    final FieldsFactory denseFactory = FieldsFactories.fromBytes(
        bytes, offset + bytesRead, offset + bytesRead + numBytesForFieldsFactory
    );
    bytesRead += numBytesForFieldsFactory;

    final OnHeapHash hasher = OnHeapHash.fromBytes(bytes, offset + bytesRead, numBytes);

    return new OnHeapHashFields(preamble, denseFactory, switchToDenseSize, hasher, growthBound);
  }

  @Override
  public Version getFieldsVersion() {
    return Fields.Version.HASH_SPARSE_VERSION;
  }

  @Override
  public Preamble getPreamble() {
    return preamble;
  }

  @Override
  public Fields updateBucket(final int key, final byte val, final UpdateCallback callback) {
    hasher.updateBucket(key, val, callback);

    if (hasher.getNumElements() >= growthBound) {
      final int[] fields = hasher.getFields();
      growthBound = 3 * (fields.length >>> 2);
      if (fields.length == switchToDenseSize) {
        final Fields retVal = denseFactory.make(preamble);
        final BucketIterator iter = getBucketIterator();
        while (iter.next()) {
          retVal.updateBucket(iter.getKey(), iter.getValue(), NOOP_CB);
        }
        return retVal;
      } else {
        hasher.resetFields(fields.length << 1);
        hasher.boostrap(fields);
      }
    }

    return this;
  }

  @Override
  public int intoByteArray(final byte[] array, int offset) {
    final int numBytesNeeded = numBytesToSerialize();
    if ((array.length - offset) < numBytesNeeded) {
      throw new SketchesArgumentException(
          String.format("array too small[%,d] < [%,d]", array.length - offset, numBytesNeeded)
      );
    }

    array[offset++] = VERSION_ID;

    final NativeMemory mem = new NativeMemory(array);
    offset = Serde.putInt(mem, offset, switchToDenseSize);
    offset = Serde.putInt(mem, offset, growthBound);
    offset = Serde.putInt(mem, offset, denseFactory.numBytesToSerialize());
    offset = denseFactory.intoByteArray(array, offset);

    return hasher.intoByteArray(array, offset);
  }

  @Override
  public int numBytesToSerialize() {
    return 1 // type
         + 4 // switchToDenseSize
         + 4 // denseFactoryNumBytes
         + denseFactory.numBytesToSerialize()
         + 4 // growthBound
         + hasher.numBytesToSerialize();
  }

  @Override
  public Fields toCompact() {
    return OnHeapImmutableCompactFields.fromFields(this);
  }

  @Override
  public BucketIterator getBucketIterator() {
    return hasher.getBucketIterator();
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
  public Fields unionCompressedAndExceptions(final byte[] compressed, final int minVal,
      final OnHeapHash exceptions, final UpdateCallback cb) {
    return unionBucketIterator(
        CompressedBucketUtils.getBucketIterator(compressed, minVal, exceptions), cb);
  }

}
