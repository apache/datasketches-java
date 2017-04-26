/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * An interface that abstracts out the underlying storage of an HLL from the hashing
 * and other activities required to maintain an HLL.
 *
 * <p>This interface is experimental, but the API is not solidified and we reserve the
 * right to make backwards incompatible changes without pushing up the library's version.</p>
 *
 * <p>Implement at your own risk.</p>
 */
public interface Fields {
  String LS = System.getProperty("line.separator");

  /**
   * The first byte after the preamble (if it exists) and before the fields' data starts
   * @author Lee Rhodes
   */
  enum Version {
    /**
     * Standard (8-bits per bucket) byte array. Used by OnHeapFields.
     */
    NAIVE_DENSE_VERSION((byte) 0),
    /**
     * An int hash-table of coupons, used during warmup. Used by OnHeapHashFields
     */
    HASH_SPARSE_VERSION((byte) 1),
    /**
     * A immutable, sorted int array of coupons, used for serialized storage.
     * Used by OnHeapImmutableCompactFields.
     */
    SORTED_SPARSE_VERSION((byte) 2),
    /**
     * A 4-bit nibble array (+ Aux exceptions array). Used by OnHeapCompressedFields
     */
    COMPRESSED_DENSE_VERSION((byte) 3);

    private final byte id_;

    private Version(final byte id) {
      id_ = id;
    }

    public byte getId() {
      return id_;
    }

    public static Version idToVersion(final int id) {
      switch (id) {
        case 0 : return NAIVE_DENSE_VERSION;
        case 1 : return HASH_SPARSE_VERSION;
        case 2 : return SORTED_SPARSE_VERSION;
        case 3 : return COMPRESSED_DENSE_VERSION;
        default:
          throw new SketchesArgumentException("Possible Corruption: Illegal Version ID: " + id);
      }
    }
  }

  /**
   * Gets the Fields Version
   * @return the Fields Version
   */
  Version getFieldsVersion();

  /**
   * Gets  the Preamble
   * @return the Preamble
   */
  Preamble getPreamble();

  /**
   * Potentially updates a bucket in the underlying storage.  The Fields implementation
   * is expected to maintain the MAX value for each bucket.  If the value passed in is less
   * than or equal to the currently stored value, this method should do nothing.
   *
   * <p>A callback *must* be provided which will be called whenever the provided value is
   * greater than the currently stored value.
   *
   * @param bucket the bucket to update
   * @param val the value to update to
   * @param callback the callback to be called if the provided value is greater than the current
   * @return the Fields object that should be used from this point forward
   */
  Fields updateBucket(int bucket, byte val, UpdateCallback callback);

  /**
   * Fills the array starting from offset with the byte array representation of the fields
   *
   * <p>This should *not* include the preamble
   * @param array given array to fill
   * @param offset starting with this offset
   * @return the last offset written +1
   */
  int intoByteArray(byte[] array, int offset);

  /**
   * Provides an indication of how many bytes would be required to serialize the fields to
   * a byte[].
   *
   * @return the number of bytes to serialize the fields to a byte[]
   */
  int numBytesToSerialize();

  /**
   * Return the compact form of this fields object.  The compact form is intended as the most
   * compact representation of the same data.  Generally, the compact form is not updatable and
   * it is acceptable for it to throw UnsupportedOperationExceptions out of the update calls.
   *
   * @return  The compact representation of these fields.
   */
  Fields toCompact();

  /**
   * Returns a BucketIterator over the buckets in this Fields object.
   *
   * @return a Bucket iterator that walks the buckets of this Fields object.
   */
  BucketIterator getBucketIterator();

  /**
   * Unions the current Fields into the Fields presented as an argument. This exists to allow for
   * polymorphic dispatch to enable optimized unioning when available. That is, most
   * implementations will end up delegating to:
   *
   * <code>recipient.unionBucketIterator(getBucketIterator());</code>
   *
   * <p>But, if there is a method that is more specific to the implementation, it can choose to
   * delegate to, e.g:
   *
   * <code>unionCompressedAndExceptions()</code>
   *
   * @param recipient The fields to be unioned *into*
   * @param cb The callback to be called whenever a bucket value is updated
   * @return The new fields object to use to represent the unioned buckets
   */
  Fields unionInto(Fields recipient, UpdateCallback cb);

  /**
   * Unions the provided BucketIterator into the current Fields object.
   *
   * @param iter the BucketIterator to union into the current Fields object
   * @param cb The callback to be called whenever a bucket value is updated
   * @return The new fields object to use to represent the unioned buckets
   */
  Fields unionBucketIterator(BucketIterator iter, UpdateCallback cb);

  /**
   * Unions the provided compressed byte[] and exceptions hash into the current Fields object.
   *
   * @param compressed a byte array of compressed, 4-bit values as used by OnHeapCompressedFields
   * @param minVal the minimum value (or "offset").  I.e. a 0 in a nibble from compressed represent
   * this actual value
   * @param exceptions the exceptions hash
   * @param cb The callback to be called whenever a bucket value is updated
   * @return The new fields object to use to represent the unioned buckets
   */
  Fields unionCompressedAndExceptions(byte[] compressed, int minVal, OnHeapHash exceptions,
      UpdateCallback cb);

  /**
   * Returns a readable summary, optionally with detail, of this sketch.
   * @param detail if true, list all the data detail
   * @return  a readable summary, optionally with detail, of this sketch.
   */
  default String toString(final boolean detail) {
    final String thisSimpleName = getClass().getSimpleName();
    final StringBuilder sb = new StringBuilder();
    final Preamble preamble = getPreamble();
    final Version version = getFieldsVersion();
    final String verStr = version.toString() + ", " +  version.getId();
    int preBytes = 0;
    if (preamble != null) {
      sb.append(preamble.toString());
      preBytes = 8;
    }
    final int totBytes = preBytes + numBytesToSerialize();
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   Version, ID             : ").append(verStr).append(LS);
    sb.append("   Total Bytes             : ").append(totBytes).append(LS);
    sb.append(LS);
    if (!detail) {
      return sb.toString();
    }
    sb.append("#### HLL DATA DETAIL:").append(LS);
    final BucketIterator iter = getBucketIterator();
    int idx = 0;
    sb.append("  Index  Value").append(LS);
    while (iter.nextAll()) {
      final int key = iter.getKey();
      final int retVal = iter.getValue() & 0XFF; //make positive
      final String idxStr = String.format("%9s:%4s", key, retVal);
      if ((idx != 0) && ((idx % 8) == 0)) { sb.append(LS + idxStr); }
      else { sb.append(idxStr); }
      idx++;
    }
    sb.append(LS);
    return sb.toString();
  }

  /**
   * An UpdateCallback is a callback provided to calls that potentially update buckets.  It is a
   * single method interface that can provide feedback to the caller about when a bucket was updated.
   * This enables the HipHllSketch
   * to get the information it needs to use the HipEstimation algorithm instead of the built-in one.
   */
  interface UpdateCallback {

    /**
     * Called when a bucket value is updated.
     *
     * @param bucket the index of the bucket that was updated
     * @param oldVal the old value of the bucket
     * @param newVal the new value of the bucket
     */
    void bucketUpdated(int bucket, byte oldVal, byte newVal);
  }

  /**
   * Returns a new No-Op Callback
   */
  UpdateCallback NOOP_CB = new UpdateCallback() {

    @Override
    public void bucketUpdated(final int bucket, final byte oldVal, final byte newVal) {
      //intentionally empty
    }
  };
}
