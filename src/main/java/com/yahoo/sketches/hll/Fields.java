package com.yahoo.sketches.hll;

/**
 * An interface that abstracts out the underlying storage of an HLL from the hashing
 * and other activities required to maintain an HLL.
 *
 * This interface is experimental, but the API is not solidified and we reserve the
 * right to make backwards incompatible changes without pushing up the library's version.
 *
 * Implement at your own risk.
 */
public interface Fields
{
  byte NAIVE_DENSE_VERSION = 0x0;
  byte HASH_SPARSE_VERSION = 0x1;
  byte SORTED_SPARSE_VERSION = 0x2;
  byte COMPRESSED_DENSE_VERSION = 0x3;

  Preamble getPreamble();

  /**
   * Potentially updates a bucket in the underlying storage.  The Fields implementation
   * is expected to maintain the MAX val for each bucket.  If the val passed in is less
   * than the currently stored val, this method should do nothing.
   *
   * A callback *must* be provided which will be called whenever the provided val is
   * greater than the currently stored value.
   *
   * @param bucket the bucket to update
   * @param val the val to update to
   * @param callback the callback to be called if the provided val is greater than the current
   * @return the Fields object that should be used from this point forward
   */
  Fields updateBucket(int bucket, byte val, UpdateCallback callback);

  /**
   * Fills the array starting from offset with the byte array representation of the fields
   *
   * This should *not* include the preamble
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
   * Return the compact form of this fields object.  The compact form is intended as the most compact representation
   * of the same data.  Generally, the compact form is not updatable and it is acceptable for it to throw
   * UnsupportedOperationException's out of the update calls.
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
   * Unions the current Fields into the Fields presented as an argument.  This exists to allow for polymorphic dispatch
   * to enable optimized unioning when available.  That is, most implementations will end up delegating to
   *
   * recipient.unionBucketIterator(getBucketIterator());
   *
   * But, if there is a method that is more specific to the implementation, it can choose to delegate to, e.g.
   *
   * unionCompressedAndExceptions()
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
   * @param minVal the minimum value (or "offset").  I.e. a 0 in a nibble from compressed represent this actual value
   * @param exceptions the exceptions hash
   * @param cb The callback to be called whenever a bucket value is updated
   * @return The new fields object to use to represent the unioned buckets
   */
  Fields unionCompressedAndExceptions(byte[] compressed, int minVal, OnHeapHash exceptions, UpdateCallback cb);

  /**
   * An UpdateCallback is a callback provided to calls that potentially update buckets.  It is a single method
   * interface that can provide feedback to the caller about when a bucket was updated.  This enables the HipHllSketch
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

  UpdateCallback NOOP_CB = new UpdateCallback()
  {
    @Override
    public void bucketUpdated(int bucket, byte oldVal, byte newVal)
    {

    }
  };
}
