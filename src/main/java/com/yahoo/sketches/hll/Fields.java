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

  Fields toCompact();

  BucketIterator getBucketIterator();

  interface UpdateCallback {
    void bucketUpdated(int bucket, byte oldVal, byte newVal);
  }
}
