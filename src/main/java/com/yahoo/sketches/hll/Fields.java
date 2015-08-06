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
  public static byte NAIVE_DENSE_VERSION = 0x0;
  public static byte HASH_SPARSE_VERSION = 0x1;
  public static byte SORTED_SPARSE_VERSION = 0x2;

  Preamble getPreamble();

  Fields updateBucket(int i, byte val);

  /**
   * Fills the array starting from offset with the byte array representation of the fields
   *
   * This should *not* include the preamble
   *
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
}
