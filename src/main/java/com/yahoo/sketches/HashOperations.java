/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * Helper class for the common hash table methods.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class HashOperations {
  private static final int STRIDE_HASH_BITS = 7;

  private static final int EMPTY = 0;

  /**
   * The stride mask for the Open Address, Double Hashing (OADH) hash table algorithm.
   */
  public static final int STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

  private HashOperations() {}

  /**
   * Counts the cardinality of the first Log2 values of the given source array.
   * @param srcArr the given source array
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>
   * @param thetaLong <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @return the cardinality
   */
  public static int countPart(final long[] srcArr, final int lgArrLongs, final long thetaLong) {
    int cnt = 0;
    final int len = 1 << lgArrLongs;
    for (int i = len; i-- > 0;) {
      final long hash = srcArr[i];
      if (continueCondition(thetaLong, hash) ) {
        continue;
      }
      cnt++ ;
    }
    return cnt;
  }

  /**
   * Counts the cardinality of the given source array.
   * @param srcArr the given source array
   * @param thetaLong <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @return the cardinality
   */
  public static int count(final long[] srcArr, final long thetaLong) {
    int cnt = 0;
    final int len = srcArr.length;
    for (int i = len; i-- > 0;) {
      final long hash = srcArr[i];
      if (continueCondition(thetaLong, hash) ) {
        continue;
      }
      cnt++ ;
    }
    return cnt;
  }

  // make odd and independent of index assuming lgArrLongs lowest bits of the hash were used for
  //  index
  private static int getStride(final long hash, final int lgArrLongs) {
    return (2 * (int) ((hash >>> (lgArrLongs)) & STRIDE_MASK)) + 1;
  }

  //ON-HEAP

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash search scheme for on-heap.
   * Returns the index if found, -1 if not found.
   *
   * @param hashTable The hash table to search. Must be a power of 2 in size.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash A hash value to search for. Must not be zero.
   * @return Current probe index if found, -1 if not found.
   */
  public static int hashSearch(final long[] hashTable, final int lgArrLongs, final long hash) {
    if (hash == 0) {
      throw new SketchesArgumentException("Given hash cannot be zero: " + hash);
    }
    final int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    final int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);

    // search for duplicate or empty slot
    final int loopIndex = curProbe;
    do {
      final long arrVal = hashTable[curProbe];
      if (arrVal == EMPTY) {
        return -1; // not found
      } else if (arrVal == hash) {
        return curProbe; // found
      }
      curProbe = (curProbe + stride) & arrayMask;
    } while (curProbe != loopIndex);
    return -1;
  }

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme for on-heap.
   * This method assumes that the input hash is not a duplicate.
   * Useful for rebuilding tables to avoid unnecessary comparisons.
   * Returns the index of insertion, which is always positive or zero. Throws an exception if the
   * table is full with no empty slot.
   * Throws an exception if table has no empty slot.
   *
   * @param hashTable the hash table to insert into.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash value that must not be zero and will be inserted into the array into an empty slot.
   * @return index of insertion.  Always positive or zero.
   */
  public static int hashInsertOnly(final long[] hashTable, final int lgArrLongs, final long hash) {
    final int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    final int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);

    final long loopIndex = curProbe;
    do {
      final long arrVal = hashTable[curProbe];
      if (arrVal == EMPTY) {
        hashTable[curProbe] = hash;
        return curProbe;
      }
      curProbe = (curProbe + stride) & arrayMask;
    } while (curProbe != loopIndex);
    throw new SketchesArgumentException("No empty slot in table!");
  }

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme for on-heap.
   * Returns index &ge; 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
   * Throws an exception if the value is not found and table has no empty slot.
   *
   * @param hashTable the hash table to insert into.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash hash value that must not be zero and if not a duplicate will be inserted into the
   * array into an empty slot
   * @return index &ge; 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
   */
  public static int hashSearchOrInsert(final long[] hashTable, final int lgArrLongs,
      final long hash) {
    final int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    final int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);

    // search for duplicate or zero
    final int loopIndex = curProbe;
    do {
      final long arrVal = hashTable[curProbe];
      if (arrVal == EMPTY) {
        hashTable[curProbe] = hash; // insert value
        return ~curProbe;
      } else if (arrVal == hash) {
        return curProbe; // found a duplicate
      }
      curProbe = (curProbe + stride) & arrayMask;
    } while (curProbe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

  /**
   * Inserts the given long array into the given hash table array of the target size,
   * removes any negative input values, ignores duplicates and counts the values inserted.
   * The given hash table may have values, but they must have been inserted by this method or one
   * of the other OADH insert methods in this class and they may not be dirty.
   * This method performs additional checks against potentially invalid hash values or theta values.
   * Returns the count of values actually inserted.
   *
   * @param srcArr the source hash array to be potentially inserted
   * @param hashTable The correctly sized target hash table that must be a power of two.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param thetaLong must greater than zero
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @return the count of values actually inserted
   */
  public static int hashArrayInsert(final long[] srcArr, final long[] hashTable,
      final int lgArrLongs, final long thetaLong) {
    int count = 0;
    final int arrLen = srcArr.length;
    checkThetaCorruption(thetaLong);
    for (int i = 0; i < arrLen; i++ ) { // scan source array, build target array
      final long hash = srcArr[i];
      checkHashCorruption(hash);
      if (continueCondition(thetaLong, hash) ) {
        continue;
      }
      if (hashSearchOrInsert(hashTable, lgArrLongs, hash) < 0) {
        count++ ;
      }
    }
    return count;
  }

  //OFF-HEAP (these are kept for backward compatibility)

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash search scheme for off-heap.
   * Returns the index if found, -1 if not found.
   *
   * @param mem The Memory hash table to search.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash A hash value to search for. Must not be zero.
   * @param memOffsetBytes offset in the memory where the hash array starts
   * @return index if found, -1 if not found.
   */
  public static int hashSearch(final Memory mem, final int lgArrLongs, final long hash,
      final int memOffsetBytes) {
    final int arrayMask = (1 << lgArrLongs) - 1;
    final int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    final int loopIndex = curProbe;
    do {
      final int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
      final long curArrayHash = mem.getLong(curProbeOffsetBytes);
      if (curArrayHash == EMPTY) { return -1; }
      else if (curArrayHash == hash) { return curProbe; }
      curProbe = (curProbe + stride) & arrayMask;
    } while (curProbe != loopIndex);
    return -1;
  }

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme, but inserts
   * values directly into a Memory.
   * This method assumes that the input hash is not a duplicate.
   * Useful for rebuilding tables to avoid unnecessary comparisons.
   * Returns the index of insertion, which is always positive or zero.
   * Throws an exception if table has no empty slot.
   *
   * @param wmem The writable memory
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash value that must not be zero and will be inserted into the array into an empty slot.
   * @param memOffsetBytes offset in the memory where the hash array starts
   * @return index of insertion.  Always positive or zero.
   */
  public static int fastHashInsertOnly(final WritableMemory wmem, final int lgArrLongs,
      final long hash, final int memOffsetBytes) {
    final int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    final int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    // search for duplicate or zero
    final int loopIndex = curProbe;
    do {
      final int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
      final long curArrayHash = wmem.getLong(curProbeOffsetBytes);
      if (curArrayHash == EMPTY) {
        wmem.putLong(curProbeOffsetBytes, hash);
        return curProbe;
      }
      curProbe = (curProbe + stride) & arrayMask;
    } while (curProbe != loopIndex);
    throw new SketchesArgumentException("No empty slot in table!");
  }

  //FAST OFF-HEAP

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme, but inserts
   * values directly into a Memory.
   * Returns index &ge; 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
   * Throws an exception if the value is not found and table has no empty slot.
   *
   * @param wmem the WritableMemory
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash A hash value that must not be zero and if not a duplicate will be inserted into the
   * array into an empty slot.
   * @param memOffsetBytes offset in the memory where the hash array starts
   * @return index &ge; 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
   */
  public static int fastHashSearchOrInsert(final WritableMemory wmem, final int lgArrLongs,
      final long hash, final int memOffsetBytes) {
    final int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    final int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    // search for duplicate or zero
    final int loopIndex = curProbe;
    do {
      final int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
      final long curArrayHash = wmem.getLong(curProbeOffsetBytes);
      if (curArrayHash == EMPTY) {
        wmem.putLong(curProbeOffsetBytes, hash);
        return ~curProbe;
      } else if (curArrayHash == hash) { return curProbe; } // curArrayHash is a duplicate
      // curArrayHash is not a duplicate and not zero, continue searching
      curProbe = (curProbe + stride) & arrayMask;
    } while (curProbe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slot in table!");
  }

  /**
   * @param thetaLong must be greater than zero otherwise throws an exception.
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   */
  public static void checkThetaCorruption(final long thetaLong) {
    //if any one of the groups go negative it fails.
    if (( thetaLong | (thetaLong - 1) ) < 0L ) {
      throw new SketchesStateException(
          "Data Corruption: thetaLong was negative or zero: " + "ThetaLong: " + thetaLong);
    }
  }

  /**
   * @param hash must be greater than -1 otherwise throws an exception.
   * Note a hash of zero is normally ignored, but a negative hash is never allowed.
   */
  public static void checkHashCorruption(final long hash) {
    if ( hash < 0L ) {
      throw new SketchesArgumentException(
          "Data Corruption: hash was negative: " + "Hash: " + hash);
    }
  }

  /**
   * Return true (continue) if hash is greater than or equal to thetaLong, or if hash == 0,
   * or if hash == Long.MAX_VALUE.
   * @param thetaLong must be greater than the hash value
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @param hash must be less than thetaLong and not less than or equal to zero.
   * @return true (continue) if hash is greater than or equal to thetaLong, or if hash == 0,
   * or if hash == Long.MAX_VALUE.
   */
  public static boolean continueCondition(final long thetaLong, final long hash) {
    //if any one of the groups go negative it returns true
    return (( (hash - 1L) | (thetaLong - hash - 1L)) < 0L );
  }

}
