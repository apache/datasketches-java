/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

import com.yahoo.sketches.memory.Memory;

/**
 * Helper class for the common hash table methods.
 * 
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class HashOperations {
  private static final int STRIDE_HASH_BITS = 7; 
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
  public static int countPart(long[] srcArr, int lgArrLongs, long thetaLong) {
    int cnt = 0;
    int len = 1 << lgArrLongs;
    for (int i = len; i-- > 0;) {
      long hash = srcArr[i];
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
  public static int count(long[] srcArr, long thetaLong) {
    int cnt = 0;
    int len = srcArr.length;
    for (int i = len; i-- > 0;) {
      long hash = srcArr[i];
      if (continueCondition(thetaLong, hash) ) { 
        continue; 
      }
      cnt++ ;
    }
    return cnt;
  }

  // make odd and independent of index assuming lgArrLongs lowest bits of the hash were used for index
  private static int getStride(long hash, int lgArrLongs) {
    return (2 * (int) ((hash >> (lgArrLongs)) & STRIDE_MASK)) + 1;
  }

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash search scheme.
   * 
   * @param hashTable The hash table to search. Must be a power of 2 in size.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash A hash value to search for. Must not be zero.
   * @return Current probe index if found, -1 if not found.
   */
  public static int hashSearch(long[] hashTable, int lgArrLongs, long hash) {
    if (hash == 0) throw new IllegalArgumentException("Given hash cannot be zero: "+hash);
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    // search for duplicate or zero
    while (hashTable[curProbe] != 0) {
      if (hashTable[curProbe] == hash) return curProbe; // a duplicate
      curProbe = (curProbe + stride) & arrayMask;
    }
    return -1;
  }
  
  /**
   * Inserts the given long array into the given hash table array of the target size,
   * removes any negative input values, ignores duplicates and counts the values inserted. 
   * The given hash table may have values, but they must have been inserted by this method or one 
   * of the other OADH insert methods in this class and they may not be dirty. 
   * This method performs additional checks against potentially invalid hash values or theta values.
   * 
   * @param srcArr the source hash array to be potentially inserted
   * @param hashTable The correctly sized target hash table that must be a power of two. 
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param thetaLong must greater than zero 
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @return the count of values actually inserted
   */
  public static int hashArrayInsert(long[] srcArr, long[] hashTable, int lgArrLongs, long thetaLong) {
    int count = 0;
    int arrLen = srcArr.length;
    checkThetaCorruption(thetaLong); //TODO only place used
    for (int i = 0; i < arrLen; i++ ) { // scan source array, build target array
      long hash = srcArr[i];
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

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme.
   * 
   * @param hashTable the hash table to insert into.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash hash value that must not be zero and if not a duplicate will be inserted into the
   * array into an empty slot
   * @return index if found, -(index + 1) if inserted
   */
  public static int hashSearchOrInsert(long[] hashTable, int lgArrLongs, long hash) {
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    // search for duplicate or zero
    while (hashTable[curProbe] != 0) {
      if (hashTable[curProbe] == hash) return curProbe; // a duplicate
      // not a duplicate and not zero, continue searching
      curProbe = (curProbe + stride) & arrayMask;
    }
    // must be zero, so insert
    hashTable[curProbe] = hash;
    return ~curProbe;
  }

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme.
   * This method assumes that the input hash is not a duplicate.
   * Useful for rebuilding tables to avoid unnecessary comparisons.
   *
   * @param hashTable the hash table to insert into.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash value that must not be zero and will be inserted into the array into an empty slot.
   * @return index of insertion.
   */
  public static int hashInsertOnly(long[] hashTable, int lgArrLongs, long hash) {
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    while (hashTable[curProbe] != 0) {
      curProbe = (curProbe + stride) & arrayMask;
    }
    hashTable[curProbe] = hash;
    return curProbe;
  }

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme, but inserts
   * values directly into a Memory.
   * 
   * @param mem The Memory hash table to insert into.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash A hash value that must not be zero and if not a duplicate will be inserted into the
   * array into an empty slot.
   * @param memOffsetBytes offset in the memory where the hash array starts
   * @return index if found, -(index + 1) if inserted
   */
  public static int hashSearchOrInsert(Memory mem, int lgArrLongs, long hash, int memOffsetBytes) {
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes; 
    long curArrayHash = mem.getLong(curProbeOffsetBytes);
    // search for duplicate or zero
    while (curArrayHash != 0) {
      if (curArrayHash == hash) return curProbe; // curArrayHash is a duplicate
      // curArrayHash is not a duplicate and not zero, continue searching
      curProbe = (curProbe + stride) & arrayMask;
      curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
      curArrayHash = mem.getLong(curProbeOffsetBytes);
    }
    // must be zero, so insert
    mem.putLong(curProbeOffsetBytes, hash);
    return ~curProbe;
  }
  
  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme, but inserts
   * values directly into a Memory.
   * This method assumes that the input hash is not a duplicate.
   * Useful for rebuilding tables to avoid unnecessary comparisons.
   *
   * @param mem The Memory hash table to insert into.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param hash value that must not be zero and will be inserted into the array into an empty slot.
   * @param memOffsetBytes offset in the memory where the hash array starts
   * @return index of insertion.
   */
  public static int hashInsertOnly(Memory mem, int lgArrLongs, long hash, int memOffsetBytes) {
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    int stride = getStride(hash, lgArrLongs);
    int curProbe = (int) (hash & arrayMask);
    int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes; 
    long curArrayHash = mem.getLong(curProbeOffsetBytes);
    // search for duplicate or zero
    while (curArrayHash != 0L) {
      curProbe = (curProbe + stride) & arrayMask;
      curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
      curArrayHash = mem.getLong(curProbeOffsetBytes);
    }
    mem.putLong(curProbeOffsetBytes, hash);
    return curProbe;
  }
  
  /**
   * @param thetaLong must be greater than zero otherwise throws an exception.
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   */
  public static void checkThetaCorruption(final long thetaLong) {
    //if any one of the groups go negative it fails.
    if (( thetaLong | (thetaLong-1) ) < 0L ) {
      throw new IllegalStateException(
          "Data Corruption: thetaLong was negative or zero: "+ "ThetaLong: "+thetaLong);
    }
  }
  
  /**
   * @param hash must be greater than -1 otherwise throws an exception.
   * Note a hash of zero is normally ignored, but a negative hash is never allowed.
   */
  public static void checkHashCorruption(final long hash) {
    //if any one of the groups go negative it fails.
    if ( hash < 0L ) {
      throw new IllegalArgumentException(
          "Data Corruption: hash was negative: "+ "Hash: "+hash);
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
    return (( (hash-1L) | (thetaLong - hash -1L)) < 0L );
  }
  
  /**
   * Checks for invalid values of both a hash value and of a theta value.
   * @param thetaLong cannot be negative or zero, otherwise it throws an exception
   * @param hash cannot be negative, otherwise it throws an exception
   */
  public static void checkHashAndThetaCorruption(final long thetaLong, final long hash) {
    //if any one of the groups go negative it fails.
    if (( hash | thetaLong | (thetaLong-1L) ) < 0L ) {
      throw new IllegalStateException(
          "Data Corruption: Either hash was negative or thetaLong was negative or zero: "+
          "Hash: "+hash+", ThetaLong: "+thetaLong);
    }
  }
  
}
