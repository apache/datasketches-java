/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.memory.Memory;

/**
 * Helper class for the common hash table methods.
 * 
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class HashOperations {
  private static final int STRIDE_HASH_BITS = 7; 
  static final int STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

  private HashOperations() {}

  /**
   * Counts the cardinality of the first Log2 values of the given source array.
   * @param srcArr the given source array
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>
   * @param thetaLong <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @return the cardinality
   */
  static int countPart(long[] srcArr, int lgArrLongs, long thetaLong) {
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
  static int count(long[] srcArr, long thetaLong) {
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
  
  /**
   * This is a classical Knuth-style Open Addressing, Double Hash search scheme.
   * 
   * @param hashTable The hash table to search. Must be a power of 2 in size.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).  
   * @param hash A hash value to search for. Must not be zero.
   * @return Current probe index if found, -1 if not found.
   */
  static int hashSearch(long[] hashTable, int lgArrLongs, long hash) {
    if (hash == 0) throw new IllegalArgumentException("Given hash cannot be zero: "+hash);
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    // make odd and independent of curProbe:
    int stride = (2 * (int) ((hash >> lgArrLongs) & STRIDE_MASK)) + 1;
    int curProbe = (int) (hash & arrayMask);
    long curArrayHash = hashTable[curProbe];
    // search for duplicate or zero
    while ((curArrayHash != hash) && (curArrayHash != 0)) {
      curProbe = (curProbe + stride) & arrayMask;
      curArrayHash = hashTable[curProbe];
    }
    // curArrayHash is a duplicate or zero
    return (curArrayHash == hash) ? curProbe : -1;
  }
  
  /**
   * Inserts the given long array into the given hash table array of the target size,
   * removes any negative input values, ignores duplicates and counts the values inserted. 
   * The given hash table may have values, but they must have been inserted by this method or one 
   * of the other OADH insert methods in this class and they may not be dirty.
   * 
   * @param srcArr the source hash array to be potentially inserted
   * @param hashTable The correctly sized target hash table that must be a power of two. 
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(hashTable.length).
   * @param thetaLong must greater than zero 
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @return the count of values actually inserted
   */
  static int hashArrayInsert(long[] srcArr, long[] hashTable, int lgArrLongs, long thetaLong) {
    int count = 0;
    int arrLen = srcArr.length;
    checkThetaCorruption(thetaLong); 
    for (int i = 0; i < arrLen; i++ ) { // scan source array, build target array
      long hash = srcArr[i];
      checkHashCorruption(hash);
      if (continueCondition(thetaLong, hash) ) { 
        continue; 
      }
      if (hashInsert(hashTable, lgArrLongs, hash)) {
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
   * @return True if hash was inserted and count must be incremented.
   */
  static boolean hashInsert(long[] hashTable, int lgArrLongs, long hash) {
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    // make odd and independent of curProbe:
    int stride = (2 * (int) ((hash >> lgArrLongs) & STRIDE_MASK)) + 1;
    int curProbe = (int) (hash & arrayMask);
    long curArrayHash = hashTable[curProbe];
    // search for duplicate or zero
    while ((curArrayHash != hash) && (curArrayHash != 0)) {
      // curArrayHash is not a duplicate and not zero, continue searching
      curProbe = (curProbe + stride) & arrayMask;
      curArrayHash = hashTable[curProbe];
    }
    // curArrayHash is a duplicate or zero
    if (curArrayHash == hash) {
      return false; // duplicate
    }
    // must be zero, so insert
    hashTable[curProbe] = hash;
    return true;
  }

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme, but inserts
   * values directly into a Memory.
   * 
   * @param mem The Memory hash table to insert into.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * lgArrLongs &le; log2(internal Memory hashTable length).
   * @param hash A hash value that must not be zero and if not a duplicate will be inserted into the
   * array into an empty slot.
   * @param memOffsetBytes offset in the memory where the hash array starts
   * @return True if hash was inserted and count must be incremented.
   */
  static boolean hashInsert(Memory mem, int lgArrLongs, long hash, int memOffsetBytes) {
    int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
    // make stride odd and independent of curProbe:
    int stride = (2 * (int) ((hash >> lgArrLongs) & STRIDE_MASK)) + 1;
    int curProbe = (int) (hash & arrayMask);
    int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
    long curArrayHash = mem.getLong(curProbeOffsetBytes);
    // search for duplicate or zero
    while ((curArrayHash != hash) && (curArrayHash != 0)) {
      // curArrayHash is not a duplicate and not zero, continue searching
      curProbe = (curProbe + stride) & arrayMask;
      curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
      curArrayHash = mem.getLong(curProbeOffsetBytes);
    }
    // curArrayHash is a duplicate or zero
    if (curArrayHash == hash) {
      return false; // duplicate
    }
    // must be zero, so insert
    mem.putLong(curProbeOffsetBytes, hash);
    return true;
  }
  
  /**
   * @param thetaLong must be greater than zero otherwise throws an exception.
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   */
  static void checkThetaCorruption(final long thetaLong) {
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
  static void checkHashCorruption(final long hash) {
    //if any one of the groups go negative it fails.
    if ( hash < 0L ) {
      throw new IllegalStateException(
          "Data Corruption: hash was negative: "+ "Hash: "+hash);
    }
  }
  
  /**
   * Return true if thetaLong is greater than hash, or if hash == 0
   * @param thetaLong thetaLong must be greater than the hash value
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">See Theta Long</a>
   * @param hash must be less than thetaLong and not zero
   * @return true if thetaLong is greater than hash, or if hash == 0
   */
  static boolean continueCondition(final long thetaLong, final long hash) {
    //if any one of the groups go negative it returns true
    return (( (hash-1) | (thetaLong - hash -1)) < 0L );
  }
  
  /**
   * @param thetaLong cannot be negative or zero, otherwise it throws an exception
   * @param hash cannot be negative, otherwise it throws an exception
   */
  static void checkHashAndThetaCorruption(final long thetaLong, final long hash) {
    //if any one of the groups go negative it fails.
    if (( hash | thetaLong | (thetaLong-1) ) < 0L ) {
      throw new IllegalStateException(
          "Data Corruption: Either hash was negative or thetaLong was negative or zero: "+
          "Hash: "+hash+", ThetaLong: "+thetaLong);
    }
  }
  
}