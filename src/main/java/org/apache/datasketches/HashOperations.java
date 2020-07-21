/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches;

import static java.lang.Math.max;
import static org.apache.datasketches.Util.MIN_LG_ARR_LONGS;
import static org.apache.datasketches.Util.ceilingPowerOf2;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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

  //Make odd and independent of index assuming lgArrLongs lowest bits of the hash were used for
  //  index. This results in a 8 bit value that is always odd.
  private static int getStride(final long hash, final int lgArrLongs) {
    return (2 * (int) ((hash >>> lgArrLongs) & STRIDE_MASK) ) + 1;
  }

  //ON-HEAP

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash (OADH) search scheme for on-heap.
   * Returns the index if found, -1 if not found.
   *
   * @param hashTable The hash table to search. Its size must be a power of 2.
   * @param lgArrLongs The log_base2(hashTable.length).
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @param hash The hash value to search for. It must not be zero.
   * @return Current probe index if found, -1 if not found.
   */
  public static int hashSearch(final long[] hashTable, final int lgArrLongs, final long hash) {
    if (hash == 0) {
      throw new SketchesArgumentException("Given hash must not be zero: " + hash);
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
   * This is a classical Knuth-style Open Addressing, Double Hash (OADH) insert scheme for on-heap.
   * This method assumes that the input hash is not a duplicate.
   * Useful for rebuilding tables to avoid unnecessary comparisons.
   * Returns the index of insertion, which is always positive or zero.
   * Throws an exception if the table has no empty slot.
   *
   * @param hashTable the hash table to insert into. Its size must be a power of 2.
   * @param lgArrLongs The log_base2(hashTable.length).
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @param hash The hash value to be potentially inserted into an empty slot. It must not be zero.
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
   * This is a classical Knuth-style Open Addressing, Double Hash (OADH) insert scheme for on-heap.
   * Returns index &ge; 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
   * Throws an exception if the value is not found and table has no empty slot.
   *
   * @param hashTable The hash table to insert into. Its size must be a power of 2.
   * @param lgArrLongs The log_base2(hashTable.length).
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @param hash The hash value to be potentially inserted into an empty slot only if it is not
   * a duplicate of any other hash value in the table. It must not be zero.
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
    throw new SketchesArgumentException("Hash not found and no empty slots!");
  }

  /**
   * Inserts the given long array into the given OADH hashTable of the target size,
   * ignores duplicates and counts the values inserted.
   * The hash values must not be negative, zero values and values &ge; thetaLong are ignored.
   * The given hash table may have values, but they must have been inserted by this method or one
   * of the other OADH insert methods in this class.
   * This method performs additional checks against potentially invalid hash values or theta values.
   * Returns the count of values actually inserted.
   *
   * @param srcArr the source hash array to be potentially inserted
   * @param hashTable The hash table to insert into. Its size must be a power of 2.
   * @param lgArrLongs The log_base2(hashTable.length).
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @param thetaLong The theta value that all input hash values are compared against.
   * It must greater than zero.
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

  //With Memory or WritableMemory

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash (OADH) search scheme for Memory.
   * Returns the index if found, -1 if not found.
   *
   * @param mem The <i>Memory</i> containing the hash table to search.
   * The hash table portion must be a power of 2 in size.
   * @param lgArrLongs The log_base2(hashTable.length).
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @param hash The hash value to search for. Must not be zero.
   * @param memOffsetBytes offset in the memory where the hashTable starts
   * @return Current probe index if found, -1 if not found.
   */
  public static int hashSearchMemory(final Memory mem, final int lgArrLongs, final long hash,
      final int memOffsetBytes) {
    if (hash == 0) {
      throw new SketchesArgumentException("Given hash must not be zero: " + hash);
    }
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
   * This is a classical Knuth-style Open Addressing, Double Hash (OADH) insert scheme for Memory.
   * This method assumes that the input hash is not a duplicate.
   * Useful for rebuilding tables to avoid unnecessary comparisons.
   * Returns the index of insertion, which is always positive or zero.
   * Throws an exception if table has no empty slot.
   *
   * @param wmem The <i>WritableMemory</i> that contains the hashTable to insert into.
   * The size of the hashTable portion must be a power of 2.
   * @param lgArrLongs The log_base2(hashTable.length.
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @param hash value that must not be zero and will be inserted into the array into an empty slot.
   * @param memOffsetBytes offset in the <i>WritableMemory</i> where the hashTable starts
   * @return index of insertion.  Always positive or zero.
   */
  public static int hashInsertOnlyMemory(final WritableMemory wmem, final int lgArrLongs,
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

  /**
   * This is a classical Knuth-style Open Addressing, Double Hash insert scheme, but inserts
   * values directly into a Memory.
   * Returns index &ge; 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
   * Throws an exception if the value is not found and table has no empty slot.
   *
   * @param wmem The <i>WritableMemory</i> that contains the hashTable to insert into.
   * @param lgArrLongs The log_base2(hashTable.length).
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @param hash The hash value to be potentially inserted into an empty slot only if it is not
   * a duplicate of any other hash value in the table. It must not be zero.
   * @param memOffsetBytes offset in the <i>WritableMemory</i> where the hash array starts
   * @return index &ge; 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
   */
  public static int hashSearchOrInsertMemory(final WritableMemory wmem, final int lgArrLongs,
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

  //Other related methods

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

  /**
   * Converts the given array to a hash table.
   * @param hashArr The given array of hashes. Gaps are OK.
   * @param count The number of valid hashes in the array
   * @param thetaLong Any hashes equal to or greater than thetaLong will be ignored
   * @param rebuildThreshold The fill fraction for the hash table forcing a rebuild or resize.
   * @return a HashTable
   */
  public static long[] convertToHashTable(
      final long[] hashArr,
      final int count,
      final long thetaLong,
      final double rebuildThreshold) {
    final int lgArrLongs = minLgHashTableSize(count, rebuildThreshold);
    final int arrLongs = 1 << lgArrLongs;
    final long[] hashTable = new long[arrLongs];
    hashArrayInsert(hashArr, hashTable, lgArrLongs, thetaLong);
    return hashTable;
  }

  /**
   * Returns the smallest log hash table size given the count of items and the rebuild threshold.
   * @param count the given count of items
   * @param rebuild_threshold the rebuild threshold as a fraction between zero and one.
   * @return the smallest log hash table size
   */
  public static int minLgHashTableSize(final int count, final double rebuild_threshold) {
    final int upperCount = (int) Math.ceil(count / rebuild_threshold);
    final int arrLongs = max(ceilingPowerOf2(upperCount), 1 << MIN_LG_ARR_LONGS);
    final int newLgArrLongs = Integer.numberOfTrailingZeros(arrLongs);
    return newLgArrLongs;
  }

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

}
