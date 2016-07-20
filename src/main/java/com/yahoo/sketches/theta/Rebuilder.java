/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * Handles common resize, rebuild and move operations. 
 * The Memory based operations assume a specific data structure that is unique to the theta sketches.
 */
final class Rebuilder {
  
  private Rebuilder() {}
  
  /**
   * Rebuild the hashTable in the given Memory at its current size. Changes theta and thus count.
   * This assumes a Memory preamble of standard form with correct values of curCount and thetaLong.
   * ThetaLong and curCount will change.
   * Afterwards, caller must update local class members curCount and thetaLong from Memory.
   * 
   * @param mem the Memory the given Memory
   * @param preambleLongs size of preamble in longs
   * @param lgNomLongs the log_base2 of k, the configuration parameter of the sketch
   * @param lgArrLongs the log_base2 of the current size of the hash table
   * @param curCount the number of valid entries
   */
  static final void quickSelectAndRebuild(final Memory mem, final int preambleLongs, 
      final int lgNomLongs, final int lgArrLongs, int curCount) {
    //Pull data into tmp arr for QS algo
    int arrLongs = 1 << lgArrLongs;
    long[] tmpArr = new long[arrLongs];
    int preBytes = preambleLongs << 3;
    mem.getLongArray(preBytes, tmpArr, 0, arrLongs); //copy mem data to tmpArr
    
    //Do the QuickSelect on a tmp arr to create new thetaLong
    int pivot = (1 << lgNomLongs) + 1; // (K+1) pivot for QS
    long newThetaLong = selectExcludingZeros(tmpArr, curCount, pivot);
    mem.putLong(THETA_LONG, newThetaLong); //UPDATE thetalong
    
    //Rebuild to clean up dirty data, update count
    long[] tgtArr = new long[arrLongs];
    int newCurCount = HashOperations.hashArrayInsert(tmpArr, tgtArr, lgArrLongs, newThetaLong);
    mem.putInt(RETAINED_ENTRIES_INT, newCurCount); //UPDATE curCount
    
    //put the rebuilt array back into memory
    mem.putLongArray(preBytes, tgtArr, 0, arrLongs);
  }
  
  /**
   * Moves me (the entire sketch) to a new larger Memory location and rebuilds the hash table.
   * This assumes a Memory preamble of standard form with the correct value of thetaLong. 
   * Afterwards, the caller must update the local Memory reference, lgArrLongs 
   * and hashTableThreshold from the dstMemory and free the source Memory.
   * 
   * @param srcMem the source Memory
   * @param preambleLongs size of preamble in longs
   * @param srcLgArrLongs size (log_base2) of source hash table
   * @param dstMem the destination Memory, which may be garbage
   * @param dstLgArrLongs the destination hash table target size
   * @param thetaLong theta as a long
   */
  static final void moveAndResize(final Memory srcMem, final int preambleLongs, 
      final int srcLgArrLongs, final Memory dstMem, final int dstLgArrLongs, final long thetaLong) {
    //Move Preamble to destination memory
    int preBytes = preambleLongs << 3;
    NativeMemory.copy(srcMem, 0, dstMem, 0, preBytes); //copy the preamble
    //Bulk copy source to on-heap buffer
    int srcHTLen = 1 << srcLgArrLongs;
    long[] srcHTArr = new long[srcHTLen];
    srcMem.getLongArray(preBytes, srcHTArr, 0, srcHTLen);
    //Create destination buffer
    int dstHTLen = 1 << dstLgArrLongs;
    long[] dstHTArr = new long[dstHTLen];
    //Rebuild hash table in destination buffer
    HashOperations.hashArrayInsert(srcHTArr, dstHTArr, dstLgArrLongs, thetaLong);
    //Bulk copy to destination memory
    dstMem.putLongArray(preBytes, dstHTArr, 0, dstHTLen);
    dstMem.putByte(LG_ARR_LONGS_BYTE, (byte)dstLgArrLongs); //update in dstMem
  }
  
  /**
   * Resizes existing hash array into a larger one within a single Memory assuming enough space.
   * This assumes a Memory preamble of standard form with the correct value of thetaLong.  
   * The Memory lgArrLongs will change.
   * Afterwards, the caller must update local copies of lgArrLongs and hashTableThreshold from
   * Memory.
   * 
   * @param mem the Memory
   * @param preambleLongs the size of the preamble in longs
   * @param srcLgArrLongs the size of the source hash table
   * @param dstLgArrLongs the LgArrLongs value for the new hash table
   */
  static final void resize(final Memory mem, final int preambleLongs, 
      final int srcLgArrLongs, final int dstLgArrLongs) {
    //Preamble stays in place
    int preBytes = preambleLongs << 3;
    //Bulk copy source to on-heap buffer
    int srcHTLen = 1 << srcLgArrLongs; //current value
    long[] srcHTArr = new long[srcHTLen]; //on-heap src buffer
    mem.getLongArray(preBytes, srcHTArr, 0, srcHTLen);
    //Create destination on-heap buffer
    int dstHTLen = 1 << dstLgArrLongs;
    long[] dstHTArr = new long[dstHTLen]; //on-heap dst buffer
    //Rebuild hash table in destination buffer
    long thetaLong = mem.getLong(THETA_LONG);
    HashOperations.hashArrayInsert(srcHTArr, dstHTArr, dstLgArrLongs, thetaLong);
    //Bulk copy to destination memory
    mem.putLongArray(preBytes, dstHTArr, 0, dstHTLen); //put it back, no need to clear
    mem.putByte(LG_ARR_LONGS_BYTE, (byte) dstLgArrLongs); //update in mem
  }
  
  /**
   * Returns the actual log2 Resize Factor that can be used to grow the hash table. This will be 
   * an integer value between zero and the given lgRF, inclusive;
   * @param capBytes the current memory capacity in bytes
   * @param lgArrLongs the current lg hash table size in longs
   * @param preLongs the current preamble size in longs
   * @param lgRF the configured lg Resize Factor
   * @return the actual log2 Resize Factor that can be used to grow the hash table
   */
  static final int actLgResizeFactor(long capBytes, int lgArrLongs, int preLongs, int lgRF) {
    int maxHTLongs = Util.floorPowerOf2(((int)(capBytes >> 3) - preLongs));
    int lgFactor = Math.max(Integer.numberOfTrailingZeros(maxHTLongs) - lgArrLongs, 0);
    return (lgFactor >= lgRF) ? lgRF : lgFactor;
  }
  
}
