/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRequest;
import com.yahoo.sketches.memory.MemoryUtil;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * Handles common resize, rebuild and move operations. 
 * The Memory based operations assume a specific data structure that is unique to the theta sketches.
 */
final class Rebuilder {
  
  private Rebuilder() {}
  
  static final int getMemBytes(int lgArrLongs, int preambleLongs) {
    return (8 << lgArrLongs) + (preambleLongs << 3);
  }
  
  static final int getReqMemBytesFull(int lgNomLongs, int preambleLongs) {
    return (16 << lgNomLongs) + (preambleLongs << 3);
  }
  
  //Only used when MemoryRequest is not available
  static final void freeMem(Memory mem) { 
    MemoryRequest memReq = mem.getMemoryRequest();
    if (memReq != null) {
      memReq.free(mem);
    }
    else if (mem instanceof NativeMemory) {
      ((NativeMemory)mem).freeMemory();
    }
  }
  
  //Might change in Memory: thetaLong, curCount, mem_, lgArrLongs, hashTableThreshold
  /**
   * This is called when the hash table in the given srcMem must grow or be rebuilt. It may grow
   * within the given srcMem, or, if necessary, and if a MemoryRequest callback is available, it
   * can grow by moving the sketch image to a new Memory, in which case the original srcMem will
   * be freed by calling the MemoryRequest.free(memoryToFree, newMemory).
   * Afterwards the caller must update the local Memory reference, and curCount, thetaLong,
   * lgArrLongs from the returned Memory and recompute the local hashTableThreshold.
   *  
   * @param srcMem The source Memory
   * @param preambleLongs this is basically the offset prior to the start of the hash table.
   * @param lgNomLongs the inherent <i>k</i> value of the sketch required for rebuilding.
   * @param lgArrLongs the current size of the hash table.
   * @param curCount the current hash count
   * @return the original srcMem or a new destination Memory if required.
   */
  static final Memory resizeMoveOrRebuild(Memory srcMem, final int preambleLongs, 
      final int lgNomLongs, final int lgArrLongs, final int curCount, long thetaLong) {
    // curMemBytes < reqMemBytes <= reqMemBytesFull
    // curMemBytes <= curCapBytes
    int curMemBytes = getMemBytes(lgArrLongs, preambleLongs);
    int reqMemBytesFull = getReqMemBytesFull(lgNomLongs, preambleLongs);
    Memory dstMem = srcMem; //initially assume we stay put
    
    //At Full Size or Not
    if (curMemBytes >= reqMemBytesFull) { //should never be >. This is a safety net
      //Already at tgt full size, must quickselect and rebuild 
      //Assumes no dirty values, changes thetaLong_, curCount_
      assert (lgArrLongs == lgNomLongs + 1) : "lgArr: " + lgArrLongs + ", lgNom: " + lgNomLongs;
      quickSelectAndRebuild(srcMem, preambleLongs, lgNomLongs, lgArrLongs, curCount);  //rebuild
      //Remember to reset local variables curCount and thetaLong
    }
    else { //Not at full size
      int dstLgArrLongs = lgArrLongs + 1; //assume RF = 2
      int reqBytes = getMemBytes(dstLgArrLongs, preambleLongs);
      long curCapBytes = srcMem.getCapacity();
      
      //Expand in current Memory?
      if (reqBytes <= curCapBytes) { //yes
        resize(srcMem, preambleLongs, lgArrLongs, dstLgArrLongs);
        //Remember to reset local variables lgArrLongs and hashTableThreshold
      }
      else { //no, request a bigger space
        MemoryRequest memReq = srcMem.getMemoryRequest();
        dstMem = memReq.request(reqBytes);
        if (dstMem == null) {
          throw new IllegalArgumentException("Requested memory cannot be null.");
        }
        long newCap = dstMem.getCapacity();
        if (newCap < reqBytes) {
          freeMem(dstMem);
          throw new IllegalArgumentException("Requested memory not granted: "+newCap+" < "+reqBytes);
        }
        moveAndResize(srcMem, preambleLongs, lgArrLongs, dstMem, dstLgArrLongs, thetaLong);
        //Reset local variables and free the srcMem
        
        memReq.free(srcMem, dstMem);
      } //end of expand in current mem?
      
    } //end of At Full Size or not
    return dstMem;
  }
  
  /**
   * Rebuild the hashTable in the given Memory at its current size. Changes theta and thus count.
   * This assumes a Memory preamble of standard form with correct values of curCount and thetaLong.
   * ThetaLong and curCount will change.
   * Afterwards, caller must update local class members curCount and thetaLong from Memory.
   * 
   * @param mem the Memory
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
   * Afterwards, the caller must update the local Memory reference, lgArrLongs, hashTableThreshold, 
   * from the dstMemory and free the source Memory.
   * 
   * @param srcMem the source Memory
   * @param dstMem the destination Memory, which may be garbage
   * @param dstLgArrLongs the destination hash table target size
   */
  private static final void moveAndResize(final Memory srcMem, final int preambleLongs, 
      final int srcLgArrLongs, final Memory dstMem, final int dstLgArrLongs, final long thetaLong) {
    //Move Preamble to destination memory
    int preBytes = preambleLongs << 3;
    MemoryUtil.copy(srcMem, 0, dstMem, 0, preBytes); //copy the preamble
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
   * @param dstLgArrLongs the LgArrLongs value for the new hash table
   */
  private static final void resize(final Memory mem, final int preambleLongs, 
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
  
}
