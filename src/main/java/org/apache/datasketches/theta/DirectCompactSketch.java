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

package org.apache.datasketches.theta;

import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;

import org.apache.datasketches.memory.Memory;

/**
 * Parent class of the Direct Compact Sketches.
 *
 * @author Lee Rhodes
 */
abstract class DirectCompactSketch extends CompactSketch {
  final Memory mem_;

  DirectCompactSketch(final Memory mem) {
    mem_ = mem;
  }

  //Sketch

//  @Override //ordered, on-heap
//  public CompactSketch compact() {
//    //TODO
//    return null;
//  }
//
//  @Override
//  public CompactSketch compact(final boolean dstOrdered, final WritableMemory wmem) {
//    final int srcFlags = extractFlags(mem_);
//    final boolean srcOrdered = (srcFlags & ORDERED_FLAG_MASK) > 0;
//    final int srcPreLongs = extractPreLongs(mem_);
//    final int srcCurCount = (srcPreLongs == 1) ? 0 : extractCurCount(mem_);
//    final long srcThetaLong = (srcPreLongs <= 2) ? Long.MAX_VALUE : extractThetaLong(mem_);
//    final int bytes = (srcPreLongs + srcCurCount) << 3;
//    if (srcCurCount == 0) {
//      if (srcThetaLong == Long.MAX_VALUE) {
//        //this sets the ordered to true independent of the dstOrdered request
//        return EmptyCompactSketch.getInstance().compact(true, wmem);
//      } else {
//        assert srcPreLongs == 3 : "Theta < 1.0, thus PreLong must be 3: " + srcPreLongs;
//        mem_.copyTo(0, wmem, 0, srcPreLongs << 3);
//        if (dstOrdered) {
//          return new DirectCompactOrderedSketch(wmem);
//        } else {
//          return new DirectCompactUnorderedSketch(wmem);
//        }
//      }
//    }
//    if (srcCurCount == 1) {
//      if (srcThetaLong == Long.MAX_VALUE) {
//        //TODO
//      }
//    }
//    if (!srcOrdered && dstOrdered) { //sort this src mem and place in wmem
//      if (srcCurCount == 0) {
//        final long thetaLong = extractThetaLong(mem_);
//        if (thetaLong == Long.MAX_VALUE) {
//          //TODO
//        }
//      } else {
//        final byte[] srcBytes = new byte[bytes];
//        mem_.getByteArray(0, srcBytes, 0, bytes);
//        wmem.putByteArray(0, srcBytes, 0, bytes);
//        final byte dstFlags = (byte) (srcFlags & ORDERED_FLAG_MASK);
//        wmem.putByte(FLAGS_BYTE, dstFlags);
//      }
//
//    } else {
//      mem_.copyTo(0, wmem, 0, bytes);
//    }
//
//    return null;  //TODO
//  }

  //overidden by EmptyCompactSketch and SingleItemSketch
  @Override
  public int getCurrentBytes(final boolean compact) { //compact is ignored here
    final int preLongs = getCurrentPreambleLongs(true);
    //preLongs > 1
    final int curCount = extractCurCount(mem_);
    return (preLongs + curCount) << 3;
  }

  @Override
  public double getEstimate() {
    final int curCount = extractCurCount(mem_);
    final int preLongs = extractPreLongs(mem_);
    final long thetaLong = (preLongs > 2) ? extractThetaLong(mem_) : Long.MAX_VALUE;
    return Sketch.estimate(thetaLong, curCount);
  }

  //overidden by EmptyCompactSketch and SingleItemSketch
  @Override
  public int getRetainedEntries(final boolean valid) { //compact is always valid
    //preLongs > 1
    return extractCurCount(mem_);
  }

  @Override
  public long getThetaLong() {
    final int preLongs = extractPreLongs(mem_);
    return (preLongs > 2) ? extractThetaLong(mem_) : Long.MAX_VALUE;
  }

  @Override
  public boolean hasMemory() {
    return true;
  }

  @Override
  public boolean isDirect() {
    return mem_.isDirect();
  }

  @Override
  public boolean isEmpty() {
    return PreambleUtil.isEmptySketch(mem_);
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return mem_.isSameResource(that);
  }

  @Override
  public HashIterator iterator() {
    return new MemoryHashIterator(mem_, getRetainedEntries(), getThetaLong());
  }

  @Override //order is already determined.
  public byte[] toByteArray() {
    final int curCount = getRetainedEntries(true);
    Sketch.checkIllegalCurCountAndEmpty(isEmpty(), curCount);
    final int preLongs = getCurrentPreambleLongs(true);
    final int outBytes = (curCount + preLongs) << 3;
    final byte[] byteArrOut = new byte[outBytes];
    mem_.getByteArray(0, byteArrOut, 0, outBytes); //copies the whole thing
    return byteArrOut;
  }

  //restricted methods




  @Override
  long[] getCache() {
    final int curCount = getRetainedEntries(true);
    if (curCount > 0) {
      final long[] cache = new long[curCount];
      final int preLongs = getCurrentPreambleLongs(true);
      mem_.getLongArray(preLongs << 3, cache, 0, curCount);
      return cache;
    }
    return new long[0];
  }

  @Override
  int getCurrentPreambleLongs(final boolean compact) { //already compact; ignore
    return extractPreLongs(mem_);
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  @Override
  short getSeedHash() {
    return (short) extractSeedHash(mem_);
  }
}
