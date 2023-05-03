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

import static org.apache.datasketches.theta.CompactOperations.checkIllegalCurCountAndEmpty;
import static org.apache.datasketches.theta.CompactOperations.correctThetaOnCompact;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.insertFlags;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.apache.datasketches.theta.PreambleUtil.insertP;
import static org.apache.datasketches.theta.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * The parent class for Heap Updatable Theta Sketches.
 *
 * @author Lee Rhodes
 */
abstract class HeapUpdateSketch extends UpdateSketch {
  final int lgNomLongs_;
  private final long seed_;
  private final float p_;
  private final ResizeFactor rf_;

  HeapUpdateSketch(final int lgNomLongs, final long seed, final float p, final ResizeFactor rf) {
    lgNomLongs_ = Math.max(lgNomLongs, ThetaUtil.MIN_LG_NOM_LONGS);
    seed_ = seed;
    p_ = p;
    rf_ = rf;
  }

  //Sketch

  @Override
  public int getCurrentBytes() {
    final int preLongs = getCurrentPreambleLongs();
    final int dataLongs = getCurrentDataLongs();
    return (preLongs + dataLongs) << 3;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean hasMemory() {
    return false;
  }

  //UpdateSketch

  @Override
  public final int getLgNomLongs() {
    return lgNomLongs_;
  }

  @Override
  float getP() {
    return p_;
  }

  @Override
  public ResizeFactor getResizeFactor() {
    return rf_;
  }

  @Override
  long getSeed() {
    return seed_;
  }

  //restricted methods

  @Override
  short getSeedHash() {
    return ThetaUtil.computeSeedHash(getSeed());
  }

  //Used by HeapAlphaSketch and HeapQuickSelectSketch / Theta UpdateSketch
  byte[] toByteArray(final int preLongs, final byte familyID) {
    if (isDirty()) { rebuild(); }
    checkIllegalCurCountAndEmpty(isEmpty(), getRetainedEntries(true));
    final int preBytes = (preLongs << 3) & 0X3F; //24 bytes
    final int dataBytes = getCurrentDataLongs() << 3;
    final byte[] byteArrOut = new byte[preBytes + dataBytes];
    final WritableMemory memOut = WritableMemory.writableWrap(byteArrOut);

    //preamble first 8 bytes. Note: only compact can be reduced to 8 bytes.
    final int lgRf = getResizeFactor().lg() & 0x3;
    insertPreLongs(memOut, preLongs);          //byte 0 low  6 bits
    insertLgResizeFactor(memOut, lgRf);        //byte 0 high 2 bits
    insertSerVer(memOut, SER_VER);             //byte 1
    insertFamilyID(memOut, familyID);          //byte 2
    insertLgNomLongs(memOut, getLgNomLongs()); //byte 3
    insertLgArrLongs(memOut, getLgArrLongs()); //byte 4
    insertSeedHash(memOut, getSeedHash());     //bytes 6 & 7

    insertCurCount(memOut, this.getRetainedEntries(true));
    insertP(memOut, getP());
    final long thetaLong =
        correctThetaOnCompact(isEmpty(), getRetainedEntries(true), getThetaLong());
    insertThetaLong(memOut, thetaLong);

    //Flags: BigEnd=0, ReadOnly=0, Empty=X, compact=0, ordered=0
    final byte flags = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    insertFlags(memOut, flags);

    //Data
    final int arrLongs = 1 << getLgArrLongs();
    final long[] cache = getCache();
    memOut.putLongArray(preBytes, cache, 0, arrLongs); //load byteArrOut

    return byteArrOut;
  }

}
