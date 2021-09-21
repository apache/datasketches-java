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

package org.apache.datasketches.req;

import static java.lang.Math.round;
import static org.apache.datasketches.Util.numberOfTrailingOnes;
import static org.apache.datasketches.req.ReqSketch.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.req.ReqSketch.MIN_K;
import static org.apache.datasketches.req.ReqSketch.NOM_CAP_MULT;

import java.util.Random;

import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.req.ReqSketch.CompactorReturn;

/**
 * The compactor class for the ReqSketch
 * @author Lee Rhodes
 */
class ReqCompactor {
  //finals
  private static final double SQRT2 = Math.sqrt(2.0);
  private final byte lgWeight;
  private final boolean hra;
  //state variables
  private long state; //State of the deterministic compaction schedule
  private float sectionSizeFlt;
  private int sectionSize; //initialized with k, minimum 4
  private byte numSections; //# of sections, initial size 3
  private boolean coin; //true or false at random for each compaction
  //objects
  private FloatBuffer buf;
  private final ReqDebug reqDebug = null;
  private Random rand;

  /**
   * Normal Constructor
   * @param lgWeight the lgWeight of this compactor
   * @param hra High Rank Accuracy
   * @param sectionSize initially the value of k
   * @param reqDebug The debug signaling interface
   */
  ReqCompactor(
      final byte lgWeight,
      final boolean hra,
      final int sectionSize,
      final ReqDebug reqDebug) {
    this.lgWeight = lgWeight;
    this.hra = hra;
    this.sectionSize = sectionSize;
    sectionSizeFlt = sectionSize;
    state = 0;
    coin = false;
    numSections = INIT_NUMBER_OF_SECTIONS;
    final int nomCap = getNomCapacity();
    buf = new FloatBuffer(2 * nomCap, nomCap, hra);
    if (reqDebug != null) { rand = new Random(1); }
    else { rand = new Random(); }
  }

  /**
   * Copy Constructor
   * @param other the compactor to be copied into this one
   */
  ReqCompactor(final ReqCompactor other) {
    lgWeight = other.lgWeight;
    hra = other.hra;
    sectionSizeFlt = other.sectionSizeFlt;
    numSections = other.numSections;
    sectionSize = other.sectionSize;
    state = other.state;
    coin = other.coin;
    buf = new FloatBuffer(other.buf);
  }

  /**
   * Construct from elements. The buffer will need to be constructed first
   */
  ReqCompactor(
      final byte lgWeight,
      final boolean hra,
      final long state,
      final float sectionSizeFlt,
      final byte numSections,
      final FloatBuffer buf) {
    rand = new Random();
    this.lgWeight = lgWeight;
    this.hra = hra;
    this.buf = buf;
    this.sectionSizeFlt = sectionSizeFlt;
    this.numSections = numSections;
    this.state = state;
    coin = rand.nextDouble() < 0.5;
    sectionSize = nearestEven(sectionSizeFlt);
    //ReqDebug left at null
  }

  /**
   * Perform a compaction operation on this compactor
   * @return the array of items to be promoted to the next level compactor
   */
  FloatBuffer compact(final CompactorReturn cReturn) {
    if (reqDebug != null) { reqDebug.emitCompactingStart(lgWeight); }
    final int startRetItems = buf.getCount();
    final int startNomCap = getNomCapacity();
    // choose a part of the buffer to compact
    final int secsToCompact = Math.min(numberOfTrailingOnes(state) + 1, numSections);
    final long compactionRange = computeCompactionRange(secsToCompact);
    final int compactionStart = (int) (compactionRange & 0xFFFF_FFFFL); //low 32
    final int compactionEnd = (int) (compactionRange >>> 32); //high 32
    assert compactionEnd - compactionStart >= 2;

    if ((state & 1L) == 1L) { coin = !coin; } //if numCompactions odd, flip coin;
    else { coin = rand.nextDouble() < 0.5; }       //random coin flip

    final FloatBuffer promote = buf.getEvensOrOdds(compactionStart, compactionEnd, coin);

    if (reqDebug != null) {
      reqDebug.emitCompactionDetail(compactionStart, compactionEnd, secsToCompact,
          promote.getCount(), coin);
    }

    buf.trimCount(buf.getCount() - (compactionEnd - compactionStart));
    state += 1;
    ensureEnoughSections();
    cReturn.deltaRetItems = buf.getCount() - startRetItems + promote.getCount();
    cReturn.deltaNomSize = getNomCapacity() - startNomCap;
    if (reqDebug != null) { reqDebug.emitCompactionDone(lgWeight); }
    return promote;
  } //End Compact

  /**
   * Gets a reference to this compactor's internal FloatBuffer
   * @return a reference to this compactor's internal FloatBuffer
   */
  FloatBuffer getBuffer() { return buf; }

  boolean getCoin() {
    return coin;
  }

  /**
   * Gets the lgWeight of this buffer
   * @return the lgWeight of this buffer
   */
  byte getLgWeight() {
    return lgWeight;
  }

  /**
   * Sets the current nominal capacity of this compactor.
   * @return the current nominal capacity of this compactor.
   */
  int getNomCapacity() {
    return NOM_CAP_MULT * numSections * sectionSize;
  }

  /**
   * Serialize state(8) sectionSizeFlt(4), numSections(1), lgWeight(1), pad(2), count(4) + floatArr
   * @return required bytes to serialize.
   */
  int getSerializationBytes() {
    final int count = buf.getCount();
    return 8 + 4 + 1 + 1 + 2 + 4 + count * Float.BYTES; // 20 + array
  }

  int getNumSections() {
    return numSections;
  }

  int getSectionSize() {
    return sectionSize;
  }

  float getSectionSizeFlt() {
    return sectionSizeFlt;
  }

  long getState() {
    return state;
  }

  boolean isHighRankAccuracy() {
    return hra;
  }

  /**
   * Merge the other given compactor into this one. They both must have the
   * same lgWeight
   * @param other the other given compactor
   * @return this
   */
  ReqCompactor merge(final ReqCompactor other) {
    assert lgWeight == other.lgWeight;
    state |= other.state;
    while (ensureEnoughSections()) {}
    buf.sort();
    final FloatBuffer otherBuf = new FloatBuffer(other.buf);
    otherBuf.sort();
    if (otherBuf.getCount() > buf.getCount()) {
      otherBuf.mergeSortIn(buf);
      buf = otherBuf;
    } else {
      buf.mergeSortIn(otherBuf);
    }
    return this;
  }

  /**
   * Adjust the sectionSize and numSections if possible.
   * @return true if the SectionSize and NumSections were adjusted.
   */
  private boolean ensureEnoughSections() {
    final float szf;
    final int ne;
    if (state >= 1L << numSections - 1
        && sectionSize > MIN_K
        && (ne = nearestEven(szf = (float)(sectionSizeFlt / SQRT2))) >= MIN_K)
    {
      sectionSizeFlt = szf;
      sectionSize = ne;
      numSections <<= 1;
      buf.ensureCapacity(2 * getNomCapacity());
      if (reqDebug != null) { reqDebug.emitAdjSecSizeNumSec(lgWeight); }
      return true;
    }
    return false;
  }

  /**
   * Computes the start and end indices of the compacted region
   * @param secsToCompact the number of contiguous sections to compact
   * @return the  start and end indices of the compacted region
   */
  private long computeCompactionRange(final int secsToCompact) {
    final int bufLen = buf.getCount();
    int nonCompact = getNomCapacity() / 2 + (numSections - secsToCompact) * sectionSize;
    //make compacted region even:
    nonCompact = (bufLen - nonCompact & 1) == 1 ? nonCompact + 1 : nonCompact;
    final long low =  hra ? 0                   : nonCompact;
    final long high = hra ? bufLen - nonCompact : bufLen;
    return (high << 32) + low;
  }

  /**
   * Returns the nearest even integer to the given value. Also used by test.
   * @param value the given value
   * @return the nearest even integer to the given value.
   */
  static final int nearestEven(final float value) {
    return (int) round(value / 2.0) << 1;
  }

  /**
   * ReqCompactor SERIALIZATION FORMAT.
   *
   * <p>Low significance bytes of this data structure are on the right just for visualization.
   * The multi-byte values are stored in native byte order.
   * The <i>byte</i> values are treated as unsigned. Multibyte values are indicated with "*" and
   * their size depends on the specific implementation.</p>
   *
   * <p>The binary format for a compactor: </p>
   *
   * <pre>
   * Binary Format. Starting offset is either 24 or 8, both are 8-byte aligned.
   *
   * +Long Adr / +Byte Offset
   *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
   *  0   ||-----------------------------state-------------------------------------|
   *
   *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
   *  1   ||----(empty)------|-#Sects-|--lgWt--|------------sectionSizeFlt---------|
   *
   *      ||        |        |        |        |        |        |        |   16   |
   *  2   ||--------------floats[]-------------|---------------count---------------|
   *
   * </pre>
   */
  byte[] toByteArray() {
    final int bytes = getSerializationBytes();
    final byte[] arr = new byte[bytes];
    final WritableBuffer wbuf = WritableMemory.writableWrap(arr).asWritableBuffer();
    wbuf.putLong(state);
    wbuf.putFloat(sectionSizeFlt);
    wbuf.putByte(lgWeight);
    wbuf.putByte(numSections);
    wbuf.incrementPosition(2); //pad 2
    //buf.sort(); //sort if necessary
    wbuf.putInt(buf.getCount()); //count
    wbuf.putByteArray(buf.floatsToBytes(), 0, Float.BYTES * buf.getCount());
    assert wbuf.getPosition() == bytes;
    return arr;
  }

  /**
   * Returns a printable formatted prefix string summarizing the list.
   * The first number is the compactor height. the second number in brackets is the current count
   * of the compactor buffer. The third number in brackets is the nominal capacity of the compactor.
   * @return a printable formatted prefix string summarizing the list.
   */
  String toListPrefix() {
    final int h = getLgWeight();
    final int len = buf.getCount();
    final int nom = getNomCapacity();
    final int secSz = getSectionSize();
    final int numSec = getNumSections();
    final long num = getState();
    final String prefix = String.format(
      "  C:%d Len:%d NomSz:%d SecSz:%d NumSec:%d State:%d",
           h, len, nom, secSz, numSec, num);
    return prefix;
  }

}
