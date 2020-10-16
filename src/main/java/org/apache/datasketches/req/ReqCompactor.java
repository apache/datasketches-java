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

import java.util.Random;

import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;

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
  private double sectionSizeDbl;
  private int sectionSize; //initialized with k, minimum 4
  private int numSections; //# of sections, initial size 3
  private int numCompactions; //number of compaction operations performed
  private int state; //State of the deterministic compaction schedule
  private boolean coin; //true or false at random for each compaction
  //objects
  private FloatBuffer buf;
  private ReqDebug reqDebug = null;
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
    sectionSizeDbl = sectionSize;
    numCompactions = 0;
    state = 0;
    coin = false;
    numSections = INIT_NUMBER_OF_SECTIONS;
    final int nomCap = getNomCapacity();
    buf = new FloatBuffer(2 * nomCap, 0, hra); //TODO configure delta?
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
    sectionSizeDbl = other.sectionSizeDbl;
    numSections = other.numSections;
    sectionSize = other.sectionSize;
    numCompactions = other.numCompactions;
    state = other.state;
    coin = other.coin;
    buf = new FloatBuffer(other.buf);
  }

  /**
   * Construct from elements. The buffer will need to be constructed first
   */
  private ReqCompactor(
      final byte lgWeight,
      final boolean hra,
      final FloatBuffer buf,
      final double sectionSizeDbl,
      final int numSections,
      final int numCompactions,
      final int state,
      final boolean coin) {
    this.lgWeight = lgWeight;
    this.hra = hra;
    this.buf = buf;
    this.sectionSizeDbl = sectionSizeDbl;
    this.numSections = numSections;
    this.numCompactions = numCompactions;
    this.state = state;
    this.coin = coin;
    sectionSize = nearestEven(sectionSizeDbl);
    //ReqDebug left at null
  }

  static ReqCompactor heapify(final Buffer buff) {
    final double sectionSizeDbl = buff.getDouble();
    final int numSections = buff.getInt();
    final int numCompactions = buff.getInt();
    final int state = buff.getInt();
    final byte lgWeight = buff.getByte();
    final boolean coin = buff.getBoolean();
    final boolean hra = buff.getBoolean();
    buff.incrementPosition(1);
    final FloatBuffer buf = FloatBuffer.heapify(buff.region());
    return new ReqCompactor(lgWeight, hra, buf, sectionSizeDbl, numSections, numCompactions, state,
        coin);
  }

  /**
   * Perform a compaction operation on this compactor
   * @return the array of items to be promoted to the next level compactor
   */
  FloatBuffer compact() {
    if (reqDebug != null) { reqDebug.emitCompactingStart(lgWeight); }
    buf.sort();
    // choose a part of the buffer to compact
    final int secsToCompact = Math.min(numberOfTrailingOnes(state) + 1, numSections);
    final long compactionRange = computeCompactionRange(secsToCompact);
    final int compactionStart = (int) (compactionRange & 0xFFFF_FFFFL); //low 32
    final int compactionEnd = (int) (compactionRange >>> 32); //high 32
    assert compactionEnd - compactionStart >= 2;

    if ((numCompactions & 1) == 1) { coin = !coin; } //if numCompactions odd, flip coin;
    else { coin = rand.nextDouble() < 0.5; }       //random coin flip

    final FloatBuffer promote = buf.getEvensOrOdds(compactionStart, compactionEnd, coin);

    if (reqDebug != null) {
      reqDebug.emitCompactionDetail(compactionStart, compactionEnd, secsToCompact,
          promote.getLength(), coin);
    }

    buf.trimLength(buf.getLength() - (compactionEnd - compactionStart));
    numCompactions += 1;
    state += 1;
    ensureEnoughSections();

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
   * @return the current maximum capacity of this compactor.
   */
  int getNomCapacity() {
    final int nCap = 2 * numSections * sectionSize;
    return nCap;
  }

  /**
   * Serialize sectionSizeDbl (8), numSections(4), numCompactions(4), state(4), =
   * lgWeight(1), coin(1), hra(1), plus 1 byte gap.
   * @return required bytes to serialize.
   */
  int getSerializationBytes() {
    return 8 + 3 * 4 + 4 + buf.getSerializationBytes(); //hra stored in sketch
  }

  int getNumCompactions() {
    return numCompactions;
  }

  int getNumSections() {
    return numSections;
  }

  int getSectionSize() {
    return sectionSize;
  }

  double getSectionSizeDbl() {
    return sectionSizeDbl;
  }

  int getState() {
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
    numCompactions += other.numCompactions;
    while (ensureEnoughSections()) {}
    buf.sort();
    final FloatBuffer otherBuf = new FloatBuffer(other.buf);
    otherBuf.sort();
    if (otherBuf.getLength() > buf.getLength()) {
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
    final double szd;
    final int ne;
    if (numCompactions >= 1 << numSections - 1
        && (ne = nearestEven(szd = sectionSizeDbl / SQRT2)) >= MIN_K)
    {
      sectionSizeDbl = szd;
      sectionSize = ne;
      numSections <<= 1;
      buf.ensureCapacity(2 * getNomCapacity());
      if (reqDebug != null) { reqDebug.emitAdjSecSizeNumSec(lgWeight); }
      return true;
    }
    return false;
  }

  /**
   * Computes the size of the non-compacted region, which is the start index of the
   * compacted region
   * @param secsToCompact the number of contiguous sections to compact
   * @return the start index of the compacted region
   */
  private long computeCompactionRange(final int secsToCompact) {
    final int bufLen = buf.getLength();
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
  static final int nearestEven(final double value) {
    return (int) round(value / 2.0) << 1;
  }

  byte[] toByteArray() {
    final int bytes = getSerializationBytes(); //don't need sorted or hra from buffer
    final byte[] arr = new byte[bytes];
    final WritableBuffer wbuf = WritableMemory.wrap(arr).asWritableBuffer();
    wbuf.putDouble(sectionSizeDbl);
    wbuf.putInt(numSections);
    wbuf.putInt(numCompactions);//16
    wbuf.putInt(state); //20
    wbuf.putByte(lgWeight);
    wbuf.putBoolean(coin);
    wbuf.putBoolean(hra);
    wbuf.incrementPosition(1); //24
    buf.sort(); //sort if necessary
    wbuf.putByteArray(buf.toByteArray(), 0, buf.getSerializationBytes());
    assert wbuf.getPosition() == bytes;
    return arr;
  }

  /**
   * Returns a printable formatted prefix string summarizing the list.
   * The first number is the compactor height. the second number in brackets is the current length
   * of the compactor buffer. The third number in brackets is the nominal capacity of the compactor.
   * @return a printable formatted prefix string summarizing the list.
   */
  String toListPrefix() {
    final int h = getLgWeight();
    final int len = buf.getLength();
    final int nom = getNomCapacity();
    final int secSz = getSectionSize();
    final int numSec = getNumSections();
    final int num = getNumCompactions();
    final String prefix = String.format(
      "  C:%d Len:%d NomSz:%d SecSz:%d NumSec:%d NumCompactions:%d",
           h, len, nom, secSz, numSec, num);
    return prefix;
  }

}
