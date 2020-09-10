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

import static org.apache.datasketches.Util.numberOfTrailingOnes;
import static org.apache.datasketches.req.ReqHelper.nearestEven;
import static org.apache.datasketches.req.ReqSketch.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.req.ReqSketch.MIN_K;

import java.util.Random;


/**
 * The compactor class for the ReqSketch
 * @author Lee Rhodes
 */
class ReqCompactor {
  private static final double SQRT2 = Math.sqrt(2.0);
  private double sectionSizeDbl;
  private int sectionSize; //initialized with k, minimum 4
  private int numSections; //# of sections, initial size 3
  private int numCompactions; //number of compaction operations performed
  private int state; //State of the deterministic compaction schedule
  private final int lgWeight;
  private boolean coin; //true or false at random for each compaction
  private boolean hra; //high rank accuracy
  private FloatBuffer buf;
  private Random rand;
  private ReqDebug reqDebug = null;

  /**
   * Constructor
   * @param sectionSize the value of k
   * @param lgWeight this compactor's lgWeight
   * @param reqDebug the debug signaling interface.
   */
  ReqCompactor(
      final int sectionSize,
      final int lgWeight,
      final boolean hra,
      final ReqDebug reqDebug) {
    this.sectionSize = sectionSize;
    sectionSizeDbl = sectionSize;
    this.lgWeight = lgWeight;
    this.hra = hra;
    this.reqDebug = reqDebug;

    numCompactions = 0;
    state = 0;
    coin = false;
    numSections = INIT_NUMBER_OF_SECTIONS;
    final int nomCap = 2 * numSections * sectionSize; //nCap is always even
    buf = new FloatBuffer(2 * nomCap, 0, hra);

    if (reqDebug != null) { rand = new Random(1); }
    else { rand = new Random(); }

    //if (reqDebug != null) { reqDebug.emitNewCompactor(lgWeight); }
  }

  /**
   * Copy Constuctor
   * @param other the compactor to be copied into this one
   */
  ReqCompactor(final ReqCompactor other) {
    sectionSize = other.sectionSize;
    lgWeight = other.lgWeight;
    hra = other.hra;
    reqDebug = other.reqDebug;

    numCompactions = other.numCompactions;
    state = other.state;
    coin = other.coin;
    numSections = other.numSections;
    sectionSizeDbl = other.sectionSizeDbl;

    buf = new FloatBuffer(other.buf);

  }

  /**
   * Perform a compaction operation on this compactor
   * @return the array of items to be promoted to the next level compactor
   */
  FloatBuffer compact() {
    if (reqDebug != null) {
      reqDebug.emitCompactingStart(lgWeight); }
    buf.sort();
    //TODO if we are at minK no need to compute these ??
    // choose a part of the buffer to compact
    final int secsToCompact = numberOfTrailingOnes(state) + 1;
    final long compactionRange = computeCompactionRange(secsToCompact);
    final int compactionStart = (int) (compactionRange & 0xFFFF_FFFFL); //low 32
    final int compactionEnd = (int) (compactionRange >>> 32); //high 32
    assert (compactionEnd - compactionStart) >= 2;

    if ((numCompactions & 1) == 1) { coin = !coin; } //if numCompactions odd, flip coin;
    else { coin = (rand.nextDouble() < 0.5); }       //random coin flip

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
  int getLgWeight() {
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

  int getNumCompactions() {
    return numCompactions;
  }

  int getNumSections() {
    return numSections;
  }

  int getSectionSize() {
    return sectionSize;
  }

  int getState() {
    return state;
  }

  /**
   * Merge the other given compactor into this one
   * @param other the other given compactor
   * @return this
   */
  ReqCompactor merge(final ReqCompactor other) {
    state |= other.state;
    numCompactions += other.numCompactions;
    ensureEnoughSections();
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
    if ((numSections >= (1 << (numSections - 1)))
        && ((ne = nearestEven(szd = sectionSizeDbl / SQRT2)) >= MIN_K))
    {
      sectionSizeDbl = szd;
      sectionSize = ne;
      numSections <<= 1;
      buf.ensureCapacity(4 * numSections * sectionSize);
      if (reqDebug != null) { reqDebug.emitAdjSecSizeNumSec(lgWeight); }
      return true;
    }
    return false;
  }

  /**
   * Computes the size of the non-compated region, which is the start index of the
   * compacted region
   * @param secsToCompact the number of contiguous sections to compact
   * @return the start index of the compacted region
   */
  private long computeCompactionRange(final int secsToCompact) {
    final int bufLen = buf.getLength();
    int nonCompact = (getNomCapacity() / 2) + ((numSections - secsToCompact) * sectionSize);
    //make compacted region even:
    nonCompact = (((bufLen - nonCompact) & 1) == 1) ? nonCompact + 1 : nonCompact;
    final long low =  (hra) ? 0                   : nonCompact;
    final long high = (hra) ? bufLen - nonCompact : bufLen;
    return (high << 32) + low;
  }

  //debug print functions

  /**
   * Returns a printable formatted prefix string summarizing the list.
   * The first number is the compactor height. the second number in brackets is the current length
   * of the compactor buffer. The third number in brackets is the nominal capacity of the compactor.
   * @return a printable formatted prefix string summarizing the list.
   */
  String toListPrefix() {
    final int h = getLgWeight();
    final int len = buf.getLength();
    final int nomCap = getNomCapacity();
    final String prefix = String.format("%2d [%3d] [%3d]: ", h, len, nomCap);
    return prefix;
  }

}
