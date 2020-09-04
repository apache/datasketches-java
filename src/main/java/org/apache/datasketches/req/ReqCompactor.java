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
import static org.apache.datasketches.req.ReqHelper.LS;
import static org.apache.datasketches.req.ReqHelper.TAB;
import static org.apache.datasketches.req.ReqHelper.nearestEven;
import static org.apache.datasketches.req.ReqHelper.println;
import static org.apache.datasketches.req.ReqSketch.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.req.ReqSketch.MIN_K;

import java.util.Random;


/**
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
  private final boolean debug;
  private FloatBuffer buf;
  private Random rand;

  /**
   * Constructor
   * @param sectionSize the value of k
   * @param lgWeight this compactor's lgWeight
   * @param debug true for debug info
   */
  ReqCompactor(
      final int sectionSize,
      final int lgWeight,
      final boolean hra,
      final boolean debug) {
    this.sectionSize = sectionSize;
    sectionSizeDbl = sectionSize;
    this.lgWeight = lgWeight;
    this.hra = hra;
    this.debug = debug;

    numCompactions = 0;
    state = 0;
    coin = false;
    numSections = INIT_NUMBER_OF_SECTIONS;
    final int nomCap = 2 * numSections * sectionSize; //nCap is always even
    buf = new FloatBuffer(2 * nomCap, 0, hra);

    if (debug) { rand = new Random(1); }
    else { rand = new Random(); }

    if (debug) { printNewCompactor(); }
  }

  /**
   * Copy Constuctor
   * @param other the compactor to be copied into this one
   */
  ReqCompactor(final ReqCompactor other) {
    sectionSizeDbl = other.sectionSizeDbl;
    sectionSize = other.sectionSize;
    numSections = other.numSections;
    numCompactions = other.numCompactions;
    state = other.state;
    lgWeight = other.lgWeight;
    coin = other.coin;
    debug = other.debug;
    buf = new FloatBuffer(other.buf);
    if (debug) { rand = new Random(1); }
    else { rand = new Random(); }
  }

  /**
   * Perform a compaction operation on this compactor
   * @return the array of items to be promoted to the next level compactor
   */
  FloatBuffer compact() {
    if (debug) { printCompactingStart(); }
    buf.sort();
    //if (debug) { print("    "); print(toHorizontalList("%4.0f", 24, 14)); } //#decimals

    //choose a part of the buffer to compact
    final int secsToCompact = numberOfTrailingOnes(state) + 1;
    final long compactionRange = computeCompactionRange(secsToCompact);
    final int compactionStart = (int) (compactionRange & 0xFFFF_FFFFL); //low 32
    final int compactionEnd = (int) (compactionRange >>> 32); //high 32
    assert (compactionEnd - compactionStart) >= 2;

    if ((numCompactions & 1) == 1) { coin = !coin; } //if numCompactions odd, flip coin;
    else { coin = (rand.nextDouble() < 0.5); }       //random coin flip

    final FloatBuffer promote = buf.getEvensOrOdds(compactionStart, compactionEnd, coin);

    if (debug) { printCompactionDetail(compactionStart, compactionEnd, secsToCompact,
        promote.getLength()); }

    buf.trimLength(buf.getLength() - (compactionEnd - compactionStart));
    numCompactions += 1;
    state += 1;

    if (numCompactions >= (1 << (numSections - 1))) {
      adjustSectSizeNumSect();
      buf.ensureCapacity(4 * numSections * sectionSize);
      if (debug) { printAdjSecSizeNumSec(); }
    }

    if (debug) { printCompactionDone(); }

    return promote;
  } //End Compact

  /**
   * Gets a reference to this compactor's internal FloatBuffer
   * @return a reference to this compactor's internal FloatBuffer
   */
  FloatBuffer getBuffer() { return buf; }

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

  /**
   * Merge the other given compactor into this one
   * @param other the other given compactor
   * @return this
   */
  ReqCompactor merge(final ReqCompactor other) {
    state |= other.state;
    numCompactions += other.numCompactions;
    buf.sort();
    other.buf.sort();
    buf.mergeSortIn(other.getBuffer());
    return this;
  }

  /**
   * This adjusts sectionSize and numSections and guarantees that the sectionSize
   * will always be even and >= minK.
   */
  private void adjustSectSizeNumSect() {
    final double newSectSizeDbl = sectionSizeDbl / SQRT2;
    final int nearestEven = nearestEven(newSectSizeDbl);
    if (nearestEven < MIN_K) { return; }
    sectionSizeDbl = newSectSizeDbl;
    sectionSize = nearestEven;
    numSections <<= 1;
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
   * Returns a printable formatted string of the values of this buffer separated by a single space.
   * This string is prepended by the lgWeight and retained entries of this compactor.
   * @param fmt The format for each printed item.
   * @param width the number of items to print per line
   * @param indent the number of spaces at the beginning of a new line
   * @return a printable, formatted string of the values of this buffer.
   */
  String toHorizontalList(final String fmt, final int width, final int indent) {
    final int h = getLgWeight();
    final int len = buf.getLength();
    final int nomCap = getNomCapacity();

    final String prefix = String.format("%2d [%3d] [%3d]: ", h, len, nomCap);
    final String str = prefix + buf.toHorizList(fmt, width, indent) + LS;
    return str;
  }

  private void printNewCompactor() {
    println("    New Compactor: height: " + lgWeight
        + TAB + "sectionSize: " + sectionSize
        + TAB + "numSections: " + numSections);
  }

  private void printAdjSecSizeNumSec() {
    final StringBuilder sb = new StringBuilder();
    sb.append("    ");
    sb.append("Adjust: SectionSize: ").append(sectionSize);
    sb.append(" NumSections: ").append(numSections);
    println(sb.toString());
  }

  private void printCompactingStart() {
    final StringBuilder sb = new StringBuilder();
    sb.append(LS + "  ");
    sb.append("COMPACTING[").append(lgWeight).append("] ");
    sb.append("NomCapacity: ").append(getNomCapacity());
    sb.append(TAB + " SectionSize: ").append(sectionSize);
    sb.append(TAB + " NumSections: ").append(numSections);
    sb.append(TAB + " State(bin): ").append(Integer.toBinaryString(state));
    sb.append(TAB + " BufCapacity: ").append(buf.getCapacity());
    println(sb.toString());
  }

  private void printCompactionDetail(final int compactionStart, final int compactionEnd,
      final int secsToCompact,
      final int promoteLen) {
    final StringBuilder sb = new StringBuilder();
    sb.append("    ");
    sb.append("SecsToCompact: ").append(secsToCompact);
    sb.append(TAB + " CompactStart: ").append(compactionStart);
    sb.append(TAB + " CompactEnd: ").append(compactionEnd).append(LS);
    final int delete = compactionEnd - compactionStart;
    final String oddOrEven = (coin) ? "Odds" : "Evens";
    sb.append("    ");
    sb.append("Promote: ").append(promoteLen);
    sb.append(TAB + " Delete: ").append(delete);
    sb.append(TAB + " Choose: ").append(oddOrEven);
    println(sb.toString());
  }

  private void printCompactionDone() {
    println("  COMPACTING DONE: NumCompactions: " + numCompactions + LS);
  }

}
