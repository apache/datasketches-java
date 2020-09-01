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
import static org.apache.datasketches.req.FloatBuffer.LS;
import static org.apache.datasketches.req.FloatBuffer.TAB;
import static org.apache.datasketches.req.ReqSketch.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.req.ReqSketch.MIN_K;
import static org.apache.datasketches.req.ReqSketch.print;
import static org.apache.datasketches.req.ReqSketch.println;

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
      final boolean debug) {
    this.sectionSize = sectionSize;
    sectionSizeDbl = sectionSize;
    this.lgWeight = lgWeight;
    this.debug = debug;

    numCompactions = 0;
    state = 0;
    coin = false;
    numSections = INIT_NUMBER_OF_SECTIONS;
    final int nomCap = 2 * numSections * sectionSize; //nCap is always even
    buf = new FloatBuffer(nomCap, nomCap / 2);

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
    final int count = buf.getItemCount();
    if (debug) { printCompactingStart(); }
    buf.sort();
    if (debug) { print("    "); print(toHorizontalList("%4.0f", 24, 14)); } //#decimals

    //choose a part of the buffer to compact
    final int secsToCompact = numberOfTrailingOnes(state) + 1;
    final int compactionStart = computeCompactionStart(secsToCompact); //a.k.a.  "s"
    assert compactionStart <= (count - 2);

    if ((numCompactions & 1) == 1) { coin = !coin; } //if numCompactions odd, flip coin;
    else { coin = (rand.nextDouble() < 0.5); }       //random coin flip

    final FloatBuffer promote = (coin)
        ? buf.getOdds(compactionStart, count)
        : buf.getEvens(compactionStart, count);

    if (debug) { printCompactionDetail(compactionStart, secsToCompact, promote.getLength()); }

    buf.trimLength(compactionStart);
    numCompactions += 1;
    state += 1;

    if (numCompactions >= (1 << (numSections - 1))) {
      adjustSectSizeNumSect();
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
    final int nearestEven = ReqHelper.nearestEven(newSectSizeDbl);
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
  private int computeCompactionStart(final int secsToCompact) {
    int s = (getNomCapacity() / 2) + ((numSections - secsToCompact) * sectionSize);
    return (((buf.getLength() - s) & 1) == 1) ? ++s : s;
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
    final int re = buf.getLength();
    final int h = getLgWeight();
    final String prefix = String.format("%2d [%3d]: ", h, re);
    final String str = prefix + buf.toHorizList(fmt, width, indent) + LS;
    return str;
  }

  private void printNewCompactor() {
    println("    New Compactor: height: " + lgWeight
        + TAB + "sectionSize: " + sectionSize
        + TAB + "numSections: " + numSections + LS);
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
    sb.append("  ");
    sb.append("Compacting[").append(lgWeight).append("] ");
    sb.append("NomCapacity: ").append(getNomCapacity());
    sb.append(TAB + "SectionSize: ").append(sectionSize);
    sb.append(TAB + "NumSections: ").append(numSections);
    sb.append(TAB + "State(bin): ").append(Integer.toBinaryString(state));
    println(sb.toString());
  }

  private void printCompactionDetail(final int compactionStart, final int secsToCompact,
      final int promoteLen) {
    final StringBuilder sb = new StringBuilder();
    final int count = buf.getItemCount();
    sb.append("    ");
    sb.append("SecsToCompact: ").append(secsToCompact);
    sb.append(TAB + "NoCompact: ").append(compactionStart);
    sb.append(TAB + "DoCompact: ").append(count - compactionStart).append(LS);
    final int delete = count - compactionStart;
    final String oddOrEven = (coin) ? "Odds" : "Evens";
    sb.append("    ");
    sb.append("Promote: ").append(promoteLen);
    sb.append(TAB + "Delete: ").append(delete);
    sb.append(TAB + "Choose: ").append(oddOrEven);
    println(sb.toString());
  }

  private void printCompactionDone() {
    println("    DONE: NumCompactions: " + numCompactions);
  }

}
