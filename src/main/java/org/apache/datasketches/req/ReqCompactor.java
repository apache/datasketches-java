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
import static org.apache.datasketches.req.FloatBuffer.LS;
import static org.apache.datasketches.req.ReqSketch.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.req.ReqSketch.MIN_K;
import static org.apache.datasketches.req.ReqSketch.print;
import static org.apache.datasketches.req.ReqSketch.println;

import java.util.Random;

/**
 * @author Lee Rhodes
 */
//@SuppressWarnings({"javadoc","unused"})
public class ReqCompactor {
  private static final double SQRT2 = Math.sqrt(2.0);
  private int numCompactions; //number of compaction operations performed

  //State of the deterministic compaction schedule.
  //  If there are no merge operations performed, state == numCompactions
  private int state;
  //if there are no merge operations performed, state == numCompactions

  private boolean coin; //true or false uniformly at random for each compaction
  private int sectionSize; //initialized with k
  private double sectionSizeDbl;
  private int numSections; //# of sections, minimum 3
  private FloatBuffer buf;
  private final int lgWeight;
  private boolean debug;
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
    final int nCap = 2 * numSections * sectionSize; //nCap is always even
    buf = new FloatBuffer(nCap, nCap);
    if (debug) { rand = new Random(1); }
    else { rand = new Random(); }

    if (debug) { printNewCompactor(); }
  }

  private void printNewCompactor() {
    println("    New Compactor: height: " + lgWeight
        + "\tsectionSize: " + sectionSize
        + "\tnumSections: " + numSections + LS);
  }

  /**
   * Copy Constuctor
   * @param other the compactor to be copied into this one
   */
  ReqCompactor(final ReqCompactor other) {
    sectionSize = other.sectionSize;
    sectionSizeDbl = other.sectionSizeDbl;
    lgWeight = other.lgWeight;
    debug = other.debug;
    numCompactions = other.numCompactions;
    state = other.state;
    coin = other.coin;
    numSections = other.numSections;
    buf = new FloatBuffer(other.buf);
    if (debug) { rand = new Random(1); }
    else { rand = new Random(); }
  }

  /**
   * Append one item to this compactor
   * @param item the given item
   * @return this;
   */
  ReqCompactor append(final float item) {
    buf.append(item);
    return this;
  }

  /**
   * Perform a compaction operation on this compactor
   * @return the array of items to be promoted to the next level compactor
   */
  float[] compact() {
    final int count = buf.getItemCount();
    if (debug) { printCompactingStart(); }
    if (!buf.isSorted()) { buf.sort(); } //Footnote 1
    if (debug) { print("    "); print(toHorizontalList(0)); } //#decimals

    //choose a part of the buffer to compact
    final int secsToCompact = numberOfTrailingOnes(state) + 1;
    final int compactionStart = computeCompactionStart(secsToCompact); //a.k.a.  "s" see footnote 2
    assert compactionStart <= (count - 2); //Footnote 5

    if ((numCompactions & 1) == 1) { coin = !coin; } //if odd, flip coin; Footnote 6
    else { coin = (rand.nextDouble() < 0.5); }       //random coin flip

    final float[] promote = (coin)
        ? buf.getOdds(compactionStart, count)
        : buf.getEvens(compactionStart, count);

    if (debug) { printCompactionDetail(compactionStart, secsToCompact, promote.length); }

    buf.trimLength(compactionStart);
    numCompactions += 1;
    state += 1;

    if (numCompactions >= (1 << (numSections - 1))) {
      adjustSectSizeNumSect(); //see Footnotes 3, 4 and 8
      printAdjSecSizeNumSec();
    }

    if (debug) { printCompactionDone(); }

    return promote;
  } //End Compact

  private int computeCompactionStart(final int secsToCompact) {
    int s = (getNomCapacity() / 2) + ((numSections - secsToCompact) * sectionSize);
    return (((buf.getItemCount() - s) & 1) == 1) ? ++s : s;
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
    sb.append("\tSectionSize: ").append(sectionSize);
    sb.append("\tNumSections: ").append(numSections);
    sb.append("\tState(bin): ").append(Integer.toBinaryString(state));
    println(sb.toString());
  }

  private void printCompactionDetail(final int compactionStart, final int secsToCompact,
      final int promoteLen) { //Footnote 7
    final StringBuilder sb = new StringBuilder();
    final int count = buf.getItemCount();
    sb.append("    ");
    sb.append("SecsToCompact: ").append(secsToCompact);
    sb.append("\tNoCompact: ").append(compactionStart);
    sb.append("\tDoCompact: ").append(count - compactionStart).append(LS);
    final int delete = count - compactionStart;
    final String oddOrEven = (coin) ? "Odds" : "Evens";
    sb.append("    ");
    sb.append("Promote: ").append(promoteLen);
    sb.append("\tDelete: ").append(delete);
    sb.append("\tChoose: ").append(oddOrEven);
    println(sb.toString());
  }

  private void printCompactionDone() {
    println("    DONE: NumCompactions: " + numCompactions);
  }

  /**
   * Extends the given item array starting at length() by merging the items into
   * the already sorted array.
   * This will expand the FloatBuffer if necessary.
   * @param items the given item array, which also must be sorted
   * @return this
   */
  ReqCompactor extendAndMerge(final float[] items) {
    buf.mergeSortIn(items);
    return this;
  }

  /**
   * Extends this compactor with items
   * @param items the given items
   * @return this.
   */
  ReqCompactor extend(final float[] items) {
    buf.extend(items);
    return this;
  }

  /**
   * Gets a reference to this compactor's internal FloatBuffer
   * @return a reference to this compactor's internal FloatBuffer
   */
  FloatBuffer getBuffer() { return buf; }

  /**
   * Gets the current capacity of this compactor
   * @return the current capacity of this compactor
   */
  int getCapacity() {
    return buf.getCapacity();
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

  /**
   * Gets the number of retained values in this compactor.
   * @return the number of retained values in this compactor.
   */
  int getNumRetainedEntries() { return buf.getItemCount(); }

  /**
   * Merge the other given compactor into this one
   * @param other the other given compactor
   * @param mergeSort if true, apply mergeSort algorithm instead of sort().
   * @return this
   */
  ReqCompactor merge(final ReqCompactor other, final boolean mergeSort) {
    state |= other.state;
    numCompactions += other.numCompactions;
    if (mergeSort) { //assumes this and other is already sorted
      final float[] arrIn = other.getBuffer().getArray();
      buf.mergeSortIn(arrIn);
    } else {
      buf.extend(other.getBuffer());
      buf.sort();
    }
    return this;
  }

  /**
   * Returns the nearest even integer to the given value.
   * @param value the given value
   * @return the nearest even integer to the given value.
   */
  //also used by test
  static final int nearestEven(final double value) {
    return ((int) round(value / 2.0)) << 1;
  }

  /**
   * Returns a printable formatted string of the values of this buffer separated by a single space.
   * This string is prepended by the lgWeight and retained entries of this compactor.
   * @param decimals The desired precision after the decimal point
   * @return a printable, formatted string of the values of this buffer.
   */
  String toHorizontalList(final int decimals) {
    final int re = getNumRetainedEntries();
    final int h = getLgWeight();
    final String str = h + " [" + re + "]: " + buf.toHorizList(decimals) + LS;
    return str;
  }

  /**
   * Gets the non-normalized rank of the given value.
   * This is equal to the number of values in
   * this compactor that are &lt; the given value.
   * @param value the given value
   * @return the non-normalized rank of the given value
   */
  int rank(final float value) { //one-based integer
    return buf.countLessThan(value);
  }

  /**
   * Sort all values in this compactor.
   * @return this
   */
  ReqCompactor sort() {
    if (!buf.isSorted()) { buf.sort(); }
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

  /* Footnotes:
   * 1. Sort the items in the buffer; use self.sort(reverse=True) for a better accuracy for
   *    higher-ranked items; TODO: test this reversed order.
   *    Remark: it's actually not needed to sort the whole buffer, we just need to ensure that the
   *    compacted part of the buffer is sorted and contains largest items
   *
   * 2. Choose according to the deterministic schedule, i.e., according to the number
   *    of trailing zeros in binary representation of the state, which is the number of
   *    compactions so far, unless there are merge operations.
   *
   * 3. This is, basically, a doubling strategy on log_2(number of compactions).
   *    TODO replace doubling strategy by increments by 1?
   *
   * 4. Decreasing section size so that it equals roughly
   *    initial size / sqrt(log_2 (number of compactions)
   *
   * 5. TODO ensure under merge operations: s >= cap / 2 - 1)
   *    at least half of the buffer should remain unaffected by compaction.
   *
   * 6. Random offset for choosing odd/even items in the compacted part;
   *    Random choice done every other time.
   *
   * 7. Possible debug outputs: compactionOffset, numCompactions, sectionsToCompact, length,
   *    capacity, sectionSize, numSections
   *
   * 8. if (((buf.length() - compactionOffset) % 2) == 1) { //ensure compacted part has an even size
   *      if (compactionOffset > 0) { compactionOffset--; }
   *    } else { compactionOffset++; }
   */
}
