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

import static java.lang.Math.min;
import static java.lang.Math.round;
import static org.apache.datasketches.Util.numberOfTrailingOnes;
import static org.apache.datasketches.req.Buffer.LS;
import static org.apache.datasketches.req.RelativeErrorQuantiles.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.req.RelativeErrorQuantiles.MIN_K;
import static org.apache.datasketches.req.RelativeErrorQuantiles.print;
import static org.apache.datasketches.req.RelativeErrorQuantiles.println;

import java.util.Random;

/**
 * @author Lee Rhodes
 */
//@SuppressWarnings({"javadoc","unused"})
public class RelativeCompactor {
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
  private Buffer buf;
  private final int lgWeight;
  private boolean debug;
  private Random rand;

  /**
   * Constructor
   * @param sectionSize the value of k
   * @param lgWeight this compactor's lgWeight
   * @param debug optional
   */
  RelativeCompactor(
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
    buf = new Buffer(nCap, nCap);
    if (debug) { rand = new Random(1); }
    else { rand = new Random(); }

    if (debug) {
      println("    New Compactor: height: " + lgWeight
          + "\tsectionSize: " + sectionSize
          + "\tnumSections: " + numSections + LS);
    }
  }

  /**
   * Copy Constuctor
   * @param other the compactor to be copied into this one
   */
  RelativeCompactor(final RelativeCompactor other) {
    sectionSize = other.sectionSize;
    sectionSizeDbl = other.sectionSizeDbl;
    lgWeight = other.lgWeight;
    debug = other.debug;
    numCompactions = other.numCompactions;
    state = other.state;
    coin = other.coin;
    numSections = other.numSections;
    buf = new Buffer(other.buf);
    if (debug) { rand = new Random(1); }
    else { rand = new Random(); }
  }

  /**
   * Append one item to this compactor
   * @param item the given item
   * @return this;
   */
  RelativeCompactor append(final float item) {
    buf.append(item);
    return this;
  }

  /**
   * Perform a compaction operation on this compactor
   * @return the array of items to be promoted to the next level compactor
   */
  float[] compact() {
    if (debug) {
      println("  Compacting[" + lgWeight + "] nomCapacity: " + getNomCapacity()
        + "\tsectionSize: " + sectionSize
        + "\tnumSections: " + numSections
        + "\tstate(bin): " + Integer.toBinaryString(state));
    }

    if (!buf.isSorted()) {
      buf.sort(); //Footnote 1
    }

    if (debug) { print("    "); print(toHorizontalList(0)); }

    //choose a part of the buffer to compact
    final int compactionOffset; //a.k.a.  "s" see footnote 2
    final int secsToCompact = min(numberOfTrailingOnes(state) + 1, numSections - 1);
    compactionOffset = buf.getItemCount() - (secsToCompact * sectionSize);

    adjustSectSizeNumSect(); //see Footnotes 3, 4 and 8

    assert compactionOffset <= (buf.getItemCount() - 2); //Footnote 5; This is easier to read!

    if ((numCompactions % 2) == 1) { coin = !coin; } //invert coin; Footnote 6
    else { coin = (rand.nextDouble() < 0.5); }       //random coin flip

    final float[] promote = (coin)
        ? buf.getOdds(compactionOffset, buf.getItemCount())
        : buf.getEvens(compactionOffset, buf.getItemCount());

    if (debug) { //Footnote 7
      println("    s: " + compactionOffset
          + "\tsecsToComp: " + secsToCompact
          + "\tsectionSize: " + sectionSize
          + "\tnumSections: " + numSections);
      final int delete = buf.getItemCount() - compactionOffset;
      final int promoteLen = promote.length;
      final int offset = (coin) ? 1 : 0;
      println("    Promote: " + promoteLen + "\tDelete: " + delete + "\tOffset: " + offset);
    }

    buf.trimLength(compactionOffset);
    numCompactions += 1;
    state += 1;

    if (debug) {
      println("    DONE: nomCapacity: " + getNomCapacity()
        + "\tnumCompactions: " + numCompactions);
    }
    return promote;
  } //End Compact

  /**
   * Sets the current nominal capacity of this compactor.
   * @return the current maximum capacity of this compactor.
   */
  int getNomCapacity() {
    final int nCap = 2 * numSections * sectionSize;
    return nCap;
  }

  /**
   * Extends the given item array starting at length() by merging the items into
   * the already sorted array.
   * This will expand the Buffer if necessary.
   * @param items the given item array, which also must be sorted
   * @return this
   */
  RelativeCompactor extendAndMerge(final float[] items) {
    buf.mergeSortIn(items);
    return this;
  }

  /**
   * Extends this compactor with items
   * @param items the given items
   * @return this.
   */
  RelativeCompactor extend(final float[] items) {
    buf.extend(items);
    return this;
  }

  /**
   * Gets a reference to this compactor's internal Buffer
   * @return a reference to this compactor's internal Buffer
   */
  Buffer getBuffer() { return buf; }

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
  RelativeCompactor merge(final RelativeCompactor other, final boolean mergeSort) {
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
  RelativeCompactor sort() {
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
