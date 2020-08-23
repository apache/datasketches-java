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

import static java.lang.Math.max;
import static java.lang.Math.round;
import static org.apache.datasketches.Util.numberOfTrailingOnes;
import static org.apache.datasketches.req.RelativeErrorSketch.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.req.RelativeErrorSketch.MIN_K;
import static org.apache.datasketches.req.RelativeErrorSketch.println;

import java.util.Random;

/**
 * @author Lee Rhodes
 */
//@SuppressWarnings({"javadoc","unused"})
public class RelativeCompactor {
  private static final double SQRT2 = Math.sqrt(2.0);
  private int numCompactions; //number of compaction operations performed
  private int state; //state of the deterministic compaction schedule
  //if there are no merge operations performed, state == numCompactions

  private boolean coin; //true or false uniformly at random for each compaction
  private int sectionSize; //k
  private int numSections; //# of sections in the buffer, minimum 3
  Buffer buf;
  int lgWeight = 0;
  private boolean debug;
  Random rand = new Random();


  /**
   * Constructor
   * @param sectionSize the value of k
   * @param lgWeight this compactor's lgWeight
   * @param debug optional
   */
  public RelativeCompactor(final int sectionSize, final int lgWeight, final boolean debug) {
    this.sectionSize = sectionSize;
    this.lgWeight = lgWeight;
    this.debug = debug;

    numCompactions = 0;
    state = 0;
    coin = false;
    numSections = INIT_NUMBER_OF_SECTIONS;
    final int cap = 2 * numSections * sectionSize; //cap is always even
    buf = new Buffer(cap, cap / 4);
  }

  /**
   * Copy Constuctor
   * @param other the compactor to be copied into this one
   */
  public RelativeCompactor(final RelativeCompactor other) {
    sectionSize = other.sectionSize;
    lgWeight = other.lgWeight;
    debug = other.debug;
    numCompactions = other.numCompactions;
    state = other.state;
    coin = other.coin;
    numSections = other.numSections;
    buf = new Buffer(other.buf);
  }

  /**
   * Append one item to this compactor
   * @param item the given item
   * @return this;
   */
  public RelativeCompactor append(final float item) {
    buf.append(item);
    return this;
  }

  /**
   * Perform a compaction operation on this compactor
   * @return the array of items to be promoted to the next level compactor
   */
  public float[] compact() {
    if (debug) { println("Compactor " + lgWeight + " Compacting ..."); }
    final int cap = capacity(); //ensures and gets
    if (!buf.isSorted()) {
      buf.sort(); //Footnote 1
    }
    //choose a part of the buffer to compac
    final int compactionOffset;
    if (sectionSize < MIN_K) {  //COMMENT: can be avoided by making sure sectionSize >= MIN_K
      //too small sections => compact half of the buffer always
      compactionOffset = cap / 2;  //COMMENT:  Not sure this makes sense and may be unneccesary
    }
    else { //Footnote 2
      final int secsToCompact = numberOfTrailingOnes(state) + 1;
      compactionOffset = (cap / 2) + ((numSections - secsToCompact) * sectionSize);

      if (numCompactions >= (1 << (numSections - 1))) { //make numSections larger
        numSections *= 2; //Footnote 3
        sectionSize = max(nearestEven(sectionSize / SQRT2), MIN_K); //Footnote 4
      }
    }

    //COMMENT: we can avoid this if we can guarantee that buf.length, compactionSize are even
    //if (((buf.length() - compactionOffset) % 2) == 1) { //ensure compacted part has an even size
    //  if (compactionOffset > 0) { compactionOffset--; }
    //} else { compactionOffset++; }
    assert compactionOffset <= (buf.length() - 2); //Footnote 5; This is easier to read!

    if ((numCompactions % 2) == 1) { coin = !coin; } //invert coin; Footnote 6
    else { coin = (rand.nextDouble() < 0.5); } //random coin flip

    final float[] promote = (coin)
        ? buf.getEvens(compactionOffset, buf.length())
        : buf.getOdds(compactionOffset, buf.length());

    //if (debug) { println("RelativeCompactor: Compacting..."); } //Footnote 7

    buf.trimLength(compactionOffset);
    numCompactions += 1;
    state += 1;

    if (debug) { println("Compactor: Done\n  Buf Length   :\t" + buf.length()); }
    return promote;
  } //End Compact

  /**
   * Sets the current maximum capacity of this compactor.
   * @return the current maximum capacity of this compactor.
   */
  public int capacity() {
    buf.ensureCapacity(2 * numSections * sectionSize);
    return buf.capacity();
  }

  /**
   * Extends this compactor with items
   * @param items the given items
   * @return this.
   */
  public RelativeCompactor extend(final float[] items) {
    buf.extend(items);
    return this;
  }

  /**
   * Gets a reference to this compactor's internal Buffer
   * @return a reference to this compactor's internal Buffer
   */
  Buffer getBuf() { return buf; }

  /**
   * Gets the current capacity of this compactor
   * @return the current capacity of this compactor
   */
  public int getCapacity() {
    return buf.capacity();
  }

  /**
   * Gets the lgWeight of this buffer
   * @return the lgWeight of this buffer
   */
  public int getLgWeight() {
    return lgWeight;
  }

  /**
   * Gets the length (number of retained values) in this compactor.
   * @return the length of this compactor
   */
  public int length() { return buf.length(); }

  /**
   * Merge the other given compactor into this one
   * @param other the other given compactor
   * @return this
   */
  public RelativeCompactor mergeIntoSelf(final RelativeCompactor other) {
    state |= other.state;
    numCompactions += other.numCompactions;
    buf.extend(other.getBuf());
    buf.sort(); //TODO this wasn't in Pavel's code
    return this;
  }

  /**
   * Sort only the values in this compactor that are not already sorted.
   * @return this
   */
  public RelativeCompactor optimizedSort() { //TODO not done
    return this;
  }

  /**
   * Gets the non-normalized rank of the given value.  This is equal to the number of values in
   * this compactor that are &lt; the given value.
   * @param value the given value
   * @return the non-normalized rank of the given value
   */
  public int rank(final float value) { //one-based integer
    return buf.countLessThan(value);
  }

  /**
   * Sort all values in this compactor.
   * @return this
   */
  public RelativeCompactor sort() {
    if (!buf.isSorted()) { buf.sort(); }
    return this;
  }

  @Override
  public String toString() {
    return null;
  }

  private static final int nearestEven(final double value) {
    return ((int) round(value / 2.0)) << 1;
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
   */
}
