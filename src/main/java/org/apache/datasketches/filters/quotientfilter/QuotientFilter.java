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

package org.apache.datasketches.filters.quotientfilter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesException;
import org.apache.datasketches.filters.common.BitArray;
import org.apache.datasketches.filters.common.HeapBitArray;

public class QuotientFilter extends Filter {

  public static final float DEFAULT_LOAD_FACTOR = 0.8f;

  int lgQ_;
  int numFingerprintBits_;
  float loadFactor_;
  int numEntries_;
  int numExpansions_;
  BitArray bitArray_;

  // statistics, computed in the compute_statistics method. method should be called before these are used
  long numRuns_;
  long numClusters_;
  public double avgRunLength_;
  public double avgClusterLength_;

  public QuotientFilter(final int lgQ, final int numFingerprintBits) {
    this(lgQ, numFingerprintBits, DEFAULT_LOAD_FACTOR);
  }

  public QuotientFilter(final int lgQ, final int numFingerprintBits, final float loadFactor) {
    lgQ_ = lgQ;
    numFingerprintBits_ = numFingerprintBits;
    loadFactor_ = loadFactor;
    bitArray_ = makeFilter(getNumSlots(), getNumBitsPerEntry());
    numExpansions_ = 0;
    //hash_type = XxHash.hashLong ; //HashType.xxh;
  }

  public boolean rejuvenate(final long key) {
    return false;
  }

  public long getNumEntries() {
    return numEntries_;
  }

  public int getNumExpansions() {
    return numExpansions_;
  }

  public long getMaxEntriesBeforeExpansion() {
    return (long)(getNumSlots() * loadFactor_);
  }

  BitArray makeFilter(final long initSize, final int bitsPerEntry) {
    return new HeapBitArray(initSize * bitsPerEntry);
  }

  public int getFingerprintLength() {
    return numFingerprintBits_;
  }

  void expand() {
    if (getFingerprintLength() < 2) throw new SketchesException("for expansion value must have at least 2 bits");
    final QuotientFilter other = new QuotientFilter(lgQ_ + 1, numFingerprintBits_ - 1, loadFactor_);

    long i = 0;
    if (!isSlotEmpty(i)) { i = findClusterStart(i); }

    final Queue<Long> fifo = new LinkedList<Long>();
    long count = 0;
    while (count < numEntries_) {
      if (!isSlotEmpty(i)) {
        if (isOccupied(i)) { fifo.add(i); }
        final long fingerprint = getFingerprint(i);
        final long newQuotient = (fifo.element() << 1) | (fingerprint >> other.getFingerprintLength());
        final long newFingerprint = fingerprint & other.getFingerprintMask();
        other.insert(newFingerprint, newQuotient);
        count++;
      }
      i = (i + 1) & getSlotMask();
      if (!fifo.isEmpty() && ! isContinuation(i)) { fifo.remove(); }
    }
    lgQ_++;
    numFingerprintBits_--;
    bitArray_ = other.bitArray_;
    numExpansions_++;
  }

  // measures the number of bits per entry for the filter
  public double measureNumBitsPerEntry() {
    return measureNumBitsPerEntry(this, new ArrayList<QuotientFilter>());
  }

  // measures the number of bits per entry for the filter
  // it takes an array of filters as a parameter since some filter implementations here consist of multiple filter objects
  protected static double measureNumBitsPerEntry(final QuotientFilter current, final ArrayList<QuotientFilter> otherFilters) {
    //System.out.println("--------------------------");
    //current.print_filter_summary();
    //System.out.println();
    double numEntries = current.getNumEntries();
    for (QuotientFilter q : otherFilters) {
      //q.print_filter_summary();
      //System.out.println();
      numEntries += q.getNumEntries();
    }
    long numBits = current.getNumBitsPerEntry() * current.getNumSlots();
    for (final QuotientFilter q : otherFilters) {
      numBits += q.getNumBitsPerEntry() * q.getNumSlots();
    }
    //System.out.println("total entries: \t\t" + num_entries);
    //System.out.println("total bits: \t\t" + num_bits);
    final double bits_per_entry = numBits / numEntries;
    //System.out.println("total bits/entry: \t" + bits_per_entry);
    //System.out.println();
    return bits_per_entry;
  }

  // returns the fraction of occupied slots in the filter
  public double getUtilization() {
    return numEntries_ / (double) getNumSlots();
  }

  public int getLgQ() {
    return lgQ_;
  }

  public float getLoadFactor() {
    return loadFactor_;
  }

  // returns the number of slots in the filter without the extension/buffer slots
  public long getNumSlots() {
    return 1L << lgQ_;
  }

  long getSlotMask() {
    return getNumSlots() - 1;
  }

  long getFingerprintMask() {
    return (1L << getFingerprintLength()) - 1;
  }

  // sets the metadata flag bits for a given slot index
  void modifySlot(final boolean isOccupied, final boolean isContinuation, final boolean isShifted, final long index) {
    setOccupied(index, isOccupied);
    setContinuation(index, isContinuation);
    setShifted(index, isShifted);
  }

  // sets the fingerprint for a given slot index
  void setFingerprint(final long index, final long fingerprint) {
    bitArray_.setBits(index * getNumBitsPerEntry() + 3, getFingerprintLength(), fingerprint);
  }

  // print a nice representation of the filter that can be understood.
  // if vertical is on, each line will represent a slot
  public String getPrettyStr(final boolean vertical) {
    final StringBuffer sbr = new StringBuffer();
    final long numBits = getNumSlots() * getNumBitsPerEntry();
    for (long i = 0; i < numBits; i++) {
      final long remainder = i % getNumBitsPerEntry();
      if (remainder == 0) {
        final long slot = i / getNumBitsPerEntry();
        sbr.append(" ");
        if (vertical) {
          sbr.append("\n" + String.format("%-10d", slot) + "\t");
        }
      }
      if (remainder == 3) {
        sbr.append(" ");
      }
      sbr.append(bitArray_.getBit(i) ? "1" : "0");
    }
    sbr.append("\n");
    return sbr.toString();
  }

  // print a representation of the filter that can be humanly read.
  public void prettyPrint() {
    System.out.print(getPrettyStr(true));
  }

  // return a fingerprint in a given slot index
  long getFingerprint(final long index) {
    return bitArray_.getBits(index * getNumBitsPerEntry() + 3, getFingerprintLength());
  }

  // return an entire slot representation, including metadata flags and fingerprint
  long getSlot(final long index) {
    return bitArray_.getBits(index * getNumBitsPerEntry(), getNumBitsPerEntry());
  }

  // compare a fingerprint input to the fingerprint in some slot index
  protected boolean compare(final long index, final long fingerprint) {
    return getFingerprint(index) == fingerprint;
  }

  // modify the flags and fingerprint of a given slot
  void modifySlot(final boolean isOccupied, final boolean isContinuation, final boolean isShifted,
                  final long index, final long fingerprint) {
    modifySlot(isOccupied, isContinuation, isShifted, index);
    setFingerprint(index, fingerprint);
  }

  // summarize some statistical measures about the filter
  public void printFilterSummary() {
    final long slots = getNumSlots();
    final long numBits = slots * getNumBitsPerEntry();
    System.out.println("lgQ:         " + lgQ_);
    System.out.println("FP length:   " + getFingerprintLength());
    System.out.println("load factor: " + getLoadFactor());
    System.out.println("bits:        " + numBits);
    System.out.println("bits/entry:  " + numBits / (double)numEntries_);
    System.out.println("entries:     " + numEntries_);
    System.out.println("expansions:  " + numExpansions_);
    System.out.println("load:        " + numEntries_ / (double)(slots));
    computeStatistics();
    //System.out.println("num runs: \t\t" + num_runs);
    //System.out.println("avg run length: \t" + avg_run_length);
    //System.out.println("num clusters: \t\t" + num_clusters);
    //System.out.println("avg cluster length: \t" + avg_cluster_length);
  }

  /*
   * Returns the number of bits used for the filter
   */
  @Override
  public long getSpaceUse() {
    return getNumSlots() * getNumBitsPerEntry();
  }

  public int getNumBitsPerEntry() {
    return numFingerprintBits_ + 3;
  }

  boolean isOccupied(final long index) {
    return bitArray_.getBit(index * getNumBitsPerEntry());
  }

  boolean isContinuation(final long index) {
    return bitArray_.getBit(index * getNumBitsPerEntry() + 1);
  }

  boolean isShifted(final long index) {
    return bitArray_.getBit(index * getNumBitsPerEntry() + 2);
  }

  void setOccupied(final long index, final boolean val) {
    bitArray_.assignBit(index * getNumBitsPerEntry(), val);
  }

  void setContinuation(final long index, final boolean val) {
    bitArray_.assignBit(index * getNumBitsPerEntry() + 1, val);
  }

  void setShifted(final long index, final boolean val) {
    bitArray_.assignBit(index * getNumBitsPerEntry() + 2, val);
  }

  boolean isSlotEmpty(final long index) {
    return !isOccupied(index) && !isContinuation(index) && !isShifted(index);
  }

  // scan the cluster leftwards until finding the start of the cluster and returning its slot index
  // used by deletes
  long findClusterStart(long index) {
    while (isShifted(index)) {
      index = (index - 1) & getSlotMask();
    }
    return index;
  }

  // given a canonical slot A, finds the actual index B of where the run belonging to slot A now resides
  // since the run might have been shifted to the right due to collisions
  long findRunStart(long index) {
    int numRunsToSkip = 0;
    while (isShifted(index)) {
      index = (index - 1) & getSlotMask();
      if (isOccupied(index)) {
        numRunsToSkip++;
      }
    }
    while (numRunsToSkip > 0) {
      index = (index + 1) & getSlotMask();
      if (!isContinuation(index)) {
        numRunsToSkip--;
      }
    }
    return index;
  }

  // given the start of a run, scan the run and return the index of the first matching fingerprint
  // if not found returns the insertion position as bitwise complement to make it negative
  long findFirstFingerprintInRun(long index, final long fingerprint) {
    assert !isContinuation(index);
    do {
      final long fingerprintAtIndex = getFingerprint(index);
      if (fingerprintAtIndex == fingerprint) {
        return index;
      } else if (fingerprintAtIndex > fingerprint) {
        return ~index;
      }
      index = (index + 1) & getSlotMask();
    } while (isContinuation(index));
    return ~index;
  }

  // delete the last matching fingerprint in the run
  long decideWhichFingerprintToDelete(long index, final long fingerprint) {
    assert !isContinuation(index);
    long matchingFingerprintIndex = -1;
    do {
      if (compare(index, fingerprint)) {
        matchingFingerprintIndex = index;
      }
      index = (index + 1) & getSlotMask();
    } while (isContinuation(index));
    return matchingFingerprintIndex;
  }

  // given the start of a run, find the last slot index that still belongs to this run
  long findRunEnd(long index) {
    while (isContinuation((index + 1) & getSlotMask())) {
      index = (index + 1) & getSlotMask();
    }
    return index;
  }

  // given a canonical index slot and a fingerprint, find the relevant run and check if there is a matching fingerprint within it
  boolean search(final long fingerprint, final long index) {
    if (!isOccupied(index)) {
      return false;
    }
    final long runStartIndex = findRunStart(index);
    final long foundIndex = findFirstFingerprintInRun(runStartIndex, fingerprint);
    return foundIndex >= 0;
  }

  // Given a canonical slot index, find the corresponding run and return all fingerprints in the run.
  // This method is only used for testing purposes.
  Set<Long> getAllFingerprints(final long bucketIndex) {
    final boolean doesRunExist = isOccupied(bucketIndex);
    final HashSet<Long> set = new HashSet<Long>();
    if (!doesRunExist) {
      return set;
    }
    long runIndex = findRunStart(bucketIndex);
    do {
      set.add(getFingerprint(runIndex));
      runIndex = (runIndex + 1) & getSlotMask();
    } while (isContinuation(runIndex));
    return set;
  }

  boolean insert(final long fingerprint, final long index) {
    if (index >= getNumSlots() || numEntries_ == getNumSlots()) {
      return false;
    }
    final long runStart = findRunStart(index);
    if (!isOccupied(index)) {
      insertFingerprintAndPushAllElse(fingerprint, runStart, index, true, true);
      return true;
    }
    final long foundIndex = findFirstFingerprintInRun(runStart, fingerprint);
    if (foundIndex >= 0) {
      return false;
    }
    insertFingerprintAndPushAllElse(fingerprint, ~foundIndex, index, false, ~foundIndex == runStart);
    return true;
  }

  void insertFingerprintAndPushAllElse(long fingerprint, long index, final long canonical,
                                       final boolean isNewRun, final boolean isRunStart) {
    // in the first shifted entry set isContinuation flag if inserting at the start of the existing run
    // otherwise just shift the existing flag as it is
    boolean forceContinuation = !isNewRun && isRunStart;

    // prepare flags for the current slot
    boolean isContinuation = !isRunStart;
    boolean isShifted = index != canonical;

    // remember the existing entry from the current slot to be shifted to the next slot
    // isOccupied flag belongs to the slot, therefore it is never shifted
    // isShifted flag is always true for all shifted entries, no need to remember it
    long existingFingerprint = getFingerprint(index);
    boolean existingIsContinuation = isContinuation(index);

    while (!isSlotEmpty(index)) {
      // set the current slot
      setFingerprint(index, fingerprint);
      setContinuation(index, isContinuation);
      setShifted(index, isShifted);

      // prepare values for the next slot
      fingerprint = existingFingerprint;
      isContinuation = existingIsContinuation | forceContinuation;
      isShifted = true;

      index = (index + 1) & getSlotMask();

      // remember the existing entry to be shifted
      existingFingerprint = getFingerprint(index);
      existingIsContinuation = isContinuation(index);

      forceContinuation = false; // this is needed for the first shift only
    }
    // at this point the current slot is empty, so just populate with prepared values
    // either the incoming fingerprint or the last shifted one
    setFingerprint(index, fingerprint);
    setContinuation(index, isContinuation);
    setShifted(index, isShifted);

    if (isNewRun) {
      setOccupied(canonical, true);
    }
    numEntries_++;
  }

  boolean delete(final long canonicalSlot, long runStartIndex, long matchingFingerprintIndex) {
    long runEnd = findRunEnd(matchingFingerprintIndex);

    // the run has only one entry, we need to disable its is_occupied flag
    // we just remember we need to do this here, and we do it later to not interfere with counts
    boolean turnOffOccupied = runStartIndex == runEnd;

    // First thing to do is move everything else in the run back by one slot
    for (long i = matchingFingerprintIndex; i != runEnd; i = (i + 1) & getSlotMask()) {
      long f = getFingerprint((i + 1) & getSlotMask());
      setFingerprint(i, f);
    }

    // for each slot, we want to know by how much the entry there is shifted
    // we can do this by counting the number of continuation flags set to true
    // and the number of occupied flags set to false from the start of the cluster to the given cell
    // and then subtracting: num_shifted_count - num_non_occupied = number of slots by which an entry is shifted
    long clusterStart = findClusterStart(canonicalSlot);
    long numShiftedCount = 0;
    long numNonOccupied = 0;
    for (long i = clusterStart; i != ((runEnd + 1) & getSlotMask()); i = (i + 1) & getSlotMask()) {
      if (isContinuation(i)) {
        numShiftedCount++;
      }
      if (!isOccupied(i)) {
        numNonOccupied++;
      }
    }

    setFingerprint(runEnd, 0);
    setShifted(runEnd, false);
    setContinuation(runEnd, false);

    // we now have a nested loop. The outer do-while iterates over the remaining runs in the cluster.
    // the inner for loop iterates over cells of particular runs, pushing entries one slot back.
    do {
      // we first check if the next run actually exists and if it is shifted.
      // only if both conditions hold, we need to shift it back one slot.
      //boolean does_next_run_exist = !is_slot_empty(run_end + 1);
      //boolean is_next_run_shifted = is_shifted(run_end + 1);
      //if (!does_next_run_exist || !is_next_run_shifted) {
      if (isSlotEmpty((runEnd + 1) & getSlotMask()) || !isShifted((runEnd + 1) & getSlotMask())) {
        if (turnOffOccupied) {
          // if we eliminated a run and now need to turn the isOccupied flag off, we do it at the end to not interfere in our counts
          setOccupied(canonicalSlot, false);
        }
        return true;
      }

      // we now find the start and end of the next run
      final long nextRunStart = (runEnd + 1) & getSlotMask();
      runEnd = findRunEnd(nextRunStart);

      // before we start processing the next run, we check whether the previous run we shifted is now back to its canonical slot
      // The condition num_shifted_count - num_non_occupied == 1 ensures that the run was shifted by only 1 slot, meaning it is now back in its proper place
      if (isOccupied((nextRunStart - 1) & getSlotMask()) && numShiftedCount - numNonOccupied == 1) {
        setShifted((nextRunStart - 1) & getSlotMask(), false);
      } else {
        setShifted((nextRunStart - 1) & getSlotMask(), true);
      }

      for (long i = nextRunStart; i != ((runEnd + 1) & getSlotMask()); i = (i + 1) & getSlotMask()) {
        long f = getFingerprint(i);
        setFingerprint((i - 1) & getSlotMask(), f);
        if (isContinuation(i)) {
          setContinuation((i - 1) & getSlotMask(), true);
        }
        if (!isOccupied(i)) {
          numNonOccupied++;
        }
        if (i != nextRunStart) {
          numShiftedCount++;
        }
      }
      setFingerprint(runEnd, 0);
      setShifted(runEnd, false);
      setContinuation(runEnd, false);
    } while (true);
  }

  boolean delete(final long fingerprint, final long canonicalSlot) {
    // if the run doesn't exist, the key can't have possibly been inserted
    boolean doesRunExist = isOccupied(canonicalSlot);
    if (!doesRunExist) {
      return false;
    }
    long runStartIndex = findRunStart(canonicalSlot);
    long matchingFingerprintIndex = decideWhichFingerprintToDelete(runStartIndex, fingerprint);
    if (matchingFingerprintIndex == -1) {
      // we didn't find a matching fingerprint
      return false;
    }
    return delete(canonicalSlot, runStartIndex, matchingFingerprintIndex);
  }

  long getSlotFromHash(final long largeHash) {
    return (largeHash >> getFingerprintLength()) & getSlotMask();
  }

  long getFingerprintFromHash(final long largeHash) {
    return largeHash & getFingerprintMask();
  }

  /*
  This is the main insertion function accessed externally.
  It calls the underlying filter _insert function which hashes the input
  item internally.
  Hence, the `large_hash` argument is already a hash key that has been generated
  by the hashing library (eg xxhash).
   */
  protected boolean _insert(final long largeHash) {
    final long slotIndex = getSlotFromHash(largeHash);
    final long fingerprint = getFingerprintFromHash(largeHash);
    final boolean success = insert(fingerprint, slotIndex);

    if (numEntries_ == getMaxEntriesBeforeExpansion()) {
      expand();
    }
    return success;
  }

  protected boolean _delete(final long largeHash) {
    final long slotIndex = getSlotFromHash(largeHash);
    long fingerprint = getFingerprintFromHash(largeHash);
    boolean success = delete(fingerprint, slotIndex);
    if (success) {
      numEntries_--;
    }
    return success;
  }

  protected boolean _search(final long largeHash) {
    final long slotIndex = getSlotFromHash(largeHash);
    final long fingerprint = getFingerprintFromHash(largeHash);
    return search(fingerprint, slotIndex);
  }

  public boolean getBitAtOffset(final int offset) {
    return bitArray_.getBit(offset);
  }

  public void computeStatistics() {
    numRuns_ = 0;
    numClusters_ = 0;
    double sumRunLengths = 0;
    double sumClusterLengths = 0;

    int currentRunLength = 0;
    int currentCluster_length = 0;

    final long numSlots = getNumSlots();
    for (long i = 0; i < numSlots; i++) {
      final boolean occupied = isOccupied(i);
      final boolean continuation = isContinuation(i);
      final boolean shifted = isShifted(i);

      if (!occupied && !continuation && !shifted) { // empty slot
        sumClusterLengths += currentCluster_length;
        currentCluster_length = 0;
        sumRunLengths += currentRunLength;
        currentRunLength = 0;
      } else if ( !occupied && !continuation && shifted ) { // start of new run
        numRuns_++;
        sumRunLengths += currentRunLength;
        currentRunLength = 1;
        currentCluster_length++;
      } else if ( !occupied && continuation && !shifted ) {
        // not used
      } else if ( !occupied && continuation && shifted ) { // continuation of run
        currentCluster_length++;
        currentRunLength++;
      } else if ( occupied && !continuation && !shifted ) { // start of new cluster & run
        numRuns_++;
        numClusters_++;
        sumClusterLengths += currentCluster_length;
        sumRunLengths += currentRunLength;
        currentCluster_length = 1;
        currentRunLength = 1;
      } else if (occupied && !continuation && shifted ) { // start of new run
        numRuns_++;
        sumRunLengths += currentRunLength;
        currentRunLength = 1;
        currentCluster_length++;
      } else if (occupied && continuation && !shifted ) {
        // not used
      } else if (occupied && continuation && shifted ) { // continuation of run
        currentCluster_length++;
        currentRunLength++;
      }
    }
    avgRunLength_ = sumRunLengths / numRuns_;
    avgClusterLength_ = sumClusterLengths / numClusters_;
  }

  public void merge(final QuotientFilter other) {
    if (lgQ_ + numFingerprintBits_ != other.lgQ_ + other.numFingerprintBits_) {
      throw new SketchesArgumentException("incompatible sketches in merge");
    }
    long i = 0;
    if (!other.isSlotEmpty(i)) { i = other.findClusterStart(i); }

    final Queue<Long> fifo = new LinkedList<Long>();
    long count = 0;
    while (count < other.numEntries_) {
      if (!other.isSlotEmpty(i)) {
        if (other.isOccupied(i)) { fifo.add(i); }
        final long quotient = fifo.element();
        final long fingerprint = other.getFingerprint(i);
        final long hash = quotient << other.getFingerprintLength() | fingerprint;
        _insert(hash);
        count++;
      }
      i = (i + 1) & other.getSlotMask();
      if (!fifo.isEmpty() && ! other.isContinuation(i)) { fifo.remove(); }
    }
  }
}