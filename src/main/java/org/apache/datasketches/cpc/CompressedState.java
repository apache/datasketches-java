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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.Util.computeSeedHash;
import static org.apache.datasketches.cpc.PreambleUtil.checkCapacity;
import static org.apache.datasketches.cpc.PreambleUtil.checkLoPreamble;
import static org.apache.datasketches.cpc.PreambleUtil.getDefinedPreInts;
import static org.apache.datasketches.cpc.PreambleUtil.getFiCol;
import static org.apache.datasketches.cpc.PreambleUtil.getFormatOrdinal;
import static org.apache.datasketches.cpc.PreambleUtil.getHipAccum;
import static org.apache.datasketches.cpc.PreambleUtil.getKxP;
import static org.apache.datasketches.cpc.PreambleUtil.getLgK;
import static org.apache.datasketches.cpc.PreambleUtil.getNumCoupons;
import static org.apache.datasketches.cpc.PreambleUtil.getNumSv;
import static org.apache.datasketches.cpc.PreambleUtil.getPreInts;
import static org.apache.datasketches.cpc.PreambleUtil.getSeedHash;
import static org.apache.datasketches.cpc.PreambleUtil.getSvLengthInts;
import static org.apache.datasketches.cpc.PreambleUtil.getSvStream;
import static org.apache.datasketches.cpc.PreambleUtil.getWLengthInts;
import static org.apache.datasketches.cpc.PreambleUtil.getWStream;
import static org.apache.datasketches.cpc.PreambleUtil.isCompressed;
import static org.apache.datasketches.cpc.PreambleUtil.putEmptyHip;
import static org.apache.datasketches.cpc.PreambleUtil.putEmptyMerged;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingHip;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingHipNoSv;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingMerged;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingMergedNoSv;
import static org.apache.datasketches.cpc.PreambleUtil.putSparseHybridHip;
import static org.apache.datasketches.cpc.PreambleUtil.putSparseHybridMerged;
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssert;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CompressedState {
  private static final String LS = System.getProperty("line.separator");
  private boolean csvIsValid = false;
  private boolean windowIsValid = false;
  final int lgK;
  final short seedHash;
  int fiCol = 0;
  boolean mergeFlag = false; //compliment of HIP Flag
  long numCoupons = 0;

  double kxp;
  double hipEstAccum = 0.0;

  int numCsv = 0;
  int[] csvStream = null; //may be longer than required
  int csvLengthInts = 0;
  int[] cwStream = null; //may be longer than required
  int cwLengthInts = 0;

  //int cpcRequiredBytes = 0;

  private CompressedState(final int lgK, final short seedHash) {
    this.lgK = lgK;
    this.seedHash = seedHash;
    kxp = 1 << lgK;
  }

  static CompressedState compress(final CpcSketch source) {
    final short seedHash = computeSeedHash(source.seed);
    final CompressedState target = new CompressedState(source.lgK, seedHash);
    target.fiCol = source.fiCol;
    target.mergeFlag = source.mergeFlag;

    target.numCoupons = source.numCoupons;
    target.kxp = source.kxp;
    target.hipEstAccum = source.hipEstAccum;

    target.csvIsValid = source.pairTable != null;
    target.windowIsValid = (source.slidingWindow != null);
    CpcCompression.compress(source, target);
    return target;
  }

  Flavor getFlavor() {
    return CpcUtil.determineFlavor(lgK, numCoupons);
  }

  Format getFormat() {
    final int ordinal = ((cwLengthInts > 0) ? 4 : 0)
                      | ((numCsv > 0) ? 2 : 0)
                      | ( mergeFlag ? 0 : 1 ); //complement of HIP
    return Format.ordinalToFormat(ordinal);
  }

  int getWindowOffset() {
    return CpcUtil.determineCorrectOffset(lgK, numCoupons);
  }

  long getRequiredSerializedBytes() {
    final Format format = getFormat();
    final int preInts = getDefinedPreInts(format);
    return 4L * (preInts + csvLengthInts + cwLengthInts);
  }

  static CompressedState importFromMemory(final Memory mem) {
    checkLoPreamble(mem);
    rtAssert(isCompressed(mem));
    final int lgK = getLgK(mem);
    final short seedHash = getSeedHash(mem);
    final CompressedState state = new CompressedState(lgK, seedHash);
    final int fmtOrd = getFormatOrdinal(mem);
    final Format format = Format.ordinalToFormat(fmtOrd);
    state.mergeFlag = !((fmtOrd & 1) > 0); //merge flag is complement of HIP
    state.csvIsValid = (fmtOrd & 2) > 0;
    state.windowIsValid = (fmtOrd & 4) > 0;

    switch (format) {
      case EMPTY_MERGED :
      case EMPTY_HIP : {
        checkCapacity(mem.getCapacity(), 8L);
        break;
      }
      case SPARSE_HYBRID_MERGED : {
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLengthInts = getSvLengthInts(mem);
        //state.cwLength = getCwLength(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        //state.cwStream = getCwStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
      case SPARSE_HYBRID_HIP : {
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLengthInts = getSvLengthInts(mem);
        //state.cwLength = getCwLength(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        //state.cwStream = getCwStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
      case PINNED_SLIDING_MERGED_NOSV : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        state.cwLengthInts = getWLengthInts(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        //state.csvStream = getCsvStream(mem);
        break;
      }
      case PINNED_SLIDING_HIP_NOSV : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        state.cwLengthInts = getWLengthInts(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        //state.csvStream = getCsvStream(mem);
        break;
      }
      case PINNED_SLIDING_MERGED : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = getNumSv(mem);
        state.csvLengthInts = getSvLengthInts(mem);
        state.cwLengthInts = getWLengthInts(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
      case PINNED_SLIDING_HIP : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = getNumSv(mem);
        state.csvLengthInts = getSvLengthInts(mem);
        state.cwLengthInts = getWLengthInts(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
    }
    checkCapacity(mem.getCapacity(),
        4L * (getPreInts(mem) + state.csvLengthInts + state.cwLengthInts));
    return state;
  }

  void exportToMemory(final WritableMemory wmem) {
    final Format format = getFormat();
    switch (format) {
      case EMPTY_MERGED : {
        putEmptyMerged(wmem, lgK, seedHash);
        break;
      }
      case EMPTY_HIP : {
        putEmptyHip(wmem, lgK, seedHash);
        break;
      }
      case SPARSE_HYBRID_MERGED : {
        putSparseHybridMerged(wmem,
            lgK,
            (int) numCoupons, //unsigned
            csvLengthInts,
            seedHash,
            csvStream);
        break;
      }
      case SPARSE_HYBRID_HIP : {
        putSparseHybridHip(wmem,
            lgK,
            (int) numCoupons, //unsigned
            csvLengthInts,
            kxp,
            hipEstAccum,
            seedHash,
            csvStream);
        break;
      }
      case PINNED_SLIDING_MERGED_NOSV : {
        putPinnedSlidingMergedNoSv(wmem,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            cwLengthInts,
            seedHash,
            cwStream);
        break;
      }
      case PINNED_SLIDING_HIP_NOSV : {
        putPinnedSlidingHipNoSv(wmem,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            cwLengthInts,
            kxp,
            hipEstAccum,
            seedHash,
            cwStream);
        break;
      }
      case PINNED_SLIDING_MERGED : {
        putPinnedSlidingMerged(wmem,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            numCsv,
            csvLengthInts,
            cwLengthInts,
            seedHash,
            csvStream,
            cwStream);
        break;
      }
      case PINNED_SLIDING_HIP : {
        putPinnedSlidingHip(wmem,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            numCsv,
            kxp,
            hipEstAccum,
            csvLengthInts,
            cwLengthInts,
            seedHash,
            csvStream,
            cwStream);
        break;
      }
    }
  }

  @Override
  public String toString() {
    return toString(this, false);
  }

  public static String toString(final CompressedState state, final boolean detail) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CompressedState").append(LS);
    sb.append("  Flavor     : ").append(state.getFlavor()).append(LS);
    sb.append("  Format     : ").append(state.getFormat()).append(LS);
    sb.append("  lgK        : ").append(state.lgK).append(LS);
    sb.append("  seedHash   : ").append(state.seedHash).append(LS);
    sb.append("  fiCol      : ").append(state.fiCol).append(LS);
    sb.append("  mergeFlag  : ").append(state.mergeFlag).append(LS);
    sb.append("  csvStream  : ").append(state.csvIsValid).append(LS);
    sb.append("  cwStream   : ").append(state.windowIsValid).append(LS);
    sb.append("  numCoupons : ").append(state.numCoupons).append(LS);
    sb.append("  kxp        : ").append(state.kxp).append(LS);
    sb.append("  hipAccum   : ").append(state.hipEstAccum).append(LS);
    sb.append("  numCsv     : ").append(state.numCsv).append(LS);
    sb.append("  csvLengthInts  : ").append(state.csvLengthInts).append(LS);
    sb.append("  csLength   : ").append(state.cwLengthInts).append(LS);
    if (detail) {
      if (state.csvStream != null) {
        sb.append("  CsvStream  : ").append(LS);
        for (int i = 0; i < state.csvLengthInts; i++) {
          sb.append(String.format("%8d %12d" + LS, i, state.csvStream[i]));
        }
      }
      if (state.cwStream != null) {
        sb.append("  CwStream  : ").append(LS);
        for (int i = 0; i < state.cwLengthInts; i++) {
          sb.append(String.format("%8d %12d" + LS, i, state.cwStream[i]));
        }
      }
    }
    return sb.toString();
  }

}

