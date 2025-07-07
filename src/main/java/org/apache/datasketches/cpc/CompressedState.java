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

import static org.apache.datasketches.common.Util.LS;
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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Util;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CompressedState {
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
    final short seedHash = Util.computeSeedHash(source.seed);
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

  static CompressedState importFromSegment(final MemorySegment seg) {
    checkLoPreamble(seg);
    rtAssert(isCompressed(seg));
    final int lgK = getLgK(seg);
    final short seedHash = getSeedHash(seg);
    final CompressedState state = new CompressedState(lgK, seedHash);
    final int fmtOrd = getFormatOrdinal(seg);
    final Format format = Format.ordinalToFormat(fmtOrd);
    state.mergeFlag = ((fmtOrd & 1) <= 0); //merge flag is complement of HIP
    state.csvIsValid = (fmtOrd & 2) > 0;
    state.windowIsValid = (fmtOrd & 4) > 0;

    switch (format) {
      case EMPTY_MERGED :
      case EMPTY_HIP : {
        checkCapacity(seg.byteSize(), 8L);
        break;
      }
      case SPARSE_HYBRID_MERGED : {
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(seg);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLengthInts = getSvLengthInts(seg);
        //state.cwLength = getCwLength(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(seg.byteSize(), state.getRequiredSerializedBytes());
        //state.cwStream = getCwStream(mem);
        state.csvStream = getSvStream(seg);
        break;
      }
      case SPARSE_HYBRID_HIP : {
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(seg);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLengthInts = getSvLengthInts(seg);
        //state.cwLength = getCwLength(mem);
        state.kxp = getKxP(seg);
        state.hipEstAccum = getHipAccum(seg);
        checkCapacity(seg.byteSize(), state.getRequiredSerializedBytes());
        //state.cwStream = getCwStream(mem);
        state.csvStream = getSvStream(seg);
        break;
      }
      case PINNED_SLIDING_MERGED_NOSV : {
        state.fiCol = getFiCol(seg);
        state.numCoupons = getNumCoupons(seg);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        state.cwLengthInts = getWLengthInts(seg);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(seg.byteSize(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(seg);
        //state.csvStream = getCsvStream(mem);
        break;
      }
      case PINNED_SLIDING_HIP_NOSV : {
        state.fiCol = getFiCol(seg);
        state.numCoupons = getNumCoupons(seg);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        state.cwLengthInts = getWLengthInts(seg);
        state.kxp = getKxP(seg);
        state.hipEstAccum = getHipAccum(seg);
        checkCapacity(seg.byteSize(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(seg);
        //state.csvStream = getCsvStream(mem);
        break;
      }
      case PINNED_SLIDING_MERGED : {
        state.fiCol = getFiCol(seg);
        state.numCoupons = getNumCoupons(seg);
        state.numCsv = getNumSv(seg);
        state.csvLengthInts = getSvLengthInts(seg);
        state.cwLengthInts = getWLengthInts(seg);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(seg.byteSize(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(seg);
        state.csvStream = getSvStream(seg);
        break;
      }
      case PINNED_SLIDING_HIP : {
        state.fiCol = getFiCol(seg);
        state.numCoupons = getNumCoupons(seg);
        state.numCsv = getNumSv(seg);
        state.csvLengthInts = getSvLengthInts(seg);
        state.cwLengthInts = getWLengthInts(seg);
        state.kxp = getKxP(seg);
        state.hipEstAccum = getHipAccum(seg);
        checkCapacity(seg.byteSize(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(seg);
        state.csvStream = getSvStream(seg);
        break;
      }
    }
    checkCapacity(seg.byteSize(),
        4L * (getPreInts(seg) + state.csvLengthInts + state.cwLengthInts));
    return state;
  }

  void exportToSegment(final MemorySegment wseg) {
    final Format format = getFormat();
    switch (format) {
      case EMPTY_MERGED : {
        putEmptyMerged(wseg, lgK, seedHash);
        break;
      }
      case EMPTY_HIP : {
        putEmptyHip(wseg, lgK, seedHash);
        break;
      }
      case SPARSE_HYBRID_MERGED : {
        putSparseHybridMerged(wseg,
            lgK,
            (int) numCoupons, //unsigned
            csvLengthInts,
            seedHash,
            csvStream);
        break;
      }
      case SPARSE_HYBRID_HIP : {
        putSparseHybridHip(wseg,
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
        putPinnedSlidingMergedNoSv(wseg,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            cwLengthInts,
            seedHash,
            cwStream);
        break;
      }
      case PINNED_SLIDING_HIP_NOSV : {
        putPinnedSlidingHipNoSv(wseg,
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
        putPinnedSlidingMerged(wseg,
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
        putPinnedSlidingHip(wseg,
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

