/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.cpc.PreambleUtil.checkLoPreamble;
import static com.yahoo.sketches.cpc.PreambleUtil.getCsvLength;
import static com.yahoo.sketches.cpc.PreambleUtil.getCsvStream;
import static com.yahoo.sketches.cpc.PreambleUtil.getCwLength;
import static com.yahoo.sketches.cpc.PreambleUtil.getCwStream;
import static com.yahoo.sketches.cpc.PreambleUtil.getDefinedPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getFiCol;
import static com.yahoo.sketches.cpc.PreambleUtil.getHipAccum;
import static com.yahoo.sketches.cpc.PreambleUtil.getKxP;
import static com.yahoo.sketches.cpc.PreambleUtil.getLgK;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumCoupons;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumCsv;
import static com.yahoo.sketches.cpc.PreambleUtil.getSeedHash;
import static com.yahoo.sketches.cpc.PreambleUtil.hasCsv;
import static com.yahoo.sketches.cpc.PreambleUtil.hasHip;
import static com.yahoo.sketches.cpc.PreambleUtil.hasWindow;
import static com.yahoo.sketches.cpc.PreambleUtil.putEmptyHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putEmptyMerged;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingHipNoSv;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingMerged;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingMergedNoSv;
import static com.yahoo.sketches.cpc.PreambleUtil.putSparseHybridHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putSparseHybridMerged;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CompressedState {
  private static final String LS = System.getProperty("line.separator");
  final int lgK;
  final short seedHash;
  int fiCol = 0;
  boolean mergeFlag = false; //compliment of HIP Flag
  boolean csvIsValid = false;
  boolean windowIsValid = false;

  long numCoupons = 0;

  double kxp;
  double hipEstAccum = 0.0;

  int numCsv = 0;
  int[] csvStream = null; //may be longer than required
  int csvLength = 0;
  int[] cwStream = null; //may be longer than required
  int cwLength = 0;

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
    final int ordinal = ((cwLength > 0) ? 4 : 0)
                      | ((numCsv > 0) ? 2 : 0)
                      | ( mergeFlag ? 0 : 1 ); //complement of HIP
    return Format.ordinalToFormat(ordinal);
  }

  int getWindowOffset() {
    return CpcUtil.determineCorrectOffset(lgK, numCoupons);
  }

  long getMemoryCapacity() {
    final Format format = getFormat();
    final int preInts = getDefinedPreInts(format);
    return 4L * (preInts + csvLength + cwLength);
  }

  static CompressedState importFromMemory(final Memory mem) {
    checkLoPreamble(mem);
    final int lgK = getLgK(mem);
    final short seedHash = getSeedHash(mem);
    final CompressedState state = new CompressedState(lgK, seedHash);
    final Format format = PreambleUtil.getFormat(mem);
    switch (format) {
      case EMPTY_MERGED :
      case EMPTY_HIP : {
        state.mergeFlag = !hasHip(mem); //complement of HIP
        break;
      }
      case SPARSE_HYBRID_MERGED : {
        state.mergeFlag = !hasHip(mem); //complement of HIP
        state.csvIsValid = hasCsv(mem);
        state.windowIsValid = hasWindow(mem);
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLength = getCsvLength(mem);
        state.csvStream = getCsvStream(mem);
        //state.cwLength = getCwLength(mem);
        //state.cwStream = getCwStream(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        break;
      }
      case SPARSE_HYBRID_HIP : {
        state.mergeFlag = !hasHip(mem); //complement of HIP
        state.csvIsValid = hasCsv(mem);
        state.windowIsValid = hasWindow(mem);
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLength = getCsvLength(mem);
        state.csvStream = getCsvStream(mem);
        //state.cwLength = getCwLength(mem);
        //state.cwStream = getCwStream(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        break;
      }
      case PINNED_SLIDING_MERGED_NOCSV : {
        state.mergeFlag = !hasHip(mem); //complement of HIP
        state.csvIsValid = hasCsv(mem);
        state.windowIsValid = hasWindow(mem);
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        //state.csvStream = getCsvStream(mem);
        state.cwLength = getCwLength(mem);
        state.cwStream = getCwStream(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        break;
      }
      case PINNED_SLIDING_HIP_NOCSV : {
        state.mergeFlag = !hasHip(mem); //complement of HIP
        state.csvIsValid = hasCsv(mem);
        state.windowIsValid = hasWindow(mem);
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        //state.csvStream = getCsvStream(mem);
        state.cwLength = getCwLength(mem);
        state.cwStream = getCwStream(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        break;
      }
      case PINNED_SLIDING_MERGED : {
        state.mergeFlag = !hasHip(mem); //complement of HIP
        state.csvIsValid = hasCsv(mem);
        state.windowIsValid = hasWindow(mem);
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = getNumCsv(mem);
        state.csvLength = getCsvLength(mem);
        state.csvStream = getCsvStream(mem);
        state.cwLength = getCwLength(mem);
        state.cwStream = getCwStream(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        break;
      }
      case PINNED_SLIDING_HIP : {
        state.mergeFlag = !hasHip(mem); //complement of HIP
        state.csvIsValid = hasCsv(mem);
        state.windowIsValid = hasWindow(mem);
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = getNumCsv(mem);
        state.csvLength = getCsvLength(mem);
        state.csvStream = getCsvStream(mem);
        state.cwLength = getCwLength(mem);
        state.cwStream = getCwStream(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        break;
      }
    }
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
            csvLength,
            seedHash,
            csvStream);
        break;
      }
      case SPARSE_HYBRID_HIP : {
        putSparseHybridHip(wmem,
            lgK,
            (int) numCoupons, //unsigned
            csvLength,
            kxp,
            hipEstAccum,
            seedHash,
            csvStream);
        break;
      }
      case PINNED_SLIDING_MERGED_NOCSV : {
        putPinnedSlidingMergedNoSv(wmem,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            cwLength,
            seedHash,
            cwStream);
        break;
      }
      case PINNED_SLIDING_HIP_NOCSV : {
        putPinnedSlidingHipNoSv(wmem,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            cwLength,
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
            csvLength,
            cwLength,
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
            csvLength,
            cwLength,
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
    sb.append("  csvLength  : ").append(state.csvLength).append(LS);
    sb.append("  csLength   : ").append(state.cwLength).append(LS);
    if (detail) {
      if (state.csvStream != null) {
        sb.append("  CsvStream  : ").append(LS);
        for (int i = 0; i < state.csvLength; i++) {
          sb.append(String.format("%8d %12d\n", i, state.csvStream[i]));
        }
      }
      if (state.cwStream != null) {
        sb.append("  CwStream  : ").append(LS);
        for (int i = 0; i < state.cwLength; i++) {
          sb.append(String.format("%8d %12d\n", i, state.cwStream[i]));
        }
      }
    }
    return sb.toString();
  }

}
