/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.cpc.PreambleUtil.getDefinedPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.putEmpty;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingHipNoSv;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingMerged;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingMergedNoSv;
import static com.yahoo.sketches.cpc.PreambleUtil.putSparseHybridHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putSparseHybridMerged;

import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CompressedState {
  final int lgK;
  final short seedHash;
  int fiCol;
  boolean mergeFlag; //compliment of HIP Flag
  boolean svIsValid;
  boolean windowIsValid;

  long numCoupons;

  double kxp;
  double hipEstAccum;

  int numPairs;
  int[] csvStream; //may be longer than required
  int csvLength;
  int[] cwStream; //may be longer than required
  int cwLength;

  private CompressedState(final int lgK, final short seedHash) {
    this.lgK = lgK;
    this.seedHash = seedHash;
  }

  static CompressedState compress(final CpcSketch source) {
    final short seedHash = computeSeedHash(source.seed);
    final CompressedState target = new CompressedState(source.lgK, seedHash);
    target.fiCol = source.fiCol;
    target.mergeFlag = source.mergeFlag;
    target.numCoupons = source.numCoupons;
    target.kxp = source.kxp;
    target.hipEstAccum = source.hipEstAccum;

    target.svIsValid = source.pairTable != null;
    target.windowIsValid = (source.slidingWindow != null);

    target.numPairs = 0;
    target.csvStream = null;
    target.cwStream = null;
    target.cwLength = 0;
    CpcCompression.compress(source, target);
    return target;
  }

  Flavor getFlavor() {
    return CpcUtil.determineFlavor(lgK, numCoupons);
  }

  int getWindowOffset() {
    return CpcSketch.determineCorrectOffset(lgK, numCoupons);
  }

  Format getFormat() {
    final int ordinal = ((cwLength > 0) ? 4 : 0)
                      | ((numPairs > 0) ? 2 : 0)
                      | ( mergeFlag ? 0 : 1 );
    return Format.ordinalToFormat(ordinal);
  }

  long getMemoryCapacity() {
    final Format format = getFormat();
    if (format == Format.EMPTY) { return 8; }
    final int preInts = getDefinedPreInts(format);
    return 4L * (preInts + numPairs + cwLength);
  }

  void loadToMemory(final WritableMemory wmem) {
    final Format format = getFormat();
    switch (format) {
      case EMPTY : {
        putEmpty(wmem, lgK, seedHash);
        break;
      }
      case NONE : {
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
      case PINNED_SLIDING_MERGED_NOSV : {
        putPinnedSlidingMergedNoSv(wmem,
            lgK,
            fiCol,
            (int) numCoupons, //unsigned
            cwLength,
            seedHash,
            cwStream);
        break;
      }
      case PINNED_SLIDING_HIP_NOSV : {
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
            numPairs,
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
            numPairs,
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

}

