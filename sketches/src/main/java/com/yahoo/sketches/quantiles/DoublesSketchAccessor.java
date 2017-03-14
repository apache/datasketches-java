package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;

/**
 * @author Jon Malkin
 */
final class DoublesSketchAccessor extends DoublesBufferAccessor {
  static final int BB_LVL_IDX = -1;

  private final DoublesSketch ds_;
  private final boolean forceSize_;

  private long n_;
  private int currLvl_;
  private int numItems_;
  private int offset_;

  private DoublesSketchAccessor(final DoublesSketch ds,
                                final boolean forceSize,
                                final int level) {
    ds_ = ds;
    forceSize_ = forceSize;

    setLevel(level);
  }

  static DoublesSketchAccessor wrap(final DoublesSketch ds) {
    return wrap(ds, false);
  }

  static DoublesSketchAccessor wrap(final DoublesSketch ds,
                                    final boolean forceSize) {
    return new DoublesSketchAccessor(ds, forceSize, BB_LVL_IDX);
  }

  DoublesSketchAccessor copyAndSetLevel(final int level) {
    return new DoublesSketchAccessor(ds_, forceSize_, level);
  }

  DoublesSketchAccessor setLevel(final int lvl) {
    currLvl_ = lvl;
    if (lvl == BB_LVL_IDX) {
      numItems_ = (forceSize_ ? ds_.getK() * 2 : ds_.getBaseBufferCount());
      offset_ = (ds_.isDirect() ? COMBINED_BUFFER : 0);
    } else {
      assert lvl >= 0;
      if ((ds_.getBitPattern() & (1L << lvl)) > 0 || forceSize_) {
        numItems_ = ds_.getK();
      } else {
        numItems_ = 0;
      }

      // determine offset in two parts
      // 1. index into combined buffer (compact vs update)
      // 2. adjust if byte offset (direct) instead of array index (heap)
      final int levelStart;
      if (ds_.isCompact()) {
        levelStart = ds_.getBaseBufferCount() + countValidLevelsBelow(lvl);
      } else {
        levelStart = (2 + currLvl_) * ds_.getK();
      }

      if (ds_.isDirect()) {
        final int preLongsAndExtra = Family.QUANTILES.getMaxPreLongs() + 2; // +2 for min, max vals
        offset_ = (preLongsAndExtra + levelStart) << 3;
      } else {
        offset_ = levelStart;
      }
    }

    n_ = ds_.getN();

    return this;
  }

  int getLevel() {
    return currLvl_;
  }

  @Override
  double get(final int index) {
    assert index >= 0 && index < numItems_;
    assert n_ == ds_.getN();

    if (ds_.isDirect()) {
      final int idxOffset = offset_ + (index << 3);
      return ds_.getMemory().getDouble(idxOffset);
    } else {
      return ds_.getCombinedBuffer()[offset_ + index];
    }
  }

  @Override
  double set(final int index, final double value) {
    assert index >= 0 && index < numItems_;
    assert n_ == ds_.getN();

    final double oldVal;
    final int idxOffset;
    if (ds_.isDirect()) {
      idxOffset = offset_ + (index << 3);
      oldVal = ds_.getMemory().getDouble(idxOffset);
      ds_.getMemory().putDouble(idxOffset, value);
    } else {
      idxOffset = offset_ + index;
      oldVal = ds_.getCombinedBuffer()[idxOffset];
      ds_.getCombinedBuffer()[idxOffset] = value;
    }

    return oldVal;
  }

  @Override
  int numItems() {
    return numItems_;
  }

  @Override
  void sort() {
    if (ds_.isDirect()) {
      final double[] tmpBuffer = new double[numItems_];
      final Memory mem = ds_.getMemory();
      mem.getDoubleArray(offset_, tmpBuffer, 0, numItems_);
      Arrays.sort(tmpBuffer, 0, numItems_);
      mem.putDoubleArray(offset_, tmpBuffer, 0, numItems_);
    } else {
      Arrays.sort(ds_.getCombinedBuffer(), offset_, offset_ + numItems_);
    }
  }

  @Override
  double[] getArray(final int fromIdx, final int numItems) {
    if (ds_.isDirect()) {
      final double[] dstArray = new double[numItems];
      final int offsetBytes = offset_ + (fromIdx << 3);
      ds_.getMemory().getDoubleArray(offsetBytes, dstArray, 0, numItems);
      return dstArray;
    } else {
      final int stIdx = offset_ + fromIdx;
      return Arrays.copyOfRange(ds_.getCombinedBuffer(), stIdx, stIdx + numItems);
    }
  }

  @Override
  void putArray(final double[] srcArray, final int srcIndex,
                       final int dstIndex, final int numItems) {
    if (ds_.isDirect()) {
      final int offsetBytes = offset_ + (dstIndex << 3);
      ds_.getMemory().putDoubleArray(offsetBytes, srcArray, srcIndex, numItems);
    } else {
      final int tgtIdx = offset_ + dstIndex;
      System.arraycopy(srcArray, srcIndex, ds_.getCombinedBuffer(), tgtIdx, numItems);
    }
  }


  /**
   * Counts number of full levels in the sketch below tgtLvl. Useful for computing the level
   * offset in a compact sketch.
   * @param tgtLvl Target level in the sketch
   * @return Number of full levels in the sketch below tgtLvl
   */
  private int countValidLevelsBelow(final int tgtLvl) {
    int count = 0;
    long bitPattern = ds_.getBitPattern();
    for (int i = 0; i < tgtLvl && bitPattern > 0; ++i, bitPattern >>>= 1) {
      if ((bitPattern & 1L) > 0) {
        ++count;
      }
    }
    return count;

    // shorter implementation, testing suggests a tiny bit slower
    //final long mask = (1 << tgtLvl) - 1;
    //return Long.bitCount(ds_.getBitPattern() & mask);
  }
}
