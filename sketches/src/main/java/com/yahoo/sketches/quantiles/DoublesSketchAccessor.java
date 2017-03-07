package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;

/**
 * @author Jon Malkin
 */
final class DoublesSketchAccessor extends DoublesBufferAccessor {
  private static final int BB_LVL_IDX = -1;

  private final DoublesSketch ds_;
  private final boolean forceSize_;

  private long n_;
  private int currLvl_;
  private int size_;
  private int offset_;

  private DoublesSketchAccessor(final DoublesSketch ds,
                                final boolean forceSize,
                                final int level) {
    ds_ = ds;
    forceSize_ = forceSize;

    setLevel(level);
  }

  public static DoublesSketchAccessor create(final DoublesSketch ds) {
    return create(ds, false);
  }

  public static DoublesSketchAccessor create(final DoublesSketch ds,
                                             final boolean forceSize) {
    return new DoublesSketchAccessor(ds, forceSize, BB_LVL_IDX);
  }

  public DoublesSketchAccessor copyAndSetLevel(final int level) {
    return new DoublesSketchAccessor(ds_, forceSize_, level);
  }

  public int setLevel(final int lvl) {
    currLvl_ = lvl;
    if (lvl == BB_LVL_IDX) {
      size_ = (forceSize_ ? ds_.getK() * 2 : ds_.getBaseBufferCount());
      offset_ = (ds_.isDirect() ? COMBINED_BUFFER : 0);
    } else {
      assert lvl >= 0 && lvl <= Util.computeTotalLevels(ds_.getBitPattern());
      if ((ds_.getBitPattern() & (1L << lvl)) > 0 || forceSize_) {
        size_ = ds_.getK();
      } else {
        size_ = 0;
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

    return size_;
  }

  public int getTotalLevels() {
    return Util.computeTotalLevels(ds_.getBitPattern());
  }

  public int getLevel() {
    return currLvl_;
  }

  /* Uses autoboxing to handle double/Double disparity */
  @Override
  public Double get(final int index) {
    assert index >= 0 && index < size_;
    assert n_ == ds_.getN();

    if (ds_.isDirect()) {
      final int idxOffset = offset_ + (index << 3);
      return ds_.getMemory().getDouble(idxOffset);
    } else {
      return ds_.getCombinedBuffer()[offset_ + index];
    }
  }

  /* Uses autoboxing to handle double/Double disparity */
  @Override
  public Double set(final int index, final Double value) {
    assert index >= 0 && index < size_;
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
  public int size() {
    return size_;
  }

  @Override
  public double[] getArray(final int fromIdx, final int numItems) {
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
  public void putArray(final double[] srcArray, final int srcIndex,
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
  }

  public static void main(final String[] args) {
    final int k = 16;
    final int n = k * 15 + 3;

    final ByteBuffer bb = ByteBuffer.allocateDirect(100 + n << 3);
    final Memory mem = AllocMemory.wrap(bb);

    final DoublesSketchBuilder dsb = DoublesSketch.builder();
    dsb.initMemory(mem);
    final UpdateDoublesSketch ds = dsb.build(k);

    for (int i = 0; i < n; ++i) {
      ds.update(i);
    }

    System.out.println(ds.toString(true, true));

    final DoublesSketchAccessor acc = DoublesSketchAccessor.create(ds, false);
    for (int i = -1; i < acc.getTotalLevels(); ++i) {
      acc.setLevel(i);
      System.out.println("Level: " + i + "\t(" + acc.size() + ")");
      for (Double item : acc) {
        //System.out.println("\t" + j + ": " + acc.get(j));
        System.out.print(String.format("%10.1f", item));
      }
      System.out.println("");
    }
  }
}
