package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;

import java.nio.ByteBuffer;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Jon Malkin
 */
final class DoublesSketchAccessor extends DoublesBufferAccessor {
  private static final int BB_LVL_IDX = -1;

  private final DoublesSketch ds_;

  private int currLvl_;
  private int size_;
  private int offset_;
  private long n_;


  DoublesSketchAccessor(final DoublesSketch ds) {
    ds_ = ds;

    setLevel(BB_LVL_IDX);
  }

  public int setBaseBuffer() {
    return setLevel(BB_LVL_IDX);
  }

  public int setLevel(final int lvl) {
    currLvl_ = lvl;
    if (lvl == BB_LVL_IDX) {
      size_ = ds_.getBaseBufferCount();
      offset_ = (ds_.isDirect() ? COMBINED_BUFFER : 0);
    } else if (lvl < 0 || lvl > 63) {
      throw new SketchesArgumentException("Invalid combined buffer level requested: " + lvl);
    } else {
      long bitPattern = ds_.getBitPattern();
      bitPattern >>>= lvl;

      if ((bitPattern & 1L) > 0) {
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
  public Double get(final int index) {
    /*
    if (index >= size_ || index < 0) {
      throw new IndexOutOfBoundsException("Expected [0, " + (size_ - 1) + "], found: " + index);
    }
    */
    assert index > 0 && index < size_;
    assert n_ == ds_.getN();

    if (ds_.isDirect()) {
      final int idxOffset = offset_ + (index << 3);
      return ds_.getMemory().getDouble(idxOffset);
    } else {
      return ds_.getCombinedBuffer()[offset_ + index];
    }
  }

  /* Uses autoboxing to handle double/Double disparity */
  public Double set(final int index, final Double value) {
    /*
    if (index >= size_ || index < 0) {
      throw new IndexOutOfBoundsException("Expected [0, " + (size_ - 1) + "], found: " + index);
    }
    */
    assert index > 0 && index < size_;
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

  public int size() {
    return size_;
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

    final DoublesSketchAccessor it = new DoublesSketchAccessor(ds);
    for (int i = -1; i < it.getTotalLevels(); ++i) {
      it.setLevel(i);
      System.out.println("Level: " + i + "\t(" + it.size() + ")");
      for (Double item : it) {
        //System.out.println("\t" + j + ": " + it.get(j));
        System.out.print(String.format("%10.1f", item));
      }
      System.out.println("");
    }
  }
}
