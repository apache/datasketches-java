package com.yahoo.sketches.quantiles;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Jon Malkin
 */
final class DoublesMemoryAccessor extends DoublesBufferAccessor {
  private int size_;
  private Memory mem_;

  DoublesMemoryAccessor(final Memory mem, final int size) {
    size_ = size;
    mem_ = mem;

    if (size << 3 > mem_.getCapacity()) {
      throw new SketchesArgumentException("Memory capacity insufficient to hold " + (size << 3)
              + " bytes: " + mem.getCapacity());
    }
  }

  @Override
  public double get(final int index) {
    assert index >= 0 && index < size_;
    return mem_.getDouble(index << 3);
  }

  @Override
  public double set(final int index, final Double value) {
    assert index >= 0 && index < size_;

    final double retVal = mem_.getDouble(index << 3);
    mem_.putDouble(index << 3, value);
    return retVal;
  }

  @Override
  public int size() {
    return size_;
  }

  @Override
  public void sort() {
    final double[] baseBuffer = new double[size_];
    mem_.getDoubleArray(0, baseBuffer, 0, size_);
    Arrays.sort(baseBuffer, 0, size_);
    mem_.putDoubleArray(0, baseBuffer, 0, size_);

  }

  @Override
  public double[] getArray(final int fromIdx, final int numItems) {
    final double[] dstArray = new double[numItems];
    mem_.getDoubleArray(fromIdx << 3, dstArray, 0, numItems);
    return dstArray;
  }

  @Override
  public void putArray(final double[] srcArray, final int srcIndex,
                       final int dstIndex, final int numItems) {
    mem_.putDoubleArray(dstIndex << 3, srcArray, srcIndex, numItems);
  }
}
