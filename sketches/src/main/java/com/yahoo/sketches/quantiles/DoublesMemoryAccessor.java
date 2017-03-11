package com.yahoo.sketches.quantiles;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Jon Malkin
 */
final class DoublesMemoryAccessor extends DoublesBufferAccessor {
  private int numItems_;
  private Memory mem_;

  private DoublesMemoryAccessor(final Memory mem, final int numItems) {
    numItems_ = numItems;
    mem_ = mem;

    if (numItems << 3 > mem_.getCapacity()) {
      throw new SketchesArgumentException("Memory capacity insufficient to hold " + (numItems << 3)
              + " bytes: " + mem.getCapacity());
    }
  }

  static DoublesMemoryAccessor wrap(final Memory mem, final int numItems) {
    return new DoublesMemoryAccessor(mem, numItems);
  }

  @Override
  double get(final int index) {
    assert index >= 0 && index < numItems_;
    return mem_.getDouble(index << 3);
  }

  @Override
  double set(final int index, final double value) {
    assert index >= 0 && index < numItems_;

    final double retVal = mem_.getDouble(index << 3);
    mem_.putDouble(index << 3, value);
    return retVal;
  }

  @Override
  int numItems() {
    return numItems_;
  }

  @Override
  void sort() {
    final double[] baseBuffer = new double[numItems_];
    mem_.getDoubleArray(0, baseBuffer, 0, numItems_);
    Arrays.sort(baseBuffer, 0, numItems_);
    mem_.putDoubleArray(0, baseBuffer, 0, numItems_);

  }

  @Override
  double[] getArray(final int fromIdx, final int numItems) {
    final double[] dstArray = new double[numItems];
    mem_.getDoubleArray(fromIdx << 3, dstArray, 0, numItems);
    return dstArray;
  }

  @Override
  void putArray(final double[] srcArray, final int srcIndex,
                       final int dstIndex, final int numItems) {
    mem_.putDoubleArray(dstIndex << 3, srcArray, srcIndex, numItems);
  }
}
