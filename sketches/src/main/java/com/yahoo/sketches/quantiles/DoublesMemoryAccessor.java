package com.yahoo.sketches.quantiles;

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
  public Double get(final int index) {
    assert index >= 0 && index < size_;
    return mem_.getDouble(index << 3);
  }

  public Double set(final int index, final double value) {
    assert index >= 0 && index < size_;

    final double retVal = mem_.getDouble(index << 3);
    mem_.putDouble(index << 3, value);
    return retVal;
  }

  @Override
  public int size() {
    return size_;
  }
}
