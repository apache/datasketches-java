package com.yahoo.sketches.quantiles;

import java.util.Arrays;

/**
 * @author Jon Malkin
 */
final class DoublesArrayAccessor extends DoublesBufferAccessor {
  private int size_;
  private double[] buffer_;

  DoublesArrayAccessor(final double[] buffer) {
    size_ = buffer.length;
    buffer_ = buffer;
  }

  DoublesArrayAccessor(final int size) {
    size_ = size;
    buffer_ = new double[size];
  }

  @Override
  public double get(final int index) {
    assert index >= 0 && index < size_;
    return buffer_[index];
  }

  @Override
  public double set(final int index, final Double value) {
    assert index >= 0 && index < size_;

    final double retVal = buffer_[index];
    buffer_[index] = value;
    return retVal;
  }

  @Override
  public int size() {
    return size_;
  }

  @Override
  public void sort() {
    Arrays.sort(buffer_);
  }

  @Override
  public double[] getArray(final int fromIdx, final int numItems) {
    return Arrays.copyOfRange(buffer_, fromIdx, fromIdx + numItems);
  }

  @Override
  public void putArray(final double[] srcArray, final int srcIndex,
                       final int dstIndex, final int numItems) {
    System.arraycopy(srcArray, srcIndex, buffer_, dstIndex, numItems);
  }

}
