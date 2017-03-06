package com.yahoo.sketches.quantiles;

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

  @Override
  public Double get(final int index) {
    assert index >= 0 && index < size_;
    return buffer_[index];
  }

  public Double set(final int index, final double value) {
    assert index >= 0 && index < size_;

    final double retVal = buffer_[index];
    buffer_[index] = value;
    return retVal;
  }

  @Override
  public int size() {
    return size_;
  }
}
