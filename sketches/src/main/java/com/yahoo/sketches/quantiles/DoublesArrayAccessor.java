package com.yahoo.sketches.quantiles;

import java.util.Arrays;

/**
 * @author Jon Malkin
 */
final class DoublesArrayAccessor extends DoublesBufferAccessor {
  private int numItems_;
  private double[] buffer_;

  private DoublesArrayAccessor(final double[] buffer) {
    numItems_ = buffer.length;
    buffer_ = buffer;
  }

  static DoublesArrayAccessor wrap(final double[] buffer) {
    return new DoublesArrayAccessor(buffer);
  }

  static DoublesArrayAccessor initialize(final int numItems) {
    return new DoublesArrayAccessor(new double[numItems]);
  }

  @Override
  double get(final int index) {
    assert index >= 0 && index < numItems_;
    return buffer_[index];
  }

  @Override
  double set(final int index, final double value) {
    assert index >= 0 && index < numItems_;

    final double retVal = buffer_[index];
    buffer_[index] = value;
    return retVal;
  }

  @Override
  int numItems() {
    return numItems_;
  }

  @Override
  void sort() {
    Arrays.sort(buffer_);
  }

  @Override
  double[] getArray(final int fromIdx, final int numItems) {
    return Arrays.copyOfRange(buffer_, fromIdx, fromIdx + numItems);
  }

  @Override
  void putArray(final double[] srcArray, final int srcIndex,
                       final int dstIndex, final int numItems) {
    System.arraycopy(srcArray, srcIndex, buffer_, dstIndex, numItems);
  }

}
