/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package org.apache.datasketches.req;

import static org.apache.datasketches.req.ReqHelper.LS;

import java.util.Arrays;

import org.apache.datasketches.SketchesArgumentException;

/**
 * A special buffer of floats.
 *
 * @author Lee Rhodes
 */
class FloatBuffer {
  private float[] arr_;
  private int count_;
  private int capacity_;
  private int delta_;
  private boolean sorted_;

  /**
   * Constructs a new FloatBuffer with a default size of 1024 items and delta of 256 items.
   */
  FloatBuffer() {
    this(1024, 256);
  }

  /**
   * Constructs an empty FloatBuffer with an initial capacity specified by
   * the <code>capacity</code> argument.
   *
   * @param capacity the initial capacity.
   * @param delta add space in increments of this size
   */
  FloatBuffer(final int capacity, final int delta) {
    arr_ = new float[capacity];
    count_ = 0;
    capacity_ = capacity;
    delta_ = delta;
    sorted_ = true;
  }

  /**
   * Copy Constructor
   * @param buf the FloatBuffer to be copied into this one
   */
  FloatBuffer(final FloatBuffer buf) {
    arr_ = buf.arr_.clone();
    count_ = buf.count_;
    capacity_ = buf.capacity_;
    delta_ = buf.delta_;
    sorted_ = buf.sorted_;
  }

  /**
   * Private construction from elements
   * @param arr the array to be used directly as the internal array
   * @param count the number of active elements in the given array
   * @param delta add space in increments of this size
   * @param capacity the initial capacity
   * @param sorted true if already sorted
   */
  private FloatBuffer(final float[] arr, final int count, final int delta, final int capacity,
      final boolean sorted) {
    arr_ = arr;
    count_ = count;
    capacity_ = capacity;
    delta_ = delta;
    sorted_ = sorted;
  }

  /**
   * Wraps the given array to use as the internal array; thus no copies. For internal use.
   * @param arr the given array
   * @return this, which will be sorted
   */
  static FloatBuffer wrap(final float[] arr, final boolean isSorted) {
    final FloatBuffer buf = new FloatBuffer(arr, arr.length, 0, arr.length, isSorted);
    buf.sort();
    return buf;
  }

  /**
   * Appends the given item to the end of the active array and increments length().
   * This will expand the array if necessary.
   * @param item the given item
   * @return this
   */
  FloatBuffer append(final float item) {
    ensureSpace(1);
    arr_[count_++] = item;
    sorted_ = false;
    return this;
  }

  /**
   * Ensures that the capacity of this FloatBuffer is at least newCapacity.
   * If newCapacity &lt; capacity(), no action is taken.
   * @return this
   */
  private FloatBuffer ensureCapacity(final int newCapacity) {
    if (newCapacity > capacity_) {
      arr_ = Arrays.copyOf(arr_, newCapacity);
      capacity_ = newCapacity;
    }
    return this;
  }

  /**
   * Ensures that the space remaining (capacity() - length()) is at least the given space.
   * @param space the requested space remaining
   * @return this
   */
  private FloatBuffer ensureSpace(final int space) {
    if ((count_ + space) > arr_.length) {
      ensureCapacity(count_ + space + delta_);
    }
    return this;
  }

  /**
   * Returns a reference to the internal item array. Be careful and don't modify this array!
   * @return the internal item array.
   */
  float[] getArray() {
    return arr_;
  }

  /**
   * Gets the current capacity of this FloatBuffer. The capacity is the total amount of storage
   * currently available without expanding the array.
   *
   * @return the current capacity
   */
  int getCapacity() {
    return capacity_;
  }

  /**
   * Returns non-normalized rank of the given value.
   * This is the count of items less-than (or equal to) the given value.
   * @param value the given value
   * @param lteq the less-than or less-than or equal to criterion.
   * @return count of items less-than (or equal to) the given value.
   */
  int getCountLtOrEq(final float value, final boolean lteq) {
    if (!sorted_) { sort(); } //we must be sorted!
    final int index = ReqHelper.binarySearch(arr_, 0, count_ - 1, value, lteq);
    return (index == -1) ? 0 : index + 1;
  }

  /**
   * Returns an array of counts corresponding to each of the values in the given array.
   * The counts will be the number of values that are &lt; or &le; to the given values, depending on
   * the state of <i>lteq</i>.
   * @param values the given values array
   * @param lteq if true, the criterion for the counts.
   * @return an array of counts corresponding to each of the values in the given array
   */
  int[] getCountsLtOrEq(final float[] values, final boolean lteq) {
    final int len = values.length;
    final int[] nnrArr = new int[len];
    for (int i = 0; i < len; i++) {
      nnrArr[i] = getCountLtOrEq(values[i], lteq);
    }
    return nnrArr;
  }

  /**
   * Returns a sorted FloatBuffer of the even indicies from the range start (inclusive) to end (exclusive).
   * The even indicies are with respect to the start index, as if it were even.
   * If the starting index is odd with respect to the origin of the FloatBuffer, then this will
   * actually return the odd indicies with respect to the FloatBuffer origin.
   * @param start the starting index
   * @param end the end index, exclusive
   * @return the selected evens from the range
   */
  FloatBuffer getEvens(final int start, final int end) {
    sort();
    final int range = end - start;
    final int odd = range & 1;
    final int len = odd + (range / 2);
    final float[] out = new float[len];
    for (int i = start, j = 0; i < end; i += 2, j++) {
      out[j] = arr_[i];
    }
    return wrap(out, true);
  }

  /**
   * Gets an item given its index
   * @param index the given index
   * @return an item given its index
   */
  float getItem(final int index) {
    return arr_[index];
  }

  /**
   * Returns the item count.
   *
   * @return the number of active items currently in this array.
   */
  int getItemCount() {
    return count_;
  }

  /**
   * Returns the active length = item count.
   *
   * @return the active length of this buffer.
   */
  int getLength() {
    return count_;
  }

  /**
   * Returns a sorted FloatBuffer of the odd indicies from the range start (inclusive) to end (exclusive).
   * The odd indicies are with respect to the start index, as if it was even.
   * If the starting index is odd with respect to the origin of the FloatBuffer,
   * then this will actually return the even indicies with respect to the FloatBuffer origin.
   * @param start the starting index
   * @param end the end index, exclusive
   * @return the selected odds from the range
   */
  FloatBuffer getOdds(final int start, final int end) {
    sort();
    final int outLen = (end - start) / 2;
    final float[] out = new float[outLen];
    for (int i = start + 1, j = 0; i < end; i += 2, j++) {
      out[j] = arr_[i];
    }
    return wrap(out, true);
  }

  /**
   * Gets available space, which is getCapacity() - getLength().
   * @return available space
   */
  int getSpace() {
    return capacity_ - count_;
  }

  /**
   * Returns true if getLength() == 0.
   * @return true if getLength() == 0.
   */
  boolean isEmpty() {
    return count_ == 0;
  }

  /**
   * Returns true if this FloatBuffer is sorted.
   * @return true if sorted
   */
  boolean isSorted() {
    return sorted_;
  }

  /**
   * Merges the incoming sorted array into this sorted array.
   * @param bufIn sorted buffer in
   * @return this
   */
  FloatBuffer mergeSortIn(final FloatBuffer bufIn) {
    if (!sorted_ || !bufIn.isSorted()) {
      throw new SketchesArgumentException("Both buffers must be sorted.");
    }
    final float[] arrIn = bufIn.getArray(); //may be larger than its length.
    final int inLen = bufIn.getLength();
    ensureSpace(inLen);
    int i = count_;
    int j = inLen;
    for (int k = i-- + j--; k-- > 0; ) {
      if ((i >= 0) && (j >= 0)) { //both valid
        arr_[k] = (arr_[i] >= arrIn[j]) ? arr_[i--] : arrIn[j--];
      } else if (i >= 0) { //i is valid
        arr_[k] = arr_[i--];
      } else if (j >= 0) { //j is valid
        arr_[k] = arrIn[j--];
      } else {
        break;
      }
    }
    count_ += arrIn.length;
    sorted_ = true;
    return this;
  }

  /**
   * Sorts this array from 0 to length();
   * @return this
   */
  FloatBuffer sort() {
    if (!sorted_) {
      Arrays.sort(arr_, 0, count_);
      sorted_ = true;
    }
    return this;
  }

  /**
   * Returns a printable formatted string of the values of this buffer separated by a single space.
   * @param fmt The format for each printed item.
   * @param width the number of items to print per line
   * @param indent the number of spaces at the beginning of a new line
   * @return a printable, formatted string of the values of this buffer.
   */
  String toHorizList(final String fmt, final int width, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final char[] spaces = new char[indent];
    Arrays.fill(spaces, ' ');
    for (int i = 0; i < count_; i++) {
      final float v = arr_[i];
      final String str = String.format(fmt, v);
      if ((i > 0) && ((i % width) == 0)) { sb.append(LS).append(spaces); }
      sb.append(str);
    }
    return sb.toString();
  }

  /**
   * Trims the capacity of this FloatBuffer to length().
   * @return this
   */
  FloatBuffer trimCapacity() {
    if (count_ < arr_.length) {
      arr_ = Arrays.copyOf(arr_, count_);
      capacity_ = count_;
    }
    return this;
  }

  /**
   * Trims the length to newLength. If newLength &gt; length() this does nothing and returns.
   * Otherwise, the internal length is reduced to the given length. There is no clearing of
   * the remainder of the capacity. Any values there are considered garbage.
   *
   * @param newLength the new length
   * @return this
   */
  FloatBuffer trimLength(final int newLength) {
    if (newLength < count_) {
      count_ = newLength;
    }
    return this;
  }
}
