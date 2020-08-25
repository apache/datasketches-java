/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package org.apache.datasketches.req;

import java.util.Arrays;

import org.apache.datasketches.SketchesArgumentException;

/**
 * A special buffer of floats.
 *
 * @author Lee Rhodes
 */
class Buffer {
  static final String LS = System.getProperty("line.separator");
  private float[] arr_;
  private int count_;
  private int delta_;
  private int capacity_;
  private boolean sorted_;

  /**
   * Constructs a new Buffer with a default size of 1024 items.
   */
  Buffer() {
    this(1024, 256);
  }

  /**
   * Constructs an empty Buffer with an initial capacity specified by
   * the <code>capacity</code> argument.
   *
   * @param capacity the initial capacity.
   * @param delta add space in increments of this size
   */
  Buffer(final int capacity, final int delta) {
    arr_ = new float[capacity];
    count_ = 0;
    delta_ = delta;
    capacity_ = capacity;
    sorted_ = true;
  }

  /**
   * Copy Constructor
   * @param buf the Buffer to be copied into this one
   */
  Buffer(final Buffer buf) {
    arr_ = buf.arr_.clone();
    count_ = buf.count_;
    delta_ = buf.delta_;
    capacity_ = buf.capacity_;
    sorted_ = buf.sorted_;
  }

  /**
   * Appends the given item to the end of the active array and increments length().
   * This will expand the array if necessary.
   * @param item the given item
   * @return this
   */
  Buffer append(final float item) {
    ensureSpace(1);
    arr_[count_++] = item;
    sorted_ = false;
    return this;
  }

  /**
   * Returns count of items less-than the given value.
   * @param value the given value
   * @return count of items less-than the given value.
   */
  int countLessThan(final float value) {
    int num = 0;
    if (sorted_) {
      for (int i = 0; i < count_; i++) {
        if (arr_[i] < value) { num++; }
        else { break; }
      }
    } else {
      for (int i = 0; i < count_; i++) {
        if (arr_[i] < value) { num++; }
      }
    }
    return num;
  }

  /**
   * Ensures that the capacity of this Buffer is at least newCapacity.
   * If newCapacity &lt; capacity(), no action is taken.
   * @return this
   */
  private Buffer ensureCapacity(final int newCapacity) {
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
  private Buffer ensureSpace(final int space) {
    if ((count_ + space) > arr_.length) {
      ensureCapacity(count_ + space + delta_);
    }
    return this;
  }

  /**
   * Extends the given item array starting at length(). This will expand this Buffer if necessary.
   * This buffer becomes unsorted after this operation.
   * @param floatArray the given item array
   * @return this Buffer
   */
  Buffer extend(final float[] floatArray) {
    final int len = floatArray.length;
    ensureSpace(len);
    System.arraycopy(floatArray, 0, arr_, count_, len);
    count_ += len;
    sorted_ = false;
    return this;
  }


  /**
   * Append other buffer to this buffer. Any items beyond other.length() are ignored.
   * This will expand this Buffer if necessary.
   * This buffer becomes unsorted after this operation.
   * @param other the other buffer
   * @return this
   */
  Buffer extend(final Buffer other) { //may not need this
    final int len = other.getItemCount();
    ensureSpace(len);
    System.arraycopy(other.getArray(), 0, arr_, getItemCount(), len);
    count_ += len;
    sorted_ = false;
    return this;
  }

  /**
   * Returns a reference to the internal item array.
   * @return the internal item array.
   */
  float[] getArray() {
    return arr_;
  }

  /**
   * Gets the current capacity of this Buffer. The capacity is the total amount of storage
   * currently available without expanding the array.
   *
   * @return the current capacity
   */
  int getCapacity() {
    return capacity_;
  }

  /**
   * Returns an array of the even values from the range start (inclusive) to end (exclusive).
   * The even values are with respect to the start index. If the starting index is odd with
   * respect to the origin of the Buffer, then this will actually return the odd values.
   * @param start the starting index
   * @param end the end index, exclusive
   * @return the selected evens from the range
   */
  float[] getEvens(final int start, final int end) {
    final int range = end - start;
    final int odd = range & 1;
    final int len = odd + (range / 2);
    final float[] out = new float[len];
    for (int i = start, j = 0; i < end; i += 2, j++) {
      out[j] = arr_[i];
    }
    return out;
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
   * Returns an array of the odd values from the range start (inclusive) to end (exclusive).
   * The odd values are with respect to the start index. If the starting index is odd with
   * respect to the origin of the Buffer, then this will actually return the even values.
   * @param start the starting index
   * @param end the end index, exclusive
   * @return the selected odds from the range
   */
  float[] getOdds(final int start, final int end) {
    final int outLen = (end - start) / 2;
    final float[] out = new float[outLen];
    for (int i = start + 1, j = 0; i < end; i += 2, j++) {
      out[j] = arr_[i];
    }
    return out;
  }

  boolean isEmpty() {
    return count_ == 0;
  }

  /**
   * Returns true if this Buffer is sorted.
   * @return true if sorted
   */
  boolean isSorted() {
    return sorted_;
  }

  /**
   * Merges the incoming sorted array into this sorted array.
   * @param arrIn sorted array in
   * @return this
   */
  Buffer mergeSortIn(final float[] arrIn) {
    if (!sorted_) {
      throw new SketchesArgumentException("Must be sorted.");
    }
    ensureSpace(arrIn.length);
    int i = count_;
    int j = arrIn.length;
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
  Buffer sort() {
    Arrays.sort(arr_, 0, count_);
    sorted_ = true;
    return this;
  }

  /**
   * Returns a printable formatted string of the values of this buffer separated by a single space.
   * @param decimals The desired precision after the decimal point
   * @return a printable, formatted string of the values of this buffer.
   */
  String toHorizList(final int decimals) {
    final StringBuilder sb = new StringBuilder();
    final String fmt = " %." + decimals + "f";
    for (int i = 0; i < count_; i++) {
      final String str = String.format(fmt, arr_[i]);
      sb.append(str);
    }
    return sb.toString();
  }

  /**
   * Trims the capacity of this Buffer to length().
   * @return this
   */
  Buffer trimCapacity() {
    if (count_ < arr_.length) {
      arr_ = Arrays.copyOf(arr_, count_);
      capacity_ = count_;
    }
    return this;
  }

  /**
   * Trims the length to newLength. If newLength &gt; length() this does nothing and returns. If
   * newLength is &lt; length() this clears all values between newLength and length() and resets
   * length() to the newLength.
   *
   * @param newLength the new length
   * @return this
   */
  Buffer trimLength(final int newLength) {
    if (newLength < count_) {
      for (int i = newLength; i < count_; i++) {
        arr_[i] = 0;
      }
      count_ = newLength;
    }
    return this;
  }
}
