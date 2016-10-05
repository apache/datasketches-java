/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * @author Kevin Lang
 */
public final class PreambleFlags {
  static final int EMPTY_FLAG_MASK = 2;
  static final int UNION_MODE_FLAG_MASK = 4;
  static final int EIGHT_BYTE_PADDING_FLAG_MASK = 8;
  static final int BIG_ENDIAN_FLAG_MASK = 16;
  static final int READ_ONLY_FLAG_MASK = 32;  // set but not read. Reserve for future
  static final int SHARED_PREAMBLE_FLAG_MASK = 64;
  static final int SPARSE_MODE_FLAG_MASK = 1;

  private PreambleFlags() {}

  /**
   * Sets the flags of the flags byte
   * @param flagsByte the given byte to initialize
   * @param isSparseMode the state of Sparse Mode
   * @param isUnionMode the state of Union Mode
   * @param isEmpty the state of Empty
   * @param isEightBytePadding the state of Eight Byte Padding
   * @param isBigEndian the state of Big Endian
   * @param isReadOnly the state of Read Only
   * @param isSharedPreambleMode the state of Shared Preamble Mode
   * @return the resulting flags byte
   */
  public static byte setAllFlags(byte flagsByte, boolean isSparseMode, boolean isUnionMode,
      boolean isEmpty, boolean isEightBytePadding, boolean isBigEndian, boolean isReadOnly,
      boolean isSharedPreambleMode) {

    flagsByte = initFlag(flagsByte, isSparseMode, SPARSE_MODE_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isEmpty, EMPTY_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isUnionMode, UNION_MODE_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isEightBytePadding, EIGHT_BYTE_PADDING_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isBigEndian, BIG_ENDIAN_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isReadOnly, READ_ONLY_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isSharedPreambleMode, SHARED_PREAMBLE_FLAG_MASK);
    return flagsByte;
  }

  private static byte initFlag(byte flagsByte, boolean isSet, int mask) {
    if (isSet) {
      return (byte) (flagsByte | mask);
    } else {
      return (byte) (flagsByte & (mask ^ 0xff));
    }
  }

  /**
   * The Builder for the Flags byte
   */
  public static class Builder {
    private boolean isBigEndian = false;
    private boolean isReadOnly = false;
    private boolean isEmpty = true;
    private boolean isSharedPreambleMode = false;
    private boolean isSparseMode = true;
    private boolean isUnionMode = false;
    private boolean isEightBytePadding = false;

    /**
     * Sets the Endianness state
     * @param isBigEndian true if Big Endian
     * @return this Builder
     */
    public Builder setBigEndian(boolean isBigEndian) {
      this.isBigEndian = isBigEndian;
      return this;
    }

    /**
     * Sets the Read Only state
     * @param isReadOnly true if Read Only
     * @return this Builder
     */
    public Builder setReadOnly(boolean isReadOnly) {
      this.isReadOnly = isReadOnly;
      return this;
    }

    /**
     * Sets the Empty state
     * @param isEmpty true if Empty
     * @return this Builder
     */
    public Builder setEmpty(boolean isEmpty) {
      this.isEmpty = isEmpty;
      return this;
    }

    /**
     * Sets the Shared Preamble Mode
     * @param isSharedPreambleMode true if using a Shared Preamble
     * @return this Builder
     */
    public Builder setSharedPreambleMode(boolean isSharedPreambleMode) {
      this.isSharedPreambleMode = isSharedPreambleMode;
      return this;
    }

    /**
     * Sets the Sparse Mode
     * @param isSparseMode true if using Sparse Mode
     * @return this Builder
     */
    public Builder setSparseMode(boolean isSparseMode) {
      this.isSparseMode = isSparseMode;
      return this;
    }

    /**
     * Sets the Union Mode
     * @param isUnionMode true if in Union Mode
     * @return this Builder
     */
    public Builder setUnionMode(boolean isUnionMode) {
      this.isUnionMode = isUnionMode;
      return this;
    }

    /**
     * Sets the Eight-Byte-Padding state
     * @param isEightBytePadding true if using Eight-Byte-Padding
     * @return this Builder
     */
    public Builder setEightBytePadding(boolean isEightBytePadding) {
      this.isEightBytePadding = isEightBytePadding;
      return this;
    }

    /**
     * Build a Preamble Flags byte
     * @return the flags byte
     */
    public byte build() {
      byte flags = 0;
      flags = PreambleFlags.setAllFlags(flags, isSparseMode, isUnionMode, isEmpty,
          isEightBytePadding, isBigEndian, isReadOnly, isSharedPreambleMode);
      return flags;
    }

  } //End builder

}
