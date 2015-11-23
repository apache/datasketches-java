/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

/**
 * @author Eric Tschetter
 * @author Kevin Lang
 */
public class PreambleFlags {
  static final int EMPTY_FLAG_MASK = 2;
  static final int UNION_MODE_FLAG_MASK = 4;
  static final int EIGHT_BYTE_PADDING_FLAG_MASK = 8;
  static final int BIG_ENDIAN_FLAG_MASK = 16;
  static final int READ_ONLY_FLAG_MASK = 32;  // set but not read. Reserve for future
  static final int SHARED_PREAMBLE_FLAG_MASK = 64;
  static final int SPARSE_MODE_FLAG_MASK = 1;

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


  public static class Builder {
    private boolean isBigEndian = false;
    private boolean isReadOnly = false;
    private boolean isEmpty = true;
    private boolean isSharedPreambleMode = false;
    private boolean isSparseMode = true;
    private boolean isUnionMode = false;
    private boolean isEightBytePadding = false;

    public Builder setBigEndian(boolean isBigEndian) {
      this.isBigEndian = isBigEndian;
      return this;
    }

    public Builder setReadOnly(boolean isReadOnly) {
      this.isReadOnly = isReadOnly;
      return this;
    }

    public Builder setEmpty(boolean isEmpty) {
      this.isEmpty = isEmpty;
      return this;
    }

    public Builder setSharedPreambleMode(boolean isSharedPreambleMode) {
      this.isSharedPreambleMode = isSharedPreambleMode;
      return this;
    }

    public Builder setSparseMode(boolean isSparseMode) {
      this.isSparseMode = isSparseMode;
      return this;
    }

    public Builder setUnionMode(boolean isUnionMode) {
      this.isUnionMode = isUnionMode;
      return this;
    }

    public Builder setEightBytePadding(boolean isEightBytePadding) {
      this.isEightBytePadding = isEightBytePadding;
      return this;
    }

    public byte build() {
      byte flags = 0;
      flags = PreambleFlags.setAllFlags(flags, isSparseMode, isUnionMode, isEmpty, 
          isEightBytePadding, isBigEndian, isReadOnly, isSharedPreambleMode);
      return flags;
    }

  } //End builder

}
