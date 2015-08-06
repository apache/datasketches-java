package com.yahoo.sketches.hll;

public class PreambleFlags
{
  static final int EMPTY_FLAG_MASK = 2;
  static final int UNION_MODE_FLAG_MASK = 4;
  static final int EIGHT_BYTE_PADDING_FLAG_MASK = 8;
  static final int BIG_ENDIAN_FLAG_MASK = 16;
  static final int READ_ONLY_FLAG_MASK = 32;  // set but not read. Reserve for future
  static final int SHARED_PREAMBLE_FLAG_MASK = 64;
  static final int SPARSE_MODE_FLAG_MASK = 1;

  public static byte setAllFlags(byte flagsByte, boolean isSparseMode, boolean isUnionMode, boolean isEmpty, boolean isEightBytePadding, boolean isBigEndian, boolean isReadOnly, boolean isSharedPreambleMode) {

    flagsByte = initFlag(flagsByte, isSparseMode, SPARSE_MODE_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isEmpty, EMPTY_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isUnionMode, UNION_MODE_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isEightBytePadding, EIGHT_BYTE_PADDING_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isBigEndian, BIG_ENDIAN_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isReadOnly, READ_ONLY_FLAG_MASK);
    flagsByte = initFlag(flagsByte, isSharedPreambleMode, SHARED_PREAMBLE_FLAG_MASK);
    return flagsByte;
  }

  public static boolean isUnionMode(byte flagsByte) {
    return isFlagSet(flagsByte, UNION_MODE_FLAG_MASK);
  }

  public static byte setUnionMode(byte flagsByte, boolean isUnionMode) {
    return initFlag(flagsByte, isUnionMode, UNION_MODE_FLAG_MASK);
  }

  public static boolean isBigEndian(byte flagsByte) {
    return isFlagSet(flagsByte, BIG_ENDIAN_FLAG_MASK);
  }

  public static byte setBigEndian(byte flagsByte, boolean isBigEndian) {
    return initFlag(flagsByte, isBigEndian, BIG_ENDIAN_FLAG_MASK);
  }

  public static boolean isReadOnly(byte flagsByte) {
    return isFlagSet(flagsByte, READ_ONLY_FLAG_MASK);
  }

  public static byte setReadOnly(byte flagsByte, boolean isReadOnly) {
    return initFlag(flagsByte, isReadOnly, READ_ONLY_FLAG_MASK);
  }

  public static boolean isSharedPreambleMode(byte flagsByte) {
    return isFlagSet(flagsByte, SHARED_PREAMBLE_FLAG_MASK);
  }

  public static byte setSharedPreambleMode(byte flagsByte, boolean isSharedPreambleMode) {
    return initFlag(flagsByte, isSharedPreambleMode, SHARED_PREAMBLE_FLAG_MASK);
  }

  public static boolean isEmpty(byte flagsByte) {
    return isFlagSet(flagsByte, EMPTY_FLAG_MASK);
  }

  public static byte setEmpty(byte flagsByte, boolean isEmpty) {
    return initFlag(flagsByte, isEmpty, EMPTY_FLAG_MASK);
  }

  public static byte setEightBytePadding(byte flagsByte, boolean hasEightBytePadding) {
    return initFlag(flagsByte, hasEightBytePadding, EIGHT_BYTE_PADDING_FLAG_MASK);
  }

  public static boolean isEightBytePadding(byte flagsByte) {
    return isFlagSet(flagsByte, EIGHT_BYTE_PADDING_FLAG_MASK);
  }

  public static boolean isSparseMode(byte flagsByte) {

    return isFlagSet(flagsByte, SPARSE_MODE_FLAG_MASK);
  }

  public static byte setSparseMode(byte flagsByte, boolean isSparseMode) {
    return initFlag(flagsByte, isSparseMode, SPARSE_MODE_FLAG_MASK);
  }

  private static boolean isFlagSet(byte flagsByte, int mask) {

    return (mask & flagsByte) != 0;
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
      flags = PreambleFlags.setAllFlags(flags, isSparseMode, isUnionMode, isEmpty, isEightBytePadding, isBigEndian, isReadOnly, isSharedPreambleMode);
      return flags;
    }

  }

}
