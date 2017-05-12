package com.yahoo.sketches;

public final class ByteArrayUtil {

  public static void putInt(final byte[] bytes, final int offset, final int value) {
    for (int i = 0; i < Integer.BYTES; i++) {
      bytes[offset + i] = (byte) ((value >> 8 * i) & 0xff);
    }
  }

  public static void putLong(final byte[] bytes, final int offset, final long value) {
    for (int i = 0; i < Long.BYTES; i++) {
      bytes[offset + i] = (byte) ((value >> 8 * i) & 0xff);
    }
  }

  public static void putFloat(final byte[] bytes, final int offset, final float value) {
    putInt(bytes, offset, Float.floatToRawIntBits(value));
  }

  public static void putDouble(final byte[] bytes, final int offset, final double value) {
    putLong(bytes, offset, Double.doubleToLongBits(value));
  }

}
