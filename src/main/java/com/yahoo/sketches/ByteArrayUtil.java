package com.yahoo.sketches;

public final class ByteArrayUtil {

  /**
   * Put a given short value into a given byte array at a given offset.
   * Assumes little-endian byte order.
   * @param bytes destination byte array
   * @param offset destination offset
   * @param value source value
   */
  public static void putShort(final byte[] bytes, final int offset, final short value) {
    for (int i = 0; i < Short.BYTES; i++) {
      bytes[offset + i] = (byte) ((value >>> (8 * i)) & 0xff);
    }
  }

  /**
   * Put a given integer value into a given byte array at a given offset.
   * Assumes little-endian byte order.
   * @param bytes destination byte array
   * @param offset destination offset
   * @param value source value
   */
  public static void putInt(final byte[] bytes, final int offset, final int value) {
    for (int i = 0; i < Integer.BYTES; i++) {
      bytes[offset + i] = (byte) ((value >>> (8 * i)) & 0xff);
    }
  }

  /**
   * Put a given long value into a given byte array at a given offset.
   * Assumes little-endian byte order.
   * @param bytes destination byte array
   * @param offset destination offset
   * @param value source value
   */
  public static void putLong(final byte[] bytes, final int offset, final long value) {
    for (int i = 0; i < Long.BYTES; i++) {
      bytes[offset + i] = (byte) ((value >>> (8 * i)) & 0xff);
    }
  }

  /**
   * Put a given float value into a given byte array at a given offset.
   * Assumes little-endian byte order.
   * @param bytes destination byte array
   * @param offset destination offset
   * @param value source value
   */
  public static void putFloat(final byte[] bytes, final int offset, final float value) {
    putInt(bytes, offset, Float.floatToRawIntBits(value));
  }

  /**
   * Put a given double value into a given byte array at a given offset.
   * Assumes little-endian byte order.
   * @param bytes destination byte array
   * @param offset destination offset
   * @param value source value
   */
  public static void putDouble(final byte[] bytes, final int offset, final double value) {
    putLong(bytes, offset, Double.doubleToLongBits(value));
  }

}
