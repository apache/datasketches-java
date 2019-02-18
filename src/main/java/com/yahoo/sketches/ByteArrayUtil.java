package com.yahoo.sketches;

public final class ByteArrayUtil {

  /**
   * Get a <i>short</i> from the given byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>short</i>
   */
  public static short getShortLE(final byte[] array, final int offset) {
    return (short) ((array[offset    ] & 0XFF       )
                 | ((array[offset + 1] & 0XFF) <<  8));
  }

  /**
   * Put the source <i>short</i> into the destination byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>short</i>
   */
  public static void putShortLE(final byte[] array, final int offset, final short value) {
    array[offset    ] = (byte) (value       );
    array[offset + 1] = (byte) (value >>>  8);
  }

  /**
   * Get a <i>short</i> from the given byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>short</i>
   */
  public static short getShortBE(final byte[] array, final int offset) {
    return (short) ((array[offset + 1] & 0XFF       )
                 | ((array[offset    ] & 0XFF) <<  8));
  }

  /**
   * Put the source <i>short</i> into the destination byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>short</i>
   */
  public static void putShortBE(final byte[] array, final int offset, final short value) {
    array[offset + 1] = (byte) (value       );
    array[offset    ] = (byte) (value >>>  8);
  }

  /**
   * Get a <i>int</i> from the given byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>int</i>
   */
  public static int getIntLE(final byte[] array, final int offset) {
    return  ( array[offset    ] & 0XFF       )
          | ((array[offset + 1] & 0XFF) <<  8)
          | ((array[offset + 2] & 0XFF) << 16)
          | ((array[offset + 3] & 0XFF) << 24);
  }

  /**
   * Put the source <i>int</i> into the destination byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>int</i>
   */
  public static void putIntLE(final byte[] array, final int offset, final int value) {
    array[offset    ] = (byte) (value       );
    array[offset + 1] = (byte) (value >>>  8);
    array[offset + 2] = (byte) (value >>> 16);
    array[offset + 3] = (byte) (value >>> 24);
  }

  /**
   * Get a <i>int</i> from the given byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>int</i>
   */
  public static int getIntBE(final byte[] array, final int offset) {
    return  ( array[offset + 3] & 0XFF       )
          | ((array[offset + 2] & 0XFF) <<  8)
          | ((array[offset + 1] & 0XFF) << 16)
          | ((array[offset    ] & 0XFF) << 24);
  }

  /**
   * Put the source <i>int</i> into the destination byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>int</i>
   */
  public static void putIntBE(final byte[] array, final int offset, final int value) {
    array[offset + 3] = (byte) (value       );
    array[offset + 2] = (byte) (value >>>  8);
    array[offset + 1] = (byte) (value >>> 16);
    array[offset    ] = (byte) (value >>> 24);
  }

  /**
   * Get a <i>long</i> from the given byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>long</i>
   */
  public static long getLongLE(final byte[] array, final int offset) {
    return  ( array[offset    ] & 0XFFL       )
          | ((array[offset + 1] & 0XFFL) <<  8)
          | ((array[offset + 2] & 0XFFL) << 16)
          | ((array[offset + 3] & 0XFFL) << 24)
          | ((array[offset + 4] & 0XFFL) << 32)
          | ((array[offset + 5] & 0XFFL) << 40)
          | ((array[offset + 6] & 0XFFL) << 48)
          | ((array[offset + 7] & 0XFFL) << 56);
  }

  /**
   * Put the source <i>long</i> into the destination byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>long</i>
   */
  public static void putLongLE(final byte[] array, final int offset, final long value) {
    array[offset    ] = (byte) (value       );
    array[offset + 1] = (byte) (value >>>  8);
    array[offset + 2] = (byte) (value >>> 16);
    array[offset + 3] = (byte) (value >>> 24);
    array[offset + 4] = (byte) (value >>> 32);
    array[offset + 5] = (byte) (value >>> 40);
    array[offset + 6] = (byte) (value >>> 48);
    array[offset + 7] = (byte) (value >>> 56);
  }

  /**
   * Get a <i>long</i> from the source byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source starting point
   * @return the <i>long</i>
   */
  public static long getLongBE(final byte[] array, final int offset) {
    return  ( array[offset + 7] & 0XFFL       )
          | ((array[offset + 6] & 0XFFL) <<  8)
          | ((array[offset + 5] & 0XFFL) << 16)
          | ((array[offset + 4] & 0XFFL) << 24)
          | ((array[offset + 3] & 0XFFL) << 32)
          | ((array[offset + 2] & 0XFFL) << 40)
          | ((array[offset + 1] & 0XFFL) << 48)
          | ((array[offset    ] & 0XFFL) << 56);
  }

  /**
   * Put the source <i>long</i> into the destination byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination starting point
   * @param value source <i>long</i>
   */
  public static void putLongBE(final byte[] array, final int offset, final long value) {
    array[offset + 7] = (byte) (value       );
    array[offset + 6] = (byte) (value >>>  8);
    array[offset + 5] = (byte) (value >>> 16);
    array[offset + 4] = (byte) (value >>> 24);
    array[offset + 3] = (byte) (value >>> 32);
    array[offset + 2] = (byte) (value >>> 40);
    array[offset + 1] = (byte) (value >>> 48);
    array[offset    ] = (byte) (value >>> 56);
  }

  /**
   * Get a <i>float</i> from the given byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>float</i>
   */
  public static float getFloatLE(final byte[] array, final int offset) {
    return Float.intBitsToFloat(getIntLE(array, offset));
  }

  /**
   * Put the source <i>float</i> into the destination byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>float</i>
   */
  public static void putFloatLE(final byte[] array, final int offset, final float value) {
    putIntLE(array, offset, Float.floatToRawIntBits(value));
  }

  /**
   * Get a <i>float</i> from the given byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>float</i>
   */
  public static float getFloatBE(final byte[] array, final int offset) {
    return Float.intBitsToFloat(getIntBE(array, offset));
  }

  /**
   * Put the source <i>float</i> into the destination byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>float</i>
   */
  public static void putFloatBE(final byte[] array, final int offset, final float value) {
    putIntBE(array, offset, Float.floatToRawIntBits(value));
  }

  /**
   * Get a <i>double</i> from the given byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>double</i>
   */
  public static double getDoubleLE(final byte[] array, final int offset) {
    return Double.longBitsToDouble(getLongLE(array, offset));
  }

  /**
   * Put the source <i>double</i> into the destination byte array starting at the given offset
   * in little endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>double</i>
   */
  public static void putDoubleLE(final byte[] array, final int offset, final double value) {
    putLongLE(array, offset, Double.doubleToRawLongBits(value));
  }

  /**
   * Get a <i>double</i> from the given byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array source byte array
   * @param offset source offset
   * @return the <i>double</i>
   */
  public static double getDoubleBE(final byte[] array, final int offset) {
    return Double.longBitsToDouble(getLongBE(array, offset));
  }

  /**
   * Put the source <i>double</i> into the destination byte array starting at the given offset
   * in big endian order.
   * There is no bounds checking.
   * @param array destination byte array
   * @param offset destination offset
   * @param value source <i>double</i>
   */
  public static void putDoubleBE(final byte[] array, final int offset, final double value) {
    putLongBE(array, offset, Double.doubleToRawLongBits(value));
  }

}
