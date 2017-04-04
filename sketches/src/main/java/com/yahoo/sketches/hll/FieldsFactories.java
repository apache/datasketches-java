package com.yahoo.sketches.hll;

/**
 */
public class FieldsFactories
{
  public static final byte DENSE = 0x0;
  public static final byte DENSE_COMPRESSED = 0x1;

  static FieldsFactory fromBytes(byte[] bytes, int offset, int endOffset) {
    final byte version = bytes[offset];

    switch(version) {
      case DENSE:
        return new DenseFieldsFactory();
      case DENSE_COMPRESSED:
        return new DenseCompressedFieldsFactory();
      default:
        throw new IllegalStateException(String.format("Unknown FieldsFactory version[%s]", version));
    }
  }
}
