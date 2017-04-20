package com.yahoo.sketches.hll;

/**
 */
public class FieldsFactories
{
  public static final byte DENSE = 0x0; //strategy: promote to dense
  public static final byte DENSE_COMPRESSED = 0x1; //strategy: promote to compressed

  @SuppressWarnings("unused")
  static FieldsFactory fromBytes(final byte[] bytes, final int offset, final int endOffset) {
    final byte version = bytes[offset];

    switch (version) {
      case DENSE:
        return new DenseFieldsFactory();
      case DENSE_COMPRESSED:
        return new DenseCompressedFieldsFactory();
      default:
        throw new IllegalStateException(String.format("Unknown FieldsFactory version[%s]", version));
    }
  }
}
