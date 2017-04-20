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

  static Fields fromBytes(Preamble preamble, final byte[] bytes) {
    return fromBytes(preamble, bytes, 0, bytes.length);
  }

  static Fields fromBytes(Preamble preamble, final byte[] bytes, final int startOffset, final int endOffset) {
    final Fields fields;
    switch (bytes[startOffset]) {
      case Fields.NAIVE_DENSE_VERSION:
        fields = OnHeapFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      case Fields.HASH_SPARSE_VERSION:
        fields = OnHeapHashFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      case Fields.SORTED_SPARSE_VERSION:
        fields = OnHeapImmutableCompactFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      case Fields.COMPRESSED_DENSE_VERSION:
        fields = OnHeapCompressedFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown field type[%d]", bytes[startOffset]));
    }
    return fields;
  }
}
