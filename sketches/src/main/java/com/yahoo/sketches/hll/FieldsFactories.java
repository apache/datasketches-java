package com.yahoo.sketches.hll;

/**
 */
public class FieldsFactories {
  public static final byte DENSE = 0x0; //strategy bit: promote to dense
  public static final byte DENSE_COMPRESSED = 0x1; //strategy bit: promote to compressed

  static FieldsFactory fromBytes( //called from OnHeapHashFields
      final byte[] bytes,
      final int offset,
      @SuppressWarnings("unused") final int endOffset) {
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

  static Fields fromBytes( //only called from test
      final Preamble preamble,
      final byte[] bytes) {
    return fromBytes(preamble, bytes, 0, bytes.length);
  }

  static Fields fromBytes( //also called from HllSketch
      final Preamble preamble,
      final byte[] bytes,
      final int startOffset,
      final int endOffset) {
    final Fields fields;
    switch (Fields.Version.idToVersion(bytes[startOffset])) {
      case NAIVE_DENSE_VERSION:
        fields = OnHeapFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      case HASH_SPARSE_VERSION:
        fields = OnHeapHashFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      case SORTED_SPARSE_VERSION:
        fields = OnHeapImmutableCompactFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      case COMPRESSED_DENSE_VERSION:
        fields = OnHeapCompressedFields.fromBytes(preamble, bytes, startOffset, endOffset);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown field type[%d]", bytes[startOffset]));
    }
    return fields;
  }
}
