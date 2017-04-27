/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 */
final class DenseCompressedFieldsFactory implements FieldsFactory {

  @Override
  public Fields make(final Preamble preamble) {
    return new OnHeapCompressedFields(preamble);
  }

  @Override
  public int intoByteArray(final byte[] bytes, final int offset) {
    bytes[offset] = FieldsFactories.DENSE_COMPRESSED;
    return offset + 1;
  }

  @Override
  public int numBytesToSerialize() {
    return 1;
  }
}
