/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * @author Kevin Lang
 */
class DenseCompressedFieldsFactory implements FieldsFactory {
  
  @Override
  public Fields make(Preamble preamble) {
    return new OnHeapCompressedFields(preamble);
  }
}
