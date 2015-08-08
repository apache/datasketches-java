package com.yahoo.sketches.hll;

/**
 */
class DenseCompressedFieldsFactory implements FieldsFactory
{
  @Override
  public Fields make(Preamble preamble)
  {
    return new OnHeapCompressedFields(preamble);
  }
}
