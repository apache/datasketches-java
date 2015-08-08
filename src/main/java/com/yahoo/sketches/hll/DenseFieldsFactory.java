package com.yahoo.sketches.hll;

/**
 */
class DenseFieldsFactory implements FieldsFactory
{
  @Override
  public Fields make(Preamble preamble)
  {
    return new OnHeapFields(preamble);
  }
}
