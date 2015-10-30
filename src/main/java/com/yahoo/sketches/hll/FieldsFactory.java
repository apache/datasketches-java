package com.yahoo.sketches.hll;

/**
 * A Factory for a fields object.  This was created abstract which fields to create from the sparse representations.
 * They can just depend on this object to make the next Fields that they return when the sparse representation is no
 * longer considered good enough.
 */
interface FieldsFactory
{
  /**
   * Makes a new Fields object using the given preamble
   *
   * @param preamble The preamble to use to make the Fields object
   * @return a new, clean Fields object given the preamble
   */
  Fields make(Preamble preamble);
}
