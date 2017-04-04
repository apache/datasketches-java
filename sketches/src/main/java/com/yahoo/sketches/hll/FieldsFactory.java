/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * A Factory for a fields object.  This was created abstract which fields to create from the sparse
 * representations. They can just depend on this object to make the next Fields that they return
 * when the sparse representation is no longer considered good enough.
 *
 * @author Kevin Lang
 */
interface FieldsFactory {

  /**
   * Makes a new Fields object using the given preamble
   *
   * @param preamble The preamble to use to make the Fields object
   * @return a new, clean Fields object given the preamble
   */
  Fields make(Preamble preamble);

  /**
   * Fills the array starting from offset with the byte array representation of the fields
   *
   * <p>This should *not* include the preamble
   * @param bytes given array to fill
   * @param offset starting with this offset
   * @return the last offset written +1
   */
  int intoByteArray(byte[] bytes, int offset);

  /**
   * Provides an indication of how many bytes would be required to serialize this object to
   * a byte[].
   *
   * @return the number of bytes to serialize this object to a byte[]
   */
  int numBytesToSerialize();
}
