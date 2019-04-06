/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * Interface for user-defined Summary, which is associated with every key in a tuple sketch
 */
public interface Summary {

  /**
   * Deep copy.
   *
   * <p><b>Caution:</b> This must implement a deep copy.
   *
   * @return deep copy of the Summary
   */
  public Summary copy();

  /**
   * This is to serialize a Summary instance to a byte array.
   *
   * <p>The user should encode in the byte array its total size, which is used during
   * deserialization, especially if the Summary has variable sized elements.
   *
   * @return serialized representation of the Summary
   */
  public byte[] toByteArray();

}
