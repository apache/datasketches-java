/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

/**
 * Interface for user-defined Summary, which is associated with every key in a tuple sketch
 */
public interface Summary {

  /**
   * @return copy of the Summary
   */
  public <S extends Summary> S copy();

  /**
   * This is to serialize an instance to a byte array.
   * For deserialization there must be a static method
   * DeserializeResult<T> fromMemory(Memory mem)
   * @return serialized representation of the Summary
   */
  public byte[] toByteArray();

}
