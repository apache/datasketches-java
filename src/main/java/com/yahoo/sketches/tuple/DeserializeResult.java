/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * Returns an object and its size in bytes as a result of a deserialize operation
 * @param <T> Type of object
 */
public class DeserializeResult<T> {
  private final T object;
  private final int size;

  /**
   * Creates an instance.
   * @param object Deserialized object.
   * @param size Deserialized size in bytes.
   */
  public DeserializeResult(final T object, final int size) {
    this.object = object;
    this.size = size;
  }

  /**
   * @return Deserialized object
   */
  public T getObject() {
    return object;
  }

  /**
   * @return Size in bytes occupied by the object in the serialized form
   */
  public int getSize() {
    return size;
  }
}
