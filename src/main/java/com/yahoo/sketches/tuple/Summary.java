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
   * @param <S> type of summary
   * @return copy of the Summary
   */
  public <S extends Summary> S copy();

  /**
   * This is to serialize a Summary instance to a byte array.
   *
   * <p>The user should encode in the byte array its total size, which is used during
   * deserialization, especially if the Summary has variable sized elements.
   * To deserialize there must be a static method of the form:
   * <pre><code>
   * public static DeserializeResult&lt;T&gt; fromMemory(Memory mem) { ... }
   * </code></pre>
   * The user may assume that the start of the given Memory is the correct place to start
   * deserializing. However, the user must be able to determine the number of bytes required to
   * deserialize the summary as the capacity of the given Memory may
   * include multiple such summaries and may be much larger than required for a single summary.
   *
   * @return serialized representation of the Summary
   */
  public byte[] toByteArray();

}
