/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple;

/**
 * Interface for user-defined Summary, which is associated with every hash in a tuple sketch
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
