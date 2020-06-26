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

package org.apache.datasketches.theta;

import static org.apache.datasketches.theta.PreambleUtil.checkMemorySeedHash;

import org.apache.datasketches.memory.Memory;

/**
 * An off-heap (Direct), compact, ordered, read-only sketch.  This sketch can only be associated
 * with a Serialization Version 3 format binary image.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 */
final class DirectCompactOrderedSketch extends DirectCompactSketch {

  DirectCompactOrderedSketch(final Memory mem) {
    super(mem);
  }

  /**
   * Wraps the given Memory, which must be a SerVer 3, ordered, Compact Sketch image.
   * Must check the validity of the Memory before calling.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed The update seed.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return this sketch
   */
  static DirectCompactOrderedSketch wrapInstance(final Memory srcMem, final long seed) {
    checkMemorySeedHash(srcMem, seed);
    return new DirectCompactOrderedSketch(srcMem);
  }

  @Override
  public boolean isOrdered() {
    return true;
  }

}
