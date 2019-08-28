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

package org.apache.datasketches.hll;

/**
 * Represents the three fundamental modes of the HLL Sketch.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
enum CurMode { LIST, SET, HLL; //do not change the order.

  public static final CurMode values[] = values();

  /**
   * Returns the CurMode given its ordinal
   * @param ordinal the order of appearance in the enum definition.
   * @return the CurMode given its ordinal
   */
  public static CurMode fromOrdinal(final int ordinal) {
    return values[ordinal];
  }


}
