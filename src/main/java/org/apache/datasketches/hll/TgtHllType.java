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
 * Specifies the target type of HLL sketch to be created. It is a target in that the actual
 * allocation of the HLL array is deferred until sufficient number of items have been received by
 * the warm-up phases.
 *
 * <p>These three target types are isomorphic representations of the same underlying HLL algorithm.
 * Thus, given the same value of <i>lgConfigK</i> and the same input, all three HLL target types
 * will produce identical estimates and have identical error distributions.</p>
 *
 * <p>The memory usage and the serialization size of the sketch during this early warmup phase starts
 * out very small (8 bytes, when empty) and then grows in increments of 4 bytes as required
 * until the full HLL array is allocated.  This transition point occurs at about 10% of K for
 * sketches where lgConfigK is &gt; 8.</p>
 *
 * <ul>
 * <li><b>HLL 8</b> This uses an 8-bit byte per HLL bucket. It is generally the
 * fastest in terms of update time, but has the largest storage footprint of about
 * <i>K</i> bytes.</li>
 *
 * <li><b>HLL 6</b> This uses a 6-bit field per HLL bucket. It is the generally the next fastest
 * in terms of update time with a storage footprint of about <i>3/4 * K</i> bytes.</li>
 *
 * <li><b>HLL 4</b> This uses a 4-bit field per HLL bucket and for large counts may require
 * the use of a small internal auxiliary array for storing statistical exceptions, which are rare.
 * For the values of <i>lgConfigK &gt; 13</i> (<i>K</i> = 8192),
 * this additional array adds about 3% to the overall storage. It is generally the slowest in
 * terms of update time, but has the smallest storage footprint of about
 * <i>K/2 * 1.03</i> bytes.</li>
 * </ul>
 * @author Lee Rhodes
 */
public enum TgtHllType {
  /**
   * An HLL sketch with a bin size of 4 bits
   */
  HLL_4,
  /**
   * An HLL sketch with a bin size of 6 bits
   */
  HLL_6,
  /**
   * An Hll Sketch with a bin size of 8 bits
   */
  HLL_8;

  private static final TgtHllType values[] = values();

  /**
   * Convert the typeId to the enum type
   * @param typeId the given typeId
   * @return the enum type
   */
  public static final TgtHllType fromOrdinal(final int typeId) {
    return values[typeId];
  }
}
