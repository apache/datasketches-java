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

package org.apache.datasketches.common;

/**
 * For the Families that accept this configuration parameter, it controls the size multiple that
 * affects how fast the internal cache grows, when more space is required.
 * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
 *
 * @author Lee Rhodes
 */
public enum ResizeFactor {
  /**
   * Do not resize. Sketch will be configured to full size.
   */
  X1(0),
  /**
   * Resize factor is 2.
   */
  X2(1),
  /**
   * Resize factor is 4.
   */
  X4(2),
  /**
   * Resize factor is 8.
   */
  X8(3);

  private int lg_;

  ResizeFactor(final int lg) {
    this.lg_  = lg;
  }

  /**
   * Returns the Log-base 2 of the Resize Factor
   * @return the Log-base 2 of the Resize Factor
   */
  public int lg() {
    return lg_;
  }

  /**
   * Returns the Resize Factor given the Log-base 2 of the Resize Factor
   * @param lg a value between zero and 3, inclusive.
   * @return the Resize Factor given the Log-base 2 of the Resize Factor
   */
  public static ResizeFactor getRF(final int lg) {
    if (X1.lg() == lg) { return X1; }
    if (X2.lg() == lg) { return X2; }
    if (X4.lg() == lg) { return X4; }
    return X8;
  }

  /**
   * Returns the Resize Factor
   * @return the Resize Factor
   */
  public int getValue() {
    return 1 << lg_;
  }
}
