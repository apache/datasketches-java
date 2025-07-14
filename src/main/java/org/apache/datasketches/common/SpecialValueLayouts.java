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

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Value Layouts for Non-native Endianness
 */
public final class SpecialValueLayouts {

  private SpecialValueLayouts() { }

  /**
   * The static final for NON <i>ByteOrder.nativeOrder()</i>.
   */
  public static final ByteOrder NON_NATIVE_BYTE_ORDER =
      (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

  //Non-Native Endian Layouts

  /**
   * The static final for NON <i>ByteOrder.nativeOrder() char</i>.
   */
  public static final ValueLayout.OfChar JAVA_CHAR_UNALIGNED_NON_NATIVE =
      ValueLayout.JAVA_CHAR_UNALIGNED.withOrder(NON_NATIVE_BYTE_ORDER);

  /**
   * The static final for NON <i>ByteOrder.nativeOrder() double</i>.
   */
  public static final ValueLayout.OfDouble JAVA_DOUBLE_UNALIGNED_NON_NATIVE =
      ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(NON_NATIVE_BYTE_ORDER);

  /**
   * The static final for NON <i>ByteOrder.nativeOrder() float</i>.
   */
  public static final ValueLayout.OfFloat JAVA_FLOAT_UNALIGNED_NON_NATIVE =
      ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(NON_NATIVE_BYTE_ORDER);

  /**
   * The static final for NON <i>ByteOrder.nativeOrder() int</i>.
   */
  public static final ValueLayout.OfInt JAVA_INT_UNALIGNED_NON_NATIVE =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(NON_NATIVE_BYTE_ORDER);

  /**
   * The static final for NON <i>ByteOrder.nativeOrder() long</i>.
   */
  public static final ValueLayout.OfLong JAVA_LONG_UNALIGNED_NON_NATIVE =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(NON_NATIVE_BYTE_ORDER);

  /**
   * The static final for NON <i>ByteOrder.nativeOrder() short</i>.
   */
  public static final ValueLayout.OfShort JAVA_SHORT_UNALIGNED_NON_NATIVE =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(NON_NATIVE_BYTE_ORDER);

  //Big-Endian Layouts

  /**
   * The static final for <i>ByteOrder.BIG_ENDIAN char</i>.
   */
  public static final ValueLayout.OfChar JAVA_CHAR_UNALIGNED_BIG_ENDIAN =
      ValueLayout.JAVA_CHAR_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

  /**
   * The static final for <i>ByteOrder.BIG_ENDIAN double</i>.
   */
  public static final ValueLayout.OfDouble JAVA_DOUBLE_UNALIGNED_BIG_ENDIAN =
      ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

  /**
   * The static final for <i>ByteOrder.BIG_ENDIAN float</i>.
   */
  public static final ValueLayout.OfFloat JAVA_FLOAT_UNALIGNED_BIG_ENDIAN =
      ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

  /**
   * The static final for <i>ByteOrder.BIG_ENDIAN int</i>.
   */
  public static final ValueLayout.OfInt JAVA_INT_UNALIGNED_BIG_ENDIAN =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

  /**
   * The static final for <i>ByteOrder.BIG_ENDIAN long</i>.
   */
  public static final ValueLayout.OfLong JAVA_LONG_UNALIGNED_BIG_ENDIAN =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

  /**
   * The static final for <i>ByteOrder.BIG_ENDIAN short</i>.
   */
  public static final ValueLayout.OfShort JAVA_SHORT_UNALIGNED_BIG_ENDIAN =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

}
