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

package org.apache.datasketches.kll;

import org.apache.datasketches.kll.KllPreambleUtil.SketchType;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}
 */
public class KllDirectDoublesSketch extends KllDirectSketch {

  KllDirectDoublesSketch(final WritableMemory wmem) {
    super(wmem, SketchType.DOUBLE_SKETCH);
  }

  @Override
  public byte[] toByteArray() {
    return null;
  }

  @Override
  public String toString(final boolean withLevels, final boolean withData) {
    return null;
  }

  @Override
  public byte[] toUpdatableByteArray() {
    return null;
  }

  @Override
  int[] getLevelsArray() {
    return null;
  }

  @Override
  int getLevelsArrayAt(final int index) {
    return 0;
  }


  @Override
  void setLevelsArray(final int[] levels) {

  }

  @Override
  void setLevelsArrayAt(final int index, final int value) {

  }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {

  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {

  }

}

