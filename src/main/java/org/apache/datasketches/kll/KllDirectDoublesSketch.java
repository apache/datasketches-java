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


  public KllDirectDoublesSketch(final WritableMemory wmem) {
    super(wmem, SketchType.DOUBLE_SKETCH);
  }

  //public int getNumRetained()
  @SuppressWarnings("unused")
  public double[] getCDF(final double[] splitPoints) {
    return null;
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
  double[] getDoubleItemsArray() {
    return null;
  }

  @Override
  float[] getFloatItemsArray() {
    return null;
  }

  @Override
  double getMaxDoubleValue() {
    return 0;
  }

  @Override
  float getMaxFloatValue() {
    return 0;
  }

  @Override
  double getMinDoubleValue() {
    return 0;
  }

  @Override
  float getMinFloatValue() {
    return 0;
  }

  @Override
  void setDoubleItemsArray(final double[] floatItems) {
  }

  @Override
  void setFloatItemsArray(final float[] floatItems) {
  }

  @Override
  void setMaxDoubleValue(final double value) {
  }

  @Override
  void setMaxFloatValue(final float value) {
  }

  @Override
  void setMinDoubleValue(final double value) {
  }

  @Override
  void setMinFloatValue(final float value) {
  }

  @Override
  void setLevelsArray(final int[] levelsArr) {

  }

  //int getDyMinK

  //int[] getLevelsArray

  //int getLevelsArrayAt()

  //int getNumLevels

  //void incN()

  //void incNumLevels()

  //boolean isLevelZeroSorted()

  //void setDyMinK()

  //void updateLevelsArray()

  //void setLevelsArrayAt()

  //void setLevelsArrayAtMinusEq()

  //void setLevelsArrayAtPlusEq()

  //void setLevelZeroSorted()

  //void setN()

  //void setNumLevels()

  //int getItemsDataStartBytes()

  //int getItemsArrLengthItems()

  //int getLevelsArrLengthints()


}

