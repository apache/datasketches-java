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

package org.apache.datasketches.quantilescommon;

import java.lang.reflect.Array;
import java.util.Comparator;

/**
 * This class reinserts the min and max values into the sorted view arrays as required.
 */
public class IncludeMinMax {

  public static class DoublesPair {
    public double[] quantiles;
    public long[] cumWeights;

    public DoublesPair(final double[] quantiles, final long[] cumWeights) {
      this.quantiles = quantiles;
      this.cumWeights = cumWeights;
    }
  }

  public static class FloatsPair {
    public float[] quantiles;
    public long[] cumWeights;

    public FloatsPair(final float[] quantiles, final long[] cumWeights) {
      this.quantiles = quantiles;
      this.cumWeights = cumWeights;
    }
  }

  public static class ItemsPair<T> {
    public T[] quantiles;
    public long[] cumWeights;

    public ItemsPair(final T[] quantiles, final long[] cumWeights) {
      this.quantiles = quantiles;
      this.cumWeights = cumWeights;
    }
  }

  public static DoublesPair includeDoublesMinMax(
      final double[] quantilesIn,
      final long[] cumWeightsIn,
      final double maxItem,
      final double minItem) {
    final int lenIn = cumWeightsIn.length;
    final boolean adjLow = quantilesIn[0] != minItem; //if true, adjust the low end
    final boolean adjHigh = quantilesIn[lenIn - 1] != maxItem; //if true, adjust the high end
    int adjLen = lenIn; //this will be the length of the local copies of quantiles and cumWeights
    adjLen += adjLow ? 1 : 0;
    adjLen += adjHigh ? 1 : 0;
    final double[] adjQuantiles;
    final long[] adjCumWeights;
    if (adjLen > lenIn) { //is any adjustment required at all?
      adjQuantiles = new double[adjLen];
      adjCumWeights = new long[adjLen];
      final int offset = adjLow ? 1 : 0;
      System.arraycopy(quantilesIn, 0, adjQuantiles, offset, lenIn);
      System.arraycopy(cumWeightsIn,0, adjCumWeights, offset, lenIn);

      //Adjust the low end if required.
      if (adjLow) {
        adjQuantiles[0] = minItem;
        adjCumWeights[0] = 1;
      }

      if (adjHigh) {
        adjQuantiles[adjLen - 1] = maxItem;
        adjCumWeights[adjLen - 1] = cumWeightsIn[lenIn - 1];
        adjCumWeights[adjLen - 2] = cumWeightsIn[lenIn - 1] - 1;
      }
    } else { //both min and max are already in place, no adjustments are required.
      adjQuantiles = quantilesIn;
      adjCumWeights = cumWeightsIn;

    } //END of Adjust End Points
    return new DoublesPair(adjQuantiles, adjCumWeights);
  }

  public static FloatsPair includeFloatsMinMax(
      final float[] quantilesIn,
      final long[] cumWeightsIn,
      final float maxItem,
      final float minItem) {
    final int lenIn = cumWeightsIn.length;
    final boolean adjLow = quantilesIn[0] != minItem; //if true, adjust the low end
    final boolean adjHigh = quantilesIn[lenIn - 1] != maxItem; //if true, adjust the high end
    int adjLen = lenIn; //this will be the length of the local copies of quantiles and cumWeights
    adjLen += adjLow ? 1 : 0;
    adjLen += adjHigh ? 1 : 0;
    final float[] adjQuantiles;
    final long[] adjCumWeights;
    if (adjLen > lenIn) { //is any adjustment required at all?
      adjQuantiles = new float[adjLen];
      adjCumWeights = new long[adjLen];
      final int offset = adjLow ? 1 : 0;
      System.arraycopy(quantilesIn, 0, adjQuantiles, offset, lenIn);
      System.arraycopy(cumWeightsIn,0, adjCumWeights, offset, lenIn);

      //Adjust the low end if required.
      if (adjLow) {
        adjQuantiles[0] = minItem;
        adjCumWeights[0] = 1;
      }

      if (adjHigh) {
        adjQuantiles[adjLen - 1] = maxItem;
        adjCumWeights[adjLen - 1] = cumWeightsIn[lenIn - 1];
        adjCumWeights[adjLen - 2] = cumWeightsIn[lenIn - 1] - 1;
      }
    } else { //both min and max are already in place, no adjustments are required.
      adjQuantiles = quantilesIn;
      adjCumWeights = cumWeightsIn;

    } //END of Adjust End Points
    return new FloatsPair(adjQuantiles, adjCumWeights);
  }

  @SuppressWarnings("unchecked")
  public static <T> ItemsPair<T> includeItemsMinMax(
      final T[] quantilesIn,
      final long[] cumWeightsIn,
      final T maxItem,
      final T minItem,
      final Comparator<? super T> comparator) {
    final int lenIn = cumWeightsIn.length;
    final boolean adjLow = comparator.compare(quantilesIn[0], minItem) != 0; //if true, adjust the low end
    final boolean adjHigh = comparator.compare(quantilesIn[lenIn - 1], maxItem) != 0; //if true, adjust the high end
    int adjLen = lenIn; //this will be the length of the local copies of quantiles and cumWeights
    adjLen += adjLow ? 1 : 0;
    adjLen += adjHigh ? 1 : 0;
    final T[] adjQuantiles;
    final long[] adjCumWeights;
    if (adjLen > lenIn) { //is any adjustment required at all?
      adjQuantiles = (T[]) Array.newInstance(minItem.getClass(), adjLen);
      adjCumWeights = new long[adjLen];
      final int offset = adjLow ? 1 : 0;
      System.arraycopy(quantilesIn, 0, adjQuantiles, offset, lenIn);
      System.arraycopy(cumWeightsIn,0, adjCumWeights, offset, lenIn);

      //Adjust the low end if required.
      if (adjLow) {
        adjQuantiles[0] = minItem;
        adjCumWeights[0] = 1;
      }

      if (adjHigh) {
        adjQuantiles[adjLen - 1] = maxItem;
        adjCumWeights[adjLen - 1] = cumWeightsIn[lenIn - 1];
        adjCumWeights[adjLen - 2] = cumWeightsIn[lenIn - 1] - 1;
      }
    } else { //both min and max are already in place, no adjustments are required.
      adjQuantiles = quantilesIn;
      adjCumWeights = cumWeightsIn;

    } //END of Adjust End Points
    return new ItemsPair<>(adjQuantiles, adjCumWeights);
  }

}
