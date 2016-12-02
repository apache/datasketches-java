/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Comparator;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;

/**
 * The API for Union operations for GenericQuantilesSketches
 *
 * @param <T> type of item
 *
 * @author Lee Rhodes
 * @author Alex Saydakov
 */
public final class ItemsUnion<T> {

  protected final int k_;
  protected final Comparator<? super T> comparator_;
  protected ItemsSketch<T> gadget_;

  private ItemsUnion(final int k, final Comparator<? super T> comparator,
      final ItemsSketch<T> gadget) {
    k_ = k;
    comparator_ = comparator;
    gadget_ = gadget;
  }

  /**
   * Create an instance of ItemsUnion with default k
   * @param <T> type of item
   * @param comparator to compare items
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final Comparator<? super T> comparator) {
    return new ItemsUnion<T>(ItemsSketch.DEFAULT_K, comparator, null);
  }

  /**
   * Create an instance of ItemsUnion
   * @param <T> type of item
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * It is recommended that <i>k</i> be a power of 2 to enable merging of sketches with
   * different values of <i>k</i>. However, in this case it is only possible to merge from
   * larger values of <i>k</i> to smaller values.
   * @param comparator to compare items
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final int k, final Comparator<? super T> comparator) {
    return new ItemsUnion<T>(k, comparator, null);
  }

  /**
   * Heapify the given srcMem into a Union object.
   * @param <T> type of item
   * @param srcMem the given srcMem.
   * A reference to srcMem will not be maintained internally.
   * @param comparator to compare items
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final Memory srcMem,
      final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    final ItemsSketch<T> gadget = ItemsSketch.getInstance(srcMem, comparator, serDe);
    return new ItemsUnion<T>(gadget.getK(), gadget.getComparator(), gadget);
  }

  /**
   * Create an instance of ItemsUnion based on ItemsSketch
   * @param <T> type of item
   * @param sketch the basis of the union
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final ItemsSketch<T> sketch) {
    return new ItemsUnion<T>(sketch.getK(), sketch.getComparator(), ItemsSketch.copy(sketch));
  }

  //@formatter:off
  @SuppressWarnings("null")
  static <T> ItemsSketch<T> updateLogic(final int myK, final Comparator<? super T> comparator,
      final ItemsSketch<T> myQS, final ItemsSketch<T> other) {
    int sw1 = ((myQS   == null) ? 0 :   myQS.isEmpty() ? 4 : 8);
    sw1 |=    ((other  == null) ? 0 :  other.isEmpty() ? 1 : 2);
    int outCase = 0; //0=null, 1=NOOP, 2=copy, 3=merge
    switch (sw1) {
      case 0:  outCase = 0; break; //myQS = null,  other = null ; return null
      case 1:  outCase = 4; break; //myQS = null,  other = empty; copy or downsample(myK)
      case 2:  outCase = 2; break; //myQS = null,  other = valid; copy or downsample(myK)
      case 4:  outCase = 1; break; //myQS = empty, other = null ; no-op
      case 5:  outCase = 1; break; //myQS = empty, other = empty; no-op
      case 6:  outCase = 3; break; //myQS = empty, other = valid; merge
      case 8:  outCase = 1; break; //myQS = valid, other = null ; no-op
      case 9:  outCase = 1; break; //myQS = valid, other = empty: no-op
      case 10: outCase = 3; break; //myQS = valid, other = valid; merge
      //default: //This cannot happen and cannot be tested
    }
    ItemsSketch<T> ret = null;
    switch (outCase) {
      case 0: ret = null; break;
      case 1: ret = myQS; break;
      case 2: {
        if (myK < other.getK()) {
          ret = other.downSample(myK);
        } else {
          ret = ItemsSketch.copy(other); //required because caller has handle
        }
        break;
      }
      case 3: { //must merge
        if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
          mergeInto(other, myQS);
          ret = myQS;
        } else {
          //myQS_K > other_K, must reverse roles
          //must copy other as it will become mine and can't have any externally owned handles.
          final ItemsSketch<T> myNewQS = ItemsSketch.copy(other);
          mergeInto(myQS, myNewQS);
          ret = myNewQS;
        }
        break;
      }
      case 4: {
        ret = ItemsSketch.getInstance(Math.min(myK, other.getK()), comparator);
        break;
      }
      //default: //This cannot happen and cannot be tested
    }
    return ret;
  }
  //@formatter:on

  /**
     * Merges the source sketch into the target sketch that can have a smaller value of K.
     * However, it is required that the ratio of the two K values be a power of 2.
     * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
     * The source is not modified.
     *
     * <p>Note: It is easy to prove that the following simplified code which launches multiple waves of
     * carry propagation does exactly the same amount of merging work (including the work of
     * allocating fresh buffers) as the more complicated and seemingly more efficient approach that
     * tracks a single carry propagation wave through both sketches.
     *
     * <p>This simplified code probably does do slightly more "outer loop" work, but I am pretty
     * sure that even that is within a constant factor of the more complicated code, plus the
     * total amount of "outer loop" work is at least a factor of K smaller than the total amount of
     * merging work, which is identical in the two approaches.
     *
     * <p>Note: a two-way merge that doesn't modify either of its two inputs could be implemented
     * by making a deep copy of the larger sketch and then merging the smaller one into it.
     * However, it was decided not to do this.
     *
     * @param source The source sketch
     * @param target The target sketch
     */
  @SuppressWarnings("unchecked")
  static <T> void mergeInto(final ItemsSketch<T> source, final ItemsSketch<T> target) {
    final int srcK = source.getK();
    final int tgtK = target.getK();
    final long srcN = source.getN();
    final long tgtN = target.getN();

    if (srcK != tgtK) {
      ItemsUtil.downSamplingMergeInto(source, target);
      return;
    }

    final Object[] srcLevels     = source.getCombinedBuffer(); // aliasing is a bit dangerous
    final Object[] srcBaseBuffer = srcLevels;                  // aliasing is a bit dangerous

    final long nFinal = tgtN + srcN;

    for (int i = 0; i < source.getBaseBufferCount(); i++) {
      target.update((T) srcBaseBuffer[i]);
    }

    ItemsUtil.maybeGrowLevels(nFinal, target);

    final Object[] scratchBuf = new Object[2 * tgtK];

    long srcBitPattern = source.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        ItemsUtil.inPlacePropagateCarry(
            srcLvl,
            (T[]) srcLevels, (2 + srcLvl) * tgtK,
            (T[]) scratchBuf, 0,
            false, target);
        // won't update qsTarget.n_ until the very end
      }
    }
    target.n_ = nFinal;

    assert target.getN() / (2 * tgtK) == target.getBitPattern(); // internal consistency check

    final T srcMax = source.getMaxValue();
    final T srcMin = source.getMinValue();
    final T tgtMax = target.getMaxValue();
    final T tgtMin = target.getMinValue();
    if (source.getComparator().compare(srcMax, tgtMax) > 0) { target.maxValue_ = srcMax; }
    if (source.getComparator().compare(srcMin, tgtMin) < 0) { target.minValue_ = srcMin; }
  }

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given sketch into this union object.
   * The given sketch is not modified.
   * It is required that the ratio of the two K values be a power of 2.
   * This is easily satisfied if each of the K values is already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.</p>
   *
   * @param sketchIn the sketch to be merged into this one.
   */
  public void update(final ItemsSketch<T> sketchIn) {
    gadget_ = updateLogic(k_, comparator_, gadget_, sketchIn);
  }

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given Memory image of a QuantilesSketch into this union object.
   * The given Memory object is not modified and a link to it is not retained.
   * It is required that the ratio of the two K values be a power of 2.
   * This is easily satisfied if each of the K values is already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.</p>
   * @param srcMem Memory image of sketch to be merged
   * @param serDe an instance of ArrayOfItemsSerDe
   */
  public void update(final Memory srcMem, final ArrayOfItemsSerDe<T> serDe) {
    final ItemsSketch<T> that = ItemsSketch.getInstance(srcMem, comparator_, serDe);
    gadget_ = updateLogic(k_, comparator_, gadget_, that);
  }

  /**
   * Update this union with the given double (or float) data Item.
   *
   * @param dataItem The given datum.
   */
  public void update(final T dataItem) {
    if (dataItem == null) { return; }
    if (gadget_ == null) {
      gadget_ = ItemsSketch.getInstance(k_, comparator_);
    }
    gadget_.update(dataItem);
  }

  /**
   * Gets the result of this Union operation as a copy of the internal state.
   * This enables further union update operations on this state.
   * @return the result of this Union operation
   */
  public ItemsSketch<T> getResult() {
    if (gadget_ == null) {
      return ItemsSketch.getInstance(k_, comparator_);
    }
    return ItemsSketch.copy(gadget_); //can't have any externally owned handles.
  }

  /**
   * Gets the result of this Union operation (without a copy) and resets this Union to the
   * virgin state.
   *
   * @return the result of this Union operation and reset.
   */
  public ItemsSketch<T> getResultAndReset() {
    if (gadget_ == null) { return null; } //Intentionally return null here for speed.
    final ItemsSketch<T> hqs = gadget_;
    gadget_ = null;
    return hqs;
  }

  /**
   * Resets this Union to a virgin state.
   */
  public void reset() {
    gadget_ = null;
  }

  /**
   * Returns summary information about the backing sketch.
   */
  @Override
  public String toString() {
    return toString(true, false);
  }

  /**
   * Returns summary information about the backing sketch. Used for debugging.
   * @param sketchSummary if true includes sketch summary
   * @param dataDetail if true includes data detail
   * @return summary information about the sketch.
   */
  public String toString(final boolean sketchSummary, final boolean dataDetail) {
    if (gadget_ == null) {
      return ItemsSketch.getInstance(k_, comparator_).toString();
    }
    return gadget_.toString(sketchSummary, dataDetail);
  }

}
