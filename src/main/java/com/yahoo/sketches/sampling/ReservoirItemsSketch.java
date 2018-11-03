/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.extractEncodedReservoirSize;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractK;
import static com.yahoo.sketches.sampling.PreambleUtil.extractN;
import static com.yahoo.sketches.sampling.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.sampling.PreambleUtil.extractResizeFactor;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.sampling.SamplingUtil.pseudoHypergeometricLBonP;
import static com.yahoo.sketches.sampling.SamplingUtil.pseudoHypergeometricUBonP;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Predicate;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;

/**
 * This sketch provides a reservoir sample over an input stream of items. The sketch contains a
 * uniform random sample of unweighted items from the stream.
 *
 * @param <T> The type of object held in the reservoir.
 *
 * @author Jon Malkin
 * @author Kevin Lang
 */
public final class ReservoirItemsSketch<T> {

  /**
   * The smallest sampling array allocated: 16
   */
  private static final int MIN_LG_ARR_ITEMS = 4;

  /**
   * Using 48 bits to capture number of items seen, so sketch cannot process more after this
   * many items capacity
   */
  private static final long MAX_ITEMS_SEEN = 0xFFFFFFFFFFFFL;

  /**
   * Default sampling size multiple when reallocating storage: 8
   */
  private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

  private final int reservoirSize_;      // max size of reservoir
  private int currItemsAlloc_;           // currently allocated array size
  private long itemsSeen_;               // number of items presented to sketch
  private final ResizeFactor rf_;        // resize factor
  private ArrayList<T> data_;            // stored sampled items

  private ReservoirItemsSketch(final int k, final ResizeFactor rf) {
    // required due to a theorem about lightness during merging
    if (k < 2) {
      throw new SketchesArgumentException("k must be at least 2");
    }

    reservoirSize_ = k;
    rf_ = rf;

    itemsSeen_ = 0;

    final int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize_), "ReservoirItemsSketch");
    final int initialLgSize =
            SamplingUtil.startingSubMultiple(ceilingLgK, rf_.lg(), MIN_LG_ARR_ITEMS);

    currItemsAlloc_ = SamplingUtil.getAdjustedSize(reservoirSize_, 1 << initialLgSize);
    data_ = new ArrayList<>(currItemsAlloc_);
  }

  /**
   * Creates a fully-populated sketch. Used internally to avoid extraneous array allocation
   * when deserializing.
   * Uses size of items array to as initial array allocation.
   *
   * @param data      Reservoir items as an <tt>ArrayList&lt;T&gt;</tt>
   * @param itemsSeen Number of items presented to the sketch so far
   * @param rf        <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param k         Maximum size of reservoir
   */
  private ReservoirItemsSketch(final ArrayList<T> data, final long itemsSeen,
                               final ResizeFactor rf, final int k) {
    if (data == null) {
      throw new SketchesArgumentException("Instantiating sketch with null reservoir");
    }
    if (k < 2) {
      throw new SketchesArgumentException("Cannot instantiate sketch with reservoir size less than 2");
    }
    if (k < data.size()) {
      throw new SketchesArgumentException("Instantiating sketch with max size less than array length: "
              + k + " max size, array of length " + data.size());
    }
    if (((itemsSeen >= k) && (data.size() < k))
            || ((itemsSeen < k) && (data.size() < itemsSeen))) {
      throw new SketchesArgumentException("Instantiating sketch with too few samples. Items seen: "
              + itemsSeen + ", max reservoir size: " + k
              + ", items array length: " + data.size());
    }

    // Should we compute target current allocation to validate?
    reservoirSize_ = k;
    currItemsAlloc_ = data.size();
    itemsSeen_ = itemsSeen;
    rf_ = rf;
    data_ = data;
  }

  /**
   * Fast constructor for fully-specified sketch with no encoded/decoding size and no
   * validation. Used with copy().
   *
   * @param k              Maximum reservoir capacity
   * @param currItemsAlloc Current array size (assumed equal to items.length)
   * @param itemsSeen      Total items seen by this sketch
   * @param rf             <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param data           Data ArrayList backing the reservoir, will <em>not</em> be copied
   */
  private ReservoirItemsSketch(final int k, final int currItemsAlloc, final long itemsSeen,
                               final ResizeFactor rf, final ArrayList<T> data) {
    this.reservoirSize_ = k;
    this.currItemsAlloc_ = currItemsAlloc;
    this.itemsSeen_ = itemsSeen;
    this.rf_ = rf;
    this.data_ = data;
  }

  /**
   * Construct a mergeable sampling sketch with up to k samples using the default resize
   * factor (8).
   *
   * @param k   Maximum size of sampling. Allocated size may be smaller until reservoir fills.
   *            Unlike many sketches in this package, this value does <em>not</em> need to be a
   *            power of 2.
   * @param <T> The type of object held in the reservoir.
   * @return A ReservoirLongsSketch initialized with maximum size k and the default resize factor.
   */
  public static <T> ReservoirItemsSketch<T> newInstance(final int k) {
    return new ReservoirItemsSketch<>(k, DEFAULT_RESIZE_FACTOR);
  }

  /**
   * Construct a mergeable sampling sketch with up to k samples using a specified resize factor.
   *
   * @param k   Maximum size of sampling. Allocated size may be smaller until reservoir fills.
   *            Unlike many sketches in this package, this value does <em>not</em> need to be a
   *            power of 2.
   * @param rf  <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param <T> The type of object held in the reservoir.
   * @return A ReservoirLongsSketch initialized with maximum size k and resize factor rf.
   */
  public static <T> ReservoirItemsSketch<T> newInstance(final int k, final ResizeFactor rf) {
    return new ReservoirItemsSketch<>(k, rf);
  }

  /**
   * Thin wrapper around private constructor
   *
   * @param <T> data type
   * @param data      Reservoir items as ArrayList&lt;T&gt;
   * @param itemsSeen Number of items presented to the sketch so far
   * @param rf        <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param k         Compact encoding of reservoir size
   * @return New sketch built with the provided inputs
   */
  static <T> ReservoirItemsSketch<T> newInstance(final ArrayList<T> data, final long itemsSeen,
                                                 final ResizeFactor rf, final int k) {
    return new ReservoirItemsSketch<>(data, itemsSeen, rf, k);
  }

  /**
   * Returns a sketch instance of this class from the given srcMem,
   * which must be a Memory representation of this sketch class.
   *
   * @param <T>    The type of item this sketch contains
   * @param srcMem a Memory representation of a sketch of this class.
   *               <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param serDe  An instance of ArrayOfItemsSerDe
   * @return a sketch instance of this class
   */
  public static <T> ReservoirItemsSketch<T> heapify(final Memory srcMem,
                                                    final ArrayOfItemsSerDe<T> serDe) {
    Family.RESERVOIR.checkFamilyID(srcMem.getByte(FAMILY_BYTE));

    final int numPreLongs = extractPreLongs(srcMem);
    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(srcMem));
    final int serVer = extractSerVer(srcMem);
    final boolean isEmpty = (extractFlags(srcMem) & EMPTY_FLAG_MASK) != 0;
    final long itemsSeen = (isEmpty ? 0 : extractN(srcMem));
    int k = extractK(srcMem);

    // Check values
    final boolean preLongsEqMin = (numPreLongs == Family.RESERVOIR.getMinPreLongs());
    final boolean preLongsEqMax = (numPreLongs == Family.RESERVOIR.getMaxPreLongs());

    if (!preLongsEqMin & !preLongsEqMax) {
      throw new SketchesArgumentException(
              "Possible corruption: Non-empty sketch with only "
                      + Family.RESERVOIR.getMinPreLongs() + " preLong(s)");
    }

    if (serVer != SER_VER) {
      if (serVer == 1) {
        final short encK = extractEncodedReservoirSize(srcMem);
        k = ReservoirSize.decodeValue(encK);
      } else {
        throw new SketchesArgumentException(
                "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
      }
    }

    if (isEmpty) {
      return new ReservoirItemsSketch<>(k, rf);
    }

    final int preLongBytes = numPreLongs << 3;
    int allocatedItems = k; // default to full reservoir

    if (itemsSeen < k) {
      // under-full so determine size to allocate, using ceilingLog2(totalSeen) as minimum
      // casts to int are safe since under-full
      final int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(k), "heapify");
      final int minLgSize = Util.toLog2(Util.ceilingPowerOf2((int) itemsSeen), "heapify");
      final int initialLgSize = SamplingUtil.startingSubMultiple(ceilingLgK, rf.lg(),
              Math.max(minLgSize, MIN_LG_ARR_ITEMS));

      allocatedItems = SamplingUtil.getAdjustedSize(k, 1 << initialLgSize);
    }

    final int itemsToRead = (int) Math.min(k, itemsSeen);
    final T[] data = serDe.deserializeFromMemory(
        srcMem.region(preLongBytes, srcMem.getCapacity() - preLongBytes), itemsToRead);
    final ArrayList<T> dataList = new ArrayList<>(Arrays.asList(data));

    final ReservoirItemsSketch<T> ris = new ReservoirItemsSketch<>(dataList, itemsSeen, rf, k);
    ris.data_.ensureCapacity(allocatedItems);
    ris.currItemsAlloc_ = allocatedItems;

    return ris;
  }

  /**
   * Returns the sketch's value of <i>k</i>, the maximum number of samples stored in the
   * reservoir. The current number of items in the sketch may be lower.
   *
   * @return k, the maximum number of samples in the reservoir
   */
  public int getK() {
    return reservoirSize_;
  }

  /**
   * Returns the number of items processed from the input stream
   *
   * @return n, the number of stream items the sketch has seen
   */
  public long getN() {
    return itemsSeen_;
  }

  /**
   * Returns the current number of items in the reservoir, which may be smaller than the
   * reservoir capacity.
   *
   * @return the number of items currently in the reservoir
   */
  public int getNumSamples() {
    return (int) Math.min(reservoirSize_, itemsSeen_);
  }

  /**
   * Randomly decide whether or not to include an item in the sample set.
   *
   * @param item a unit-weight (equivalently, unweighted) item of the set being sampled from
   */
  public void update(final T item) {
    if (itemsSeen_ == MAX_ITEMS_SEEN) {
      throw new SketchesStateException("Sketch has exceeded capacity for total items seen: "
              + MAX_ITEMS_SEEN);
    }
    if (item == null) {
      return;
    }

    if (itemsSeen_ < reservoirSize_) { // initial phase, take the first reservoirSize_ items
      if (itemsSeen_ >= currItemsAlloc_) {
        growReservoir();
      }
      assert itemsSeen_ < currItemsAlloc_;
      // we'll randomize replacement positions, so in-order should be valid for now
      data_.add(item);
      ++itemsSeen_;
    } else { // code for steady state where we sample randomly
      ++itemsSeen_;
      // prob(keep_item) < k / n = reservoirSize_ / itemsSeen_
      // so multiply to get: keep if rand * itemsSeen_ < reservoirSize_
      if ((SamplingUtil.rand.nextDouble() * itemsSeen_) < reservoirSize_) {
        final int newSlot = SamplingUtil.rand.nextInt(reservoirSize_);
        data_.set(newSlot, item);
      }
    }
  }

  /**
   * Resets this sketch to the empty state, but retains the original value of k.
   */
  public void reset() {
    final int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize_), "ReservoirItemsSketch");
    final int initialLgSize =
            SamplingUtil.startingSubMultiple(ceilingLgK, rf_.lg(), MIN_LG_ARR_ITEMS);

    currItemsAlloc_ = SamplingUtil.getAdjustedSize(reservoirSize_, 1 << initialLgSize);
    data_ = new ArrayList<>(currItemsAlloc_);
    itemsSeen_ = 0;
  }

  /**
   * Returns a copy of the items in the reservoir, or null if empty. The returned array length
   * may be smaller than the reservoir capacity.
   *
   * <p>In order to allocate an array of generic type T, uses the class of the first item in
   * the array. This method method may throw an <tt>ArrayAssignmentException</tt> if the
   * reservoir stores instances of a polymorphic base class.</p>
   *
   * @return A copy of the reservoir array
   */
  @SuppressWarnings("unchecked")
  public T[] getSamples() {
    if (itemsSeen_ == 0) {
      return null;
    }

    final Class<?> clazz = data_.get(0).getClass();
    return data_.toArray((T[]) Array.newInstance(clazz, 0));
  }

  /**
   * Returns a copy of the items in the reservoir as members of Class <em>clazz</em>, or null
   * if empty. The returned array length may be smaller than the reservoir capacity.
   *
   * <p>This method allocates an array of class <em>clazz</em>, which must either match or
   * extend T. This method should be used when objects in the array are all instances of T but
   * are not necessarily instances of the base class.</p>
   *
   * @param clazz A class to which the items are cast before returning
   * @return A copy of the reservoir array
   */
  @SuppressWarnings("unchecked")
  public T[] getSamples(final Class<?> clazz) {
    if (itemsSeen_ == 0) {
      return null;
    }

    return data_.toArray((T[]) Array.newInstance(clazz, 0));
  }

  /**
   * Returns the actual List backing the reservoir. <em>Any changes to this List will corrupt
   * the reservoir sample.</em>
   *
   * <p>This method should be used only when making a copy of the returned samples, to avoid
   * an extraneous array copy.</p>
   *
   * @return The raw array backing this reservoir.
   */
  ArrayList<T> getRawSamplesAsList() {
    return data_;
  }

  /**
   * Returns a human-readable summary of the sketch, without items.
   *
   * @return A string version of the sketch summary
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    final String thisSimpleName = this.getClass().getSimpleName();

    sb.append(LS);
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   k            : ").append(reservoirSize_).append(LS);
    sb.append("   n            : ").append(itemsSeen_).append(LS);
    sb.append("   Current size : ").append(currItemsAlloc_).append(LS);
    sb.append("   Resize factor: ").append(rf_).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);

    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a ReservoirItemsSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a ReservoirItemsSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.preambleToString(byteArr);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a ReservoirItemsSketch.
   * @param mem the given Memory
   * @return a human readable string of the preamble of a Memory image of a ReservoirItemsSketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.preambleToString(mem);
  }

  /**
   * Returns a byte array representation of this sketch. May fail for polymorphic item types.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return a byte array representation of this sketch
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<? super T> serDe) {
    if (itemsSeen_ == 0) {
      // null class is ok since empty -- no need to call serDe
      return toByteArray(serDe, null);
    } else {
      return toByteArray(serDe, data_.get(0).getClass());
    }
  }

  /**
   * Returns a byte array representation of this sketch. Copies contents into an array of the
   * specified class for serialization to allow for polymorphic types.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @param clazz The class represented by &lt;T&gt;
   * @return a byte array representation of this sketch
   */
  @SuppressWarnings("null") // bytes will be null only if empty == true
  public byte[] toByteArray(final ArrayOfItemsSerDe<? super T> serDe, final Class<?> clazz) {
    final int preLongs, outBytes;
    final boolean empty = itemsSeen_ == 0;
    byte[] bytes = null; // for serialized items from serDe

    if (empty) {
      preLongs = 1;
      outBytes = 8;
    } else {
      preLongs = Family.RESERVOIR.getMaxPreLongs();
      bytes = serDe.serializeToByteArray(getSamples(clazz));
      outBytes = (preLongs << 3) + bytes.length;
    }
    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.wrap(outArr);

    // Common header elements
    PreambleUtil.insertPreLongs(mem, preLongs);                  // Byte 0
    PreambleUtil.insertLgResizeFactor(mem, rf_.lg());
    PreambleUtil.insertSerVer(mem, SER_VER);                     // Byte 1
    PreambleUtil.insertFamilyID(mem, Family.RESERVOIR.getID());  // Byte 2
    if (empty) {
      PreambleUtil.insertFlags(mem, EMPTY_FLAG_MASK);            // Byte 3
    } else {
      PreambleUtil.insertFlags(mem, 0);
    }
    PreambleUtil.insertK(mem, reservoirSize_);                   // Bytes 4-7

    // conditional elements
    if (!empty) {
      PreambleUtil.insertN(mem, itemsSeen_);

      // insert the bytearray of serialized samples, offset by the preamble size
      final int preBytes = preLongs << 3;
      mem.putByteArray(preBytes, bytes, 0, bytes.length);
    }

    return outArr;
  }

  /**
   * Computes an estimated subset sum from the entire stream for objects matching a given
   * predicate. Provides a lower bound, estimate, and upper bound using a target of 2 standard
   * deviations.
   *
   * <p>This is technically a heuristic method, and tries to err on the conservative side.</p>
   *
   * @param predicate A predicate to use when identifying items.
   * @return A summary object containing the estimate, upper and lower bounds, and the total
   * sketch weight.
   */
  public SampleSubsetSummary estimateSubsetSum(final Predicate<T> predicate) {
    if (itemsSeen_ == 0) {
      return new SampleSubsetSummary(0.0, 0.0, 0.0, 0.0);
    }

    final long numSamples = getNumSamples();
    final double samplingRate = numSamples / (double) itemsSeen_;
    assert samplingRate >= 0.0;
    assert samplingRate <= 1.0;

    int trueCount = 0;
    for (int i = 0; i < numSamples; ++i) {
      if (predicate.test(data_.get(i))) {
        ++trueCount;
      }
    }

    // if in exact mode, we can return an exact answer
    if (itemsSeen_ <= reservoirSize_) {
      return new SampleSubsetSummary(trueCount, trueCount, trueCount, numSamples);
    }

    final double lbTrueFraction = pseudoHypergeometricLBonP(numSamples, trueCount, samplingRate);
    final double estimatedTrueFraction = (1.0 * trueCount) / numSamples;
    final double ubTrueFraction = pseudoHypergeometricUBonP(numSamples, trueCount, samplingRate);
    return new SampleSubsetSummary(
            itemsSeen_ * lbTrueFraction,
            itemsSeen_ * estimatedTrueFraction,
            itemsSeen_ * ubTrueFraction,
            itemsSeen_);
  }

  double getImplicitSampleWeight() {
    if (itemsSeen_ < reservoirSize_) {
      return 1.0;
    } else {
      return ((1.0 * itemsSeen_) / reservoirSize_);
    }
  }

  /**
   * Useful during union operations to avoid copying the items array around if only updating a
   * few points.
   *
   * @param pos The position from which to retrieve the element
   * @return The value in the reservoir at position <tt>pos</tt>
   */
  T getValueAtPosition(final int pos) {
    if (itemsSeen_ == 0) {
      throw new SketchesArgumentException("Requested element from empty reservoir.");
    } else if ((pos < 0) || (pos >= getNumSamples())) {
      throw new SketchesArgumentException("Requested position must be between 0 and "
              + getNumSamples() + ", " + "inclusive. Received: " + pos);
    }

    return data_.get(pos);
  }

  /**
   * Useful during union operation to force-insert a value into the union gadget. Does
   * <em>NOT</em> increment count of items seen.
   *
   * @param value The entry to store in the reservoir
   * @param pos   The position at which to store the entry
   */
  void insertValueAtPosition(final T value, final int pos) {
    if ((pos < 0) || (pos >= getNumSamples())) {
      throw new SketchesArgumentException("Insert position must be between 0 and "
              + getNumSamples() + ", " + "inclusive. Received: " + pos);
    }

    data_.set(pos, value);
  }

  /**
   * Used during union operations to update count of items seen. Does <em>NOT</em> check sign,
   * but will throw an exception if the final result exceeds the maximum possible items seen
   * value.
   *
   * @param inc The value added
   */
  void forceIncrementItemsSeen(final long inc) {
    itemsSeen_ += inc;

    if (itemsSeen_ > MAX_ITEMS_SEEN) {
      throw new SketchesStateException("Sketch has exceeded capacity for total items seen. "
              + "Limit: " + MAX_ITEMS_SEEN + ", found: " + itemsSeen_);
    }
  }

  /**
   * Used during union operations to ensure we do not overwrite an existing reservoir. Creates a
   * shallow copy of the reservoir.
   *
   * @return A copy of the current sketch
   */
  @SuppressWarnings("unchecked")
  ReservoirItemsSketch<T> copy() {
    return new ReservoirItemsSketch<>(reservoirSize_, currItemsAlloc_,
            itemsSeen_, rf_, (ArrayList<T>) data_.clone());
  }

  // Note: the downsampling approach may appear strange but avoids several edge cases
  //   Q1: Why not just permute samples and then take the first "newK" of them?
  //   A1: We're assuming the sketch source is read-only
  //   Q2: Why not copy the source sketch, permute samples, then truncate the sample array and
  //       reduce k?
  //   A2: That would involve allocating memory proportional to the old k. Even if only a
  //       temporary violation of maxK, we're avoiding violating it at all.
  ReservoirItemsSketch<T> downsampledCopy(final int maxK) {
    final ReservoirItemsSketch<T> ris = new ReservoirItemsSketch<>(maxK, rf_);
    for (final T item : getSamples()) {
      // Pretending old implicit weights are all 1. Not true in general, but they're all
      // equal so update should work properly as long as we update itemsSeen_ at the end.
      ris.update(item);
    }

    // need to adjust number seen to get correct new implicit weights
    if (ris.getN() < itemsSeen_) {
      ris.forceIncrementItemsSeen(itemsSeen_ - ris.getN());
    }

    return ris;
  }

  /**
   * Increases allocated sampling size by (adjusted) ResizeFactor and copies items from old
   * sampling.
   */
  private void growReservoir() {
    currItemsAlloc_ = SamplingUtil.getAdjustedSize(reservoirSize_, currItemsAlloc_ << rf_.lg());
    data_.ensureCapacity(currItemsAlloc_);
  }
}
