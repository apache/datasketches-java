/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.GADGET_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.TOTAL_WEIGHT_R_DOUBLE;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractHRegionItemCount;
import static com.yahoo.sketches.sampling.PreambleUtil.extractK;
import static com.yahoo.sketches.sampling.PreambleUtil.extractN;
import static com.yahoo.sketches.sampling.PreambleUtil.extractRRegionItemCount;
import static com.yahoo.sketches.sampling.PreambleUtil.extractResizeFactor;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.sampling.PreambleUtil.extractTotalRWeight;
import static com.yahoo.sketches.sampling.PreambleUtil.getAndCheckPreLongs;
import static com.yahoo.sketches.sampling.SamplingUtil.pseudoHypergeometricLBonP;
import static com.yahoo.sketches.sampling.SamplingUtil.pseudoHypergeometricUBonP;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfBooleansSerDe;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;

/**
 * This sketch provides a variance optimal sample over an input stream of weighted items. The
 * sketch can be used to compute subset sums over predicates, producing estimates with optimal
 * variance for a given sketch size.
 *
 * <p>Using this sketch with uniformly constant item weights (e.g. 1.0) will produce a standard
 * reservoir sample over the steam.</p>
 *
 * @param <T> The type of object held in the sketch.
 *
 * @author Jon Malkin
 * @author Kevin Lang
 */
public final class VarOptItemsSketch<T> {
  /**
   * The smallest sampling array allocated: 16
   */
  private static final int MIN_LG_ARR_ITEMS = 4;

  /**
   * Default sampling size multiple when reallocating storage: 8
   */
  private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

  private static final ArrayOfBooleansSerDe MARK_SERDE = new ArrayOfBooleansSerDe();

  private int k_;                        // max size of sketch, in items
  private int currItemsAlloc_;           // currently allocated array size
  private final ResizeFactor rf_;        // resize factor
  private ArrayList<T> data_;            // stored sampled items
  private ArrayList<Double> weights_;    // weights for sampled items

  private long n_;                       // total number of items processed by the sketch
  private int h_;                        // number of items in heap
  private int m_;                        // number of items in middle region
  private int r_;                        // number of items in reservoir-like area
  private double totalWtR_;              // total weight of items in reservoir-like area

  // The next two fields are hidden from the user because they are part of the state of the
  // unioning algorithm, NOT part of a varopt sketch, or even of a varopt "gadget" (our name for
  // the potentially invalid sketch that is maintained by the unioning algorithm). It would make
  // more sense logically for these fields to be declared in the unioning object (whose entire
  // purpose is storing the state of the unioning algorithm) but for reasons of programming
  // convenience we are currently declaring them here. However, that could change in the future.

  // Following int is:
  //  1. Zero (for a varopt sketch)
  //  2. Count of marked items in H region, if part of a unioning algo's gadget
  private int numMarksInH_;

  // The following array is absent in a varopt sketch, and notionally present in a gadget
  // [although it really belongs in the unioning object]. If the array were to be made explicit,
  // some additional coding would need to be done to ensure that all of the necessary data motion
  // occurs and is properly tracked.
  private ArrayList<Boolean> marks_;

  // used to return a shallow copy of the sketch's samples to a VarOptItemsSamples, as arrays
  // with any null value stripped and the R region weight computed
  class Result {
    T[] items;
    double[] weights;
  }

  private VarOptItemsSketch(final int k, final ResizeFactor rf) {
    // required due to a theorem about lightness during merging
    if (k < 1) {
      throw new SketchesArgumentException("k must be at least 1");
    }

    k_ = k;
    n_ = 0;
    rf_ = rf;

    h_ = 0;
    m_ = 0;
    r_ = 0;
    totalWtR_ = 0;
    numMarksInH_ = 0;

    final int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(k_), "VarOptItemsSketch");
    final int initialLgSize =
            SamplingUtil.startingSubMultiple(ceilingLgK, rf_.lg(), MIN_LG_ARR_ITEMS);

    currItemsAlloc_ = SamplingUtil.getAdjustedSize(k_, 1 << initialLgSize);
    if (currItemsAlloc_ == k_) {
      ++currItemsAlloc_;
    }

    data_ = new ArrayList<>(currItemsAlloc_);
    weights_ = new ArrayList<>(currItemsAlloc_);
    marks_ = null;
  }

  private VarOptItemsSketch(final ArrayList<T> dataList,
                            final ArrayList<Double> weightList,
                            final int k,
                            final long n,
                            final int currItemsAlloc,
                            final ResizeFactor rf,
                            final int hCount,
                            final int rCount,
                            final double totalWtR) {
    assert dataList != null;
    assert weightList != null;
    assert dataList.size() == weightList.size();
    assert currItemsAlloc >= dataList.size();
    assert k >= 2;
    assert n >= 0;
    assert hCount >= 0;
    assert rCount >= 0;
    assert ((rCount == 0) && (dataList.size() == hCount)) || ((rCount > 0) && (dataList.size() == (k + 1)));

    k_ = k;
    n_ = n;
    h_ = hCount;
    r_ = rCount;
    m_ = 0;
    totalWtR_ = totalWtR;
    currItemsAlloc_ = currItemsAlloc;
    rf_ = rf;
    data_ = dataList;
    weights_ = weightList;

    numMarksInH_ = 0;
    marks_ = null;
  }

  /**
   * Construct a varopt sampling sketch with up to k samples using the default resize factor (8).
   *
   * @param k   Maximum size of sampling. Allocated size may be smaller until sketch fills.
   *            Unlike many sketches in this package, this value does <em>not</em> need to be a
   *            power of 2.
   * @param <T> The type of object held in the sketch.
   * @return A VarOptItemsSketch initialized with maximum size k and resize factor rf.
   */
  public static <T> VarOptItemsSketch<T> newInstance(final int k) {
    return new VarOptItemsSketch<>(k, DEFAULT_RESIZE_FACTOR);
  }

  /**
   * Construct a varopt sampling sketch with up to k samples using the specified resize factor.
   *
   * @param k   Maximum size of sampling. Allocated size may be smaller until sketch fills.
   *            Unlike many sketches in this package, this value does <em>not</em> need to be a
   *            power of 2.
   * @param rf  <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param <T> The type of object held in the sketch.
   * @return A VarOptItemsSketch initialized with maximum size k and resize factor rf.
   */
  public static <T> VarOptItemsSketch<T> newInstance(final int k, final ResizeFactor rf) {
    return new VarOptItemsSketch<>(k, rf);
  }

  /**
   * Construct a varopt sketch for use as a unioning gadget, meaning the array of marked elements
   * is also initialized.
   *
   * @param k   Maximum size of sampling. Allocated size may be smaller until sketch fills.
   *            Unlike many sketches in this package, this value does <em>not</em> need to be a
   *            power of 2.
   * @param <T> The type of object held in the sketch.
   * @return A VarOptItemsSketch initialized with maximum size k and a valid array of marks.
   */
  static <T> VarOptItemsSketch<T> newInstanceAsGadget(final int k) {
    final VarOptItemsSketch<T> sketch = new VarOptItemsSketch<>(k, DEFAULT_RESIZE_FACTOR);
    sketch.marks_ = new ArrayList<>(sketch.currItemsAlloc_);
    return sketch;
  }

  /**
   * Construct a varopt sketch as the output of a union's getResult() method. Because this method
   * is package-private, we do not perform checks on the input values.
   *
   * <p>Assumes dataList.size() is the correct allocated size but does not check.</p>
   *
   * @param <T> The type of object held in the sketch.
   * @param dataList an ArrayList of data
   * @param weightList an ArrayList of weights
   * @param k   Maximum size of sampling. Allocated size may be smaller until sketch fills.
   *            Unlike many sketches in this package, this value does <em>not</em> need to be a
   *            power of 2.
   * @param n The current count of items seen by the sketch
   * @param hCount the count of heavy items
   * @param rCount the reservoir count of (non-heavy) items
   * @param totalWtR the sum of the reservoir weights.
   * @return A VarOptItemsSketch initialized with maximum size k and a valid array of marks.
   */
  static <T> VarOptItemsSketch<T> newInstanceFromUnionResult(final ArrayList<T> dataList,
                                                             final ArrayList<Double> weightList,
                                                             final int k,
                                                             final long n,
                                                             final int hCount,
                                                             final int rCount,
                                                             final double totalWtR) {
    final VarOptItemsSketch<T> sketch =  new VarOptItemsSketch<>(dataList, weightList, k, n,
            dataList.size(), DEFAULT_RESIZE_FACTOR, hCount, rCount, totalWtR);
    sketch.convertToHeap();
    return sketch;
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
  @SuppressWarnings("null")
  public static <T> VarOptItemsSketch<T> heapify(final Memory srcMem,
                                                 final ArrayOfItemsSerDe<T> serDe) {
    final int numPreLongs = getAndCheckPreLongs(srcMem);
    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(srcMem));
    final int serVer = extractSerVer(srcMem);
    final int familyId = extractFamilyID(srcMem);
    final int flags = extractFlags(srcMem);
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) != 0;
    final boolean isGadget = (flags & GADGET_FLAG_MASK) != 0;

    // Check values
    if ((numPreLongs != Family.VAROPT.getMinPreLongs())
            && (numPreLongs != Family.VAROPT.getMaxPreLongs())
            && (numPreLongs != PreambleUtil.VO_WARMUP_PRELONGS)) {
      throw new SketchesArgumentException(
              "Possible corruption: Must have " + Family.VAROPT.getMinPreLongs()
                      + ", " + PreambleUtil.VO_WARMUP_PRELONGS + ", or "
                      + Family.VAROPT.getMaxPreLongs() + " preLongs. Found: " + numPreLongs);
    }
    if (serVer != SER_VER) {
        throw new SketchesArgumentException(
                "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
    }
    final int reqFamilyId = Family.VAROPT.getID();
    if (familyId != reqFamilyId) {
      throw new SketchesArgumentException(
              "Possible Corruption: FamilyID must be " + reqFamilyId + ": " + familyId);
    }

    final int k = extractK(srcMem);
    if (k < 1) {
      throw new SketchesArgumentException("Possible Corruption: k must be at least 1: " + k);
    }

    if (isEmpty) {
      assert numPreLongs == Family.VAROPT.getMinPreLongs();
      return new VarOptItemsSketch<>(k, rf);
    }

    final long n = extractN(srcMem);
    if (n < 0) {
      throw new SketchesArgumentException("Possible Corruption: n cannot be negative: " + n);
    }

    // get rest of preamble
    final int hCount = extractHRegionItemCount(srcMem);
    final int rCount = extractRRegionItemCount(srcMem);

    if (hCount < 0) {
      throw new SketchesArgumentException("Possible Corruption: H region count cannot be "
              + "negative: " + hCount);
    }
    if (rCount < 0) {
      throw new SketchesArgumentException("Possible Corruption: R region count cannot be "
              + "negative: " + rCount);
    }

    double totalRWeight = 0.0;
    if (numPreLongs == Family.VAROPT.getMaxPreLongs()) {
      if (rCount > 0) {
        totalRWeight = extractTotalRWeight(srcMem);
      } else {
        throw new SketchesArgumentException(
                "Possible Corruption: "
                        + Family.VAROPT.getMaxPreLongs() + " preLongs but no items in R region");
      }
    }

    final int preLongBytes = numPreLongs << 3;

    final int totalItems = hCount + rCount;
    int allocatedItems = k + 1; // default to full

    if (rCount == 0) {
      // Not in sampling mode, so determine size to allocate, using ceilingLog2(hCount) as minimum
      final int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(k), "heapify");
      final int minLgSize = Util.toLog2(Util.ceilingPowerOf2(hCount), "heapify");
      final int initialLgSize = SamplingUtil.startingSubMultiple(ceilingLgK, rf.lg(),
              Math.max(minLgSize, MIN_LG_ARR_ITEMS));

      allocatedItems = SamplingUtil.getAdjustedSize(k, 1 << initialLgSize);
      if (allocatedItems == k) {
        ++allocatedItems;
      }
    }

    // allocate full-sized ArrayLists, but we store only hCount weights at any moment
    final long weightOffsetBytes = TOTAL_WEIGHT_R_DOUBLE + (rCount > 0 ? Double.BYTES : 0);
    final ArrayList<Double> weightList = new ArrayList<>(allocatedItems);
    final double[] wts = new double[allocatedItems];
    srcMem.getDoubleArray(weightOffsetBytes, wts, 0, hCount);
    // can't use Arrays.asList(wts) since double[] rather than Double[]
    for (int i = 0; i < hCount; ++ i) {
      if (wts[i] <= 0.0) {
      throw new SketchesArgumentException("Possible Corruption: "
              + "Non-positive weight in heapify(): " + wts[i]);
      }
      weightList.add(wts[i]);
    }

    // marks, if we have a gadget
    long markBytes = 0;
    int markCount = 0;
    ArrayList<Boolean> markList = null;
    if (isGadget) {
      final long markOffsetBytes = preLongBytes + ((long) hCount * Double.BYTES);
      markBytes = ArrayOfBooleansSerDe.computeBytesNeeded(hCount);
      markList = new ArrayList<>(allocatedItems);

      final ArrayOfBooleansSerDe booleansSerDe = new ArrayOfBooleansSerDe();
      final Boolean[] markArray = booleansSerDe.deserializeFromMemory(
              srcMem.region(markOffsetBytes, (hCount >>> 3) + 1), hCount);

      for (Boolean mark : markArray) {
        if (mark) { ++markCount; }
      }
      markList.addAll(Arrays.asList(markArray));
    }

    final long offsetBytes = preLongBytes + ((long) hCount * Double.BYTES) + markBytes;
    final T[] data = serDe.deserializeFromMemory(
            srcMem.region(offsetBytes, srcMem.getCapacity() - offsetBytes), totalItems);
    final List<T> wrappedData = Arrays.asList(data);
    final ArrayList<T> dataList = new ArrayList<>(allocatedItems);
    dataList.addAll(wrappedData.subList(0, hCount));

    // Load items in R as needed
    if (rCount > 0) {
      weightList.add(-1.0); // the gap
      if (isGadget) { markList.add(false); } // the gap
      for (int i = 0; i < rCount; ++i) {
        weightList.add(-1.0);
        if (isGadget) { markList.add(false); }
      }

      dataList.add(null); // the gap
      dataList.addAll(wrappedData.subList(hCount, totalItems));
    }

    final VarOptItemsSketch<T> sketch =
            new VarOptItemsSketch<>(dataList, weightList, k, n,
                    allocatedItems, rf, hCount, rCount, totalRWeight);

    if (isGadget) {
      sketch.marks_ = markList;
      sketch.numMarksInH_ = markCount;
    }

    return sketch;
  }

  /**
   * Returns the sketch's value of <i>k</i>, the maximum number of samples stored in the
   * sketch. The current number of items in the sketch may be lower.
   *
   * @return k, the maximum number of samples in the sketch
   */
  public int getK() {
    return k_;
  }

  /**
   * Returns the number of items processed from the input stream
   *
   * @return n, the number of stream items the sketch has seen
   */
  public long getN() {
    return n_;
  }

  /**
   * Returns the current number of items in the sketch, which may be smaller than the
   * sketch capacity.
   *
   * @return the number of items currently in the sketch
   */
  public int getNumSamples() {
    return Math.min(k_, h_ + r_);
  }

  /**
   * Gets a result iterator object.
   * @return An object with an iterator over the results
   */
  public VarOptItemsSamples<T> getSketchSamples() {
    return new VarOptItemsSamples<>(this);
  }

  /**
   * Randomly decide whether or not to include an item in the sample set.
   *
   * @param item an item of the set being sampled from
   * @param weight a strictly positive weight associated with the item
   */
  public void update(final T item, final double weight) {
    update(item, weight, false);
  }

  /**
   * Resets this sketch to the empty state, but retains the original value of k.
   */
  public void reset() {
    final int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(k_), "VarOptItemsSketch");
    final int initialLgSize =
            SamplingUtil.startingSubMultiple(ceilingLgK, rf_.lg(), MIN_LG_ARR_ITEMS);

    currItemsAlloc_ = SamplingUtil.getAdjustedSize(k_, 1 << initialLgSize);
    data_    = new ArrayList<>(currItemsAlloc_);
    weights_ = new ArrayList<>(currItemsAlloc_);
    if (marks_ != null) {
      marks_ = new ArrayList<>(currItemsAlloc_);
    }

    n_ = 0;
    h_ = 0;
    m_ = 0;
    r_ = 0;
    numMarksInH_ = 0;
    totalWtR_ = 0.0;
  }

  /**
   * Returns a human-readable summary of the sketch.
   *
   * @return A string version of the sketch summary
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    final String thisSimpleName = this.getClass().getSimpleName();

    sb.append(LS);
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   k            : ").append(k_).append(LS);
    sb.append("   h            : ").append(h_).append(LS);
    sb.append("   r            : ").append(r_).append(LS);
    sb.append("   weight_r     : ").append(totalWtR_).append(LS);
    sb.append("   Current size : ").append(currItemsAlloc_).append(LS);
    sb.append("   Resize factor: ").append(rf_).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);

    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a VarOptItemsSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a VarOptItemsSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.preambleToString(byteArr);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a VarOptItemsSketch.
   * @param mem the given Memory
   * @return a human readable string of the preamble of a Memory image of a VarOptItemsSketch.
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
    if ((r_ == 0) && (h_ == 0)) {
      // null class is ok since empty -- no need to call serDe
      return toByteArray(serDe, null);
    } else {
      final int validIndex = (h_ == 0 ? 1 : 0);
      final Class<?> clazz = data_.get(validIndex).getClass();
      return toByteArray(serDe, clazz);
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
    final int preLongs, numMarkBytes, outBytes;
    final boolean empty = (r_ == 0) && (h_ == 0);
    byte[] itemBytes = null; // for serialized items from serDe
    int flags = marks_ == null ? 0 : GADGET_FLAG_MASK;

    if (empty) {
      preLongs = Family.VAROPT.getMinPreLongs();
      outBytes = Family.VAROPT.getMinPreLongs() << 3; // only contains the minimum header info
      flags |= EMPTY_FLAG_MASK;
    } else {
      preLongs = (r_ == 0 ? PreambleUtil.VO_WARMUP_PRELONGS : Family.VAROPT.getMaxPreLongs());
      itemBytes = serDe.serializeToByteArray(getDataSamples(clazz));
      numMarkBytes = marks_ == null ? 0 : ArrayOfBooleansSerDe.computeBytesNeeded(h_);
      outBytes = (preLongs << 3) + (h_ * Double.BYTES) + numMarkBytes + itemBytes.length;
    }
    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.wrap(outArr);

    // build first preLong
    PreambleUtil.insertPreLongs(mem, preLongs);               // Byte 0
    PreambleUtil.insertLgResizeFactor(mem, rf_.lg());
    PreambleUtil.insertSerVer(mem, SER_VER);                  // Byte 1
    PreambleUtil.insertFamilyID(mem, Family.VAROPT.getID());  // Byte 2
    PreambleUtil.insertFlags(mem, flags);                     // Byte 3
    PreambleUtil.insertK(mem, k_);                            // Bytes 4-7

    if (!empty) {
      PreambleUtil.insertN(mem, n_);                                      // Bytes 8-15
      PreambleUtil.insertHRegionItemCount(mem, h_);           // Bytes 16-19
      PreambleUtil.insertRRegionItemCount(mem, r_);           // Bytes 20-23
      if (r_ > 0) {
        PreambleUtil.insertTotalRWeight(mem, totalWtR_);      // Bytes 24-31
      }

      // write the first h_ weights
      int offset = preLongs << 3;
      for (int i = 0; i < h_; ++i) {
        mem.putDouble(offset, weights_.get(i));
        offset += Double.BYTES;
      }

      // write the first h_ marks, iff we have a gadget
      if (marks_ != null) {
        final byte[] markBytes;
        markBytes = MARK_SERDE.serializeToByteArray(marks_.subList(0, h_).toArray(new Boolean[0]));
        mem.putByteArray(offset, markBytes, 0, markBytes.length);
        offset += markBytes.length;
      }

      // write the sample items, using offset from earlier
      mem.putByteArray(offset, itemBytes, 0, itemBytes.length);
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

    if (n_ == 0) {
      return new SampleSubsetSummary(0.0, 0.0, 0.0, 0.0);
    }

    double totalWtH = 0.0;
    double hTrueWeight = 0.0;
    int idx = 0;
    for (; idx < h_; ++idx) {
      final double wt = weights_.get(idx);
      totalWtH += wt;
      if (predicate.test(data_.get(idx))) {
        hTrueWeight += wt;
      }
    }

    // if only heavy items, we have an exact answer
    if (r_ == 0) {
      return new SampleSubsetSummary(hTrueWeight, hTrueWeight, hTrueWeight, hTrueWeight);
    }

    final long numSampled = n_ - h_;
    assert numSampled > 0;
    final double effectiveSamplingRate = r_ / (double) numSampled;
    assert effectiveSamplingRate >= 0.0;
    assert effectiveSamplingRate <= 1.0;

    int rTrueCount = 0;
    ++idx; // skip the gap
    for (; idx < (k_ + 1); ++idx) {
      if (predicate.test(data_.get(idx))) {
        ++rTrueCount;
      }
    }

    final double lbTrueFraction = pseudoHypergeometricLBonP(r_, rTrueCount, effectiveSamplingRate);
    final double estimatedTrueFraction = (1.0 * rTrueCount) / r_;
    final double ubTrueFraction = pseudoHypergeometricUBonP(r_, rTrueCount, effectiveSamplingRate);
    return new SampleSubsetSummary(
            hTrueWeight + (totalWtR_ * lbTrueFraction),
            hTrueWeight + (totalWtR_ * estimatedTrueFraction),
            hTrueWeight + (totalWtR_ * ubTrueFraction),
            totalWtH + totalWtR_);
  }

  /**
   * Returns a VarOptItemsSketch.Result structure containing the items and weights in separate
   * lists. The returned list lengths may be smaller than the total capacity.
   *
   * @return A Result object containing items and weights.
   */
  Result getSamplesAsArrays() {
    if ((r_ + h_) == 0) {
      return null;
    }

    final int validIndex = (h_ == 0 ? 1 : 0);
    final Class<?> clazz = data_.get(validIndex).getClass();
    return getSamplesAsArrays(clazz);
  }

  /**
   * Creates a copy of the sketch, optionally discarding any information about marks that would
   * indicate the class's use as a union gadget as opposed to a valid sketch.
   *
   * @param asSketch If true, copies as a sketch; if false, copies as a union gadget
   * @param adjustedN Target value of n for the resulting sketch. Ignored if negative.
   * @return A copy of the sketch.
   */
  VarOptItemsSketch<T> copyAndSetN(final boolean asSketch, final long adjustedN) {
    final VarOptItemsSketch<T> sketch;
    sketch = new VarOptItemsSketch<>(data_, weights_, k_,n_,
            currItemsAlloc_, rf_, h_, r_, totalWtR_);

    if (!asSketch) {
      sketch.marks_ = this.marks_;
      sketch.numMarksInH_ = this.numMarksInH_;
    }

    if (adjustedN >= 0) {
      sketch.n_ = adjustedN;
    }

    return sketch;
  }

  /**
   * Strips the mark array from the object, making what had been a gadget indistinguishable form
   * a sketch. Avoids an extra copy.
   */
  void stripMarks() {
    assert marks_ != null;
    numMarksInH_ = 0;
    marks_ = null;
  }

  /**
   * Returns a VarOptItemsSketch.Result structure containing the items and weights in separate
   * lists. The returned list lengths may be smaller than the total capacity.
   *
   * <p>This method allocates an array of class <em>clazz</em>, which must either match or
   * be parent of T. This method should be used when objects in the array are all instances of T
   * but are not necessarily instances of the base class.</p>
   *
   * @param clazz A class to which the items are cast before returning
   * @return A Result object containing items and weights.
   */
  @SuppressWarnings("unchecked")
  Result getSamplesAsArrays(final Class<?> clazz) {
    if ((r_ + h_) == 0) {
      return null;
    }

    // are Array.asList(data_.subList()) copies better?
    final int numSamples = getNumSamples();
    final T[] prunedItems = (T[]) Array.newInstance(clazz, numSamples);
    final double[] prunedWeights = new double[numSamples];
    int j = 0;
    final double rWeight = totalWtR_ / r_;
    for (int i = 0; j < numSamples; ++i) {
      final T item = data_.get(i);
      if (item != null) {
        prunedItems[j] = item;
        prunedWeights[j] = (weights_.get(i) > 0 ? weights_.get(i) : rWeight);
        ++j;
      }
    }

    final Result output = new Result();
    output.items = prunedItems;
    output.weights = prunedWeights;

    return output;
  }

  // package-private getters

  // package-private: Relies on ArrayList for bounds checking and assumes caller knows how to handle
  // a null from the middle of the list
  T getItem(final int idx) {
    return data_.get(idx);
  }

  // package-private: Relies on ArrayList for bounds checking and assumes caller knows how to handle
  // a negative value (whether from the null in the middle or an R-region item)
  double getWeight(final int idx) {
    return weights_.get(idx);
  }

  // package-private: Relies on ArrayList for bounds checking and assumes caller knows how to
  // handle a null from the middle of the list.
  boolean getMark(final int idx) { return marks_.get(idx); }

  int getHRegionCount() {
    return h_;
  }

  int getRRegionCount() { return r_; }

  int getNumMarksInH() { return numMarksInH_; }

  // Needed by result object and for unioning
  double getTau() {
    return r_ == 0 ? Double.NaN : (totalWtR_ / r_);
  }

  double getTotalWtR() {
    return totalWtR_;
  }

  // package-private setter, used to resolve gadget into sketch during union
  void forceSetK(final int k) {
    assert k > 0;
    k_ = k;
  }

  /**
   * Internal implementation of update() which requires the user to know if an item is
   * marked as coming from the reservoir region of a sketch. The marks are used only in
   * merging.
   *
   * @param item an item of the set being sampled from
   * @param weight a strictly positive weight associated with the item
   * @param mark true if an item comes from a sketch's reservoir region
   */
  void update(final T item, final double weight, final boolean mark) {
    if (item == null) {
      return;
    }
    if (weight <= 0.0) {
      throw new SketchesArgumentException("Item weights must be strictly positive: "
              + weight + ", for item " + item.toString());
    }
    ++n_;

    if (r_ == 0) {
      // exact mode
      updateWarmupPhase(item, weight, mark);
    } else {
      // sketch is in estimation mode, so we can make the following check
      assert (h_ == 0) || (peekMin() >= getTau());

      // what tau would be if deletion candidates turn out to be R plus the new item
      // note: (r_ + 1) - 1 is intentional
      final double hypotheticalTau = (weight + totalWtR_) / ((r_ + 1) - 1);

      // is new item's turn to be considered for reservoir?
      final boolean condition1 = (h_ == 0) || (weight <= peekMin());

      // is new item light enough for reservoir?
      final boolean condition2 = weight < hypotheticalTau;

      if (condition1 && condition2) {
        updateLight(item, weight, mark);
      } else if (r_ == 1) {
        updateHeavyREq1(item, weight, mark);
      } else {
        updateHeavyGeneral(item, weight, mark);
      }
    }
  }

  /**
   * Decreases sketch's value of k by 1, updating stored values as needed.
   *
   * <p>Subject to certain pre-conditions, decreasing k causes tau to increase. This fact is used by
   * the unioning algorithm to force "marked" items out of H and into the reservoir region.</p>
   */
  void decreaseKBy1() {
    if (k_ <= 1) {
      throw new SketchesStateException("Cannot decrease k below 1 in union");
    }

    if ((h_ == 0) && (r_ == 0)) {
      // exact mode, but no data yet; this reduction is somewhat gratuitous
      --k_;
    } else if ((h_ > 0) && (r_ == 0)) {
      // exact mode, but we have some data
      --k_;
      if (h_ > k_) {
        transitionFromWarmup();
      }
    } else if ((h_ > 0) && (r_ > 0)) {
      // reservoir mode, but we have some exact samples.
      // Our strategy will be to pull an item out of H (which we are allowed to do since it's
      // still just data), reduce k, and then re-insert the item

      // first, slide the R zone to the left by 1, temporarily filling the gap
      final int oldGapIdx = h_;
      final int oldFinalRIdx = (h_ + 1 + r_) - 1;

      assert oldFinalRIdx == k_;
      swapValues(oldFinalRIdx, oldGapIdx);

      // now we pull an item out of H; any item is ok, but if we grab the rightmost and then
      // reduce h_, the heap invariant will be preserved (and the gap will be restored), plus
      // the push() of the item that will probably happen later will be cheap.

      final int pulledIdx = h_ - 1;
      final T pulledItem = data_.get(pulledIdx);
      final double pulledWeight = weights_.get(pulledIdx);
      final boolean pulledMark = marks_.get(pulledIdx);

      if (pulledMark) { --numMarksInH_; }
      weights_.set(pulledIdx, -1.0); // to make bugs easier to spot

      --h_;
      --k_;
      --n_; // will be re-incremented with the update

      update(pulledItem, pulledWeight, pulledMark);
    } else if ((h_ == 0) && (r_ > 0)) {
      // pure reservoir mode, so can simply eject a randomly chosen sample from the reservoir
      assert r_ >= 2;

      final int rIdxToDelete = 1 + SamplingUtil.rand.nextInt(r_); // 1 for the gap
      final int rightmostRIdx = (1 + r_) - 1;
      swapValues(rIdxToDelete, rightmostRIdx);
      weights_.set(rightmostRIdx, -1.0);

      --k_;
      --r_;
    }
  }

  /* In the "light" case the new item has weight <= old_tau, so
     would appear to the right of the R items in a hypothetical reverse-sorted
     list. It is easy to prove that it is light enough to be part of this
     round's downsampling */
  private void updateLight(final T item, final double weight, final boolean mark) {
    assert r_ >= 1;
    assert (r_ + h_) == k_;

    final int mSlot = h_; // index of the gap, which becomes the M region
    data_.set(mSlot, item);
    weights_.set(mSlot, weight);
    if (marks_ != null) { marks_.set(mSlot, mark); }
    ++m_;

    growCandidateSet(totalWtR_ + weight, r_ + 1);
  }

  /* In the "heavy" case the new item has weight > old_tau, so would
     appear to the left of items in R in a hypothetical reverse-sorted list and
     might or might not be light enough be part of this round's downsampling.
     [After first splitting off the R=1 case] we greatly simplify the code by
     putting the new item into the H heap whether it needs to be there or not.
     In other words, it might go into the heap and then come right back out,
     but that should be okay because pseudo_heavy items cannot predominate
     in long streams unless (max wt) / (min wt) > o(exp(N)) */
  private void updateHeavyGeneral(final T item, final double weight, final boolean mark) {
    assert m_ == 0;
    assert r_ >= 2;
    assert (r_ + h_) == k_;

    // put into H, although may come back out momentarily
    push(item, weight, mark);

    growCandidateSet(totalWtR_, r_);
  }

  /* The analysis of this case is similar to that of the general heavy case.
     The one small technical difference is that since R < 2, we must grab an M item
     to have a valid starting point for continue_by_growing_candidate_set () */
  private void updateHeavyREq1(final T item, final double weight, final boolean mark) {
    assert m_ == 0;
    assert r_ == 1;
    assert (r_ + h_) == k_;

    push(item, weight, mark);  // new item into H
    popMinToMRegion();   // pop lightest back into M

    // Any set of two items is downsample-able to one item,
    // so the two lightest items are a valid starting point for the following
    final int mSlot = k_ - 1; // array is k+1, 1 in R, so slot before is M
    growCandidateSet(weights_.get(mSlot) + totalWtR_, 2);
  }

  private void updateWarmupPhase(final T item, final double wt, final boolean mark) {
    assert r_ == 0;
    assert m_ == 0;
    assert h_ <= k_;

    if (h_ >= currItemsAlloc_) {
      growDataArrays();
    }

    // store items as they come in, until full
    data_.add(h_, item);
    weights_.add(h_, wt);
    if (marks_ != null) { marks_.add(h_, mark); }
    ++h_;
    numMarksInH_ += mark ? 1 : 0;

    // check if need to heapify
    if (h_ > k_) {
      transitionFromWarmup();
    }
  }

  private void transitionFromWarmup() {
    // Move 2 lightest items from H to M
    // But the lighter really belongs in R, so update counts to reflect that
    convertToHeap();
    popMinToMRegion();
    popMinToMRegion();
    --m_;
    ++r_;

    assert h_ == (k_ - 1);
    assert m_ == 1;
    assert r_ == 1;

    // Update total weight in R then, having grabbed the value, overwrite in
    // weight_ array to help make bugs more obvious
    totalWtR_ = weights_.get(k_); // only one item, known location
    weights_.set(k_, -1.0);

    // The two lightest items are necessarily downsample-able to one item, and are therefore a
    // valid initial candidate set.
    growCandidateSet(weights_.get(k_ - 1) + totalWtR_, 2);
  }

  /* Validates the heap condition for the weight array */
  /*
  private void validateHeap() {
    for (int j = h_ - 1; j >= 1; --j) {
      final int p = ((j + 1) / 2) - 1;
      assert weights_.get(p) <= weights_.get(j);
    }
  }
  */

  /* Converts the data_ and weights_ arrays to heaps. In contrast to other parts
     of the library, this has nothing to do with on- or off-heap storage or the
     Memory package.
   */
  private void convertToHeap() {
    if (h_ < 2) {
      return; // nothing to do
    }

    final int lastSlot = h_ - 1;
    final int lastNonLeaf = ((lastSlot + 1) / 2) - 1;

    for (int j = lastNonLeaf; j >= 0; --j) {
      restoreTowardsLeaves(j);
    }

    //validateHeap();
  }

  private void restoreTowardsLeaves(final int slotIn) {
    assert h_ > 0;
    final int lastSlot = h_ - 1;
    assert slotIn <= lastSlot;

    int slot = slotIn;
    int child = (2 * slotIn) + 1; // might be invalid, need to check

    while (child <= lastSlot) {
      final int child2 = child + 1; // might also be invalid
      if ((child2 <= lastSlot) && (weights_.get(child2) < weights_.get(child))) {
        // switch to other child if it's both valid and smaller
        child = child2;
      }

      if (weights_.get(slot) <= weights_.get(child)) {
        // invariant holds so we're done
        break;
      }

      // swap and continue
      swapValues(slot, child);

      slot = child;
      child = (2 * slot) + 1; // might be invalid, checked on next loop
    }
  }

  private void restoreTowardsRoot(final int slotIn) {
    int slot = slotIn;
    int p = (((slot + 1) / 2) - 1); // valid if slot >= 1
    while ((slot > 0) && (weights_.get(slot) < weights_.get(p))) {
      swapValues(slot, p);
      slot = p;
      p = (((slot + 1) / 2) - 1); // valid if slot >= 1
    }
  }

  private void push(final T item, final double wt, final boolean mark) {
    data_.set(h_, item);
    weights_.set(h_, wt);
    if (marks_ != null) {
      marks_.set(h_, mark);
      numMarksInH_ += (mark ? 1 : 0);
    }
    ++h_;

    restoreTowardsRoot(h_ - 1); // need use old h_, but want accurate h_
  }

  private double peekMin() {
    assert h_ > 0;
    return weights_.get(0);
  }

  private void popMinToMRegion() {
    assert h_ > 0;
    assert (h_ + m_ + r_) == (k_ + 1);

    if (h_ == 1) {
      // just update bookkeeping
      ++m_;
      --h_;
    } else {
      // main case
      final int tgt = h_ - 1; // last slot, will swap with root
      swapValues(0, tgt);
      ++m_;
      --h_;

      restoreTowardsLeaves(0);
    }

    if (isMarked(h_)) {
      --numMarksInH_;
    }
  }

  /* When entering here we should be in a well-characterized state where the
     new item has been placed in either h or m and we have a valid but not necessarily
     maximal sampling plan figured out. The array is completely full at this point.
     Everyone in h and m has an explicit weight. The candidates are right-justified
     and are either just the r set or the r set + exactly one m item. The number
     of cands is at least 2. We will now grow the candidate set as much as possible
     by pulling sufficiently light items from h to m.
   */
  private void growCandidateSet(double wtCands, int numCands) {
    assert (h_ + m_ + r_) == (k_ + 1);
    assert numCands >= 2;       // essential
    assert numCands == (m_ + r_); // essential
    assert (m_ == 0) || (m_ == 1);

    while (h_ > 0) {
      final double nextWt = peekMin();
      final double nextTotWt = wtCands + nextWt;

      // test for strict lightness of next prospect (denominator multiplied through)
      // ideally: (nextWt * (nextNumCands-1) < nextTotWt) but can just
      //          use numCands directly
      if ((nextWt * numCands) < nextTotWt) {
        wtCands = nextTotWt;
        ++numCands;
        popMinToMRegion(); // adjusts h_ and m_
      } else {
        break;
      }
    }

    downsampleCandidateSet(wtCands, numCands);
  }

  private int pickRandomSlotInR() {
    assert r_ > 0;
    final int offset = h_ + m_;
    if (r_ == 1) {
      return offset;
    } else {
      return offset + SamplingUtil.rand.nextInt(r_);
    }
  }

  private int chooseDeleteSlot(final double wtCand, final int numCand) {
    assert r_ > 0;

    if (m_ == 0) {
      // this happens if we insert a really heavy item
      return pickRandomSlotInR();
    } else if (m_ == 1) {
      // check if we keep the item in M or pick one from R
      // p(keep) = (numCand - 1) * wt_M / wt_cand
      final double wtMCand = weights_.get(h_); // slot of item in M is h_
      if ((wtCand * SamplingUtil.nextDoubleExcludeZero()) < ((numCand - 1) * wtMCand)) {
        return pickRandomSlotInR(); // keep item in M
      } else {
        return h_; // index of item in M
      }
    } else {
      // general case
      final int deleteSlot = chooseWeightedDeleteSlot(wtCand, numCand);
      final int firstRSlot = h_ + m_;
      if (deleteSlot == firstRSlot) {
        return pickRandomSlotInR();
      } else {
        return deleteSlot;
      }
    }
  }

  private int chooseWeightedDeleteSlot(final double wtCand, final int numCand) {
    assert m_ >= 1;

    final int offset = h_;
    final int finalM = (offset + m_) - 1;
    final int numToKeep = numCand - 1;

    double leftSubtotal = 0.0;
    double rightSubtotal = -1.0 * wtCand * SamplingUtil.nextDoubleExcludeZero();

    for (int i = offset; i <= finalM; ++i) {
      leftSubtotal += numToKeep * weights_.get(i);
      rightSubtotal += wtCand;

      if (leftSubtotal < rightSubtotal) {
        return i;
      }
    }

    // this slot tells caller that we need to delete out of R
    return finalM + 1;
  }

  private void downsampleCandidateSet(final double wtCands, final int numCands) {
    assert numCands >= 2;
    assert (h_ + numCands) == (k_ + 1);

    // need this before overwriting anything
    final int deleteSlot = chooseDeleteSlot(wtCands, numCands);
    final int leftmostCandSlot = h_;
    assert deleteSlot >= leftmostCandSlot;
    assert deleteSlot <= k_;

    // overwrite weights for items from M moving into R, to make bugs more obvious
    final int stopIdx = leftmostCandSlot + m_;
    for (int j = leftmostCandSlot; j < stopIdx; ++j) {
      weights_.set(j, -1.0);
    }

    // The next two lines work even when deleteSlot == leftmostCandSlot
    data_.set(deleteSlot, data_.get(leftmostCandSlot));
    data_.set(leftmostCandSlot, null);

    m_ = 0;
    r_ = numCands - 1;
    totalWtR_ = wtCands;
  }

  /* swap values of data_, weights_, and marks between src and dst indices */
  private void swapValues(final int src, final int dst) {
    final T item = data_.get(src);
    data_.set(src, data_.get(dst));
    data_.set(dst, item);

    final Double wt = weights_.get(src);
    weights_.set(src, weights_.get(dst));
    weights_.set(dst, wt);

    if (marks_ != null) {
      final Boolean mark = marks_.get(src);
      marks_.set(src, marks_.get(dst));
      marks_.set(dst, mark);
    }
  }

  private boolean isMarked(final int idx) {
    return marks_ != null ? marks_.get(idx) : false;
  }

  /**
   * Returns a copy of the items (no weights) in the sketch as members of Class <em>clazz</em>,
   * or null if empty. The returned array length may be smaller than the total capacity.
   *
   * <p>This method allocates an array of class <em>clazz</em>, which must either match or
   * extend T. This method should be used when objects in the array are all instances of T but
   * are not necessarily instances of the base class.</p>
   *
   * @param clazz A class to which the items are cast before returning
   * @return A copy of the sample array
   */
  @SuppressWarnings("unchecked")
  private T[] getDataSamples(final Class<?> clazz) {
    assert (h_ + r_) > 0;

    // are 2 Array.asList(data_.subList()) copies better?
    final T[] prunedList = (T[]) Array.newInstance(clazz, getNumSamples());
    int i = 0;
    for (T item : data_) {
      if (item != null) {
        prunedList[i++] = item;
      }
    }
    return prunedList;
  }

  /**
   * Increases allocated sampling size by (adjusted) ResizeFactor and copies items from old
   * sampling. Only happens when buffer is not full, so don't need to worry about blindly copying
   * the array items.
   */
  private void growDataArrays() {
    currItemsAlloc_ = SamplingUtil.getAdjustedSize(k_, currItemsAlloc_ << rf_.lg());
    if (currItemsAlloc_ == k_) {
      ++currItemsAlloc_;
    }

    data_.ensureCapacity(currItemsAlloc_);
    weights_.ensureCapacity(currItemsAlloc_);
    if (marks_ != null) {
      marks_.ensureCapacity(currItemsAlloc_);
    }
  }
}
