package com.yahoo.sketches.quantiles;

import java.util.Arrays;
import java.util.Random;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per bit.
 * See https://arxiv.org/abs/1603.05346v2
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
public class KllFloatsSketch {

  /*
   * Data is stored in items_.
   * The data for level i lies in positions levels_[i] through levels_[i + 1] - 1 inclusive.
   * Hence levels_ must contain (numLevels_ + 1) indices.
   * The valid portion of items_ is completely packed, except for level 0.
   * Level 0 is filled from the top down.
   *
   * Invariants:
   * 1) After a compaction, or an update, or a merge, all levels are sorted except for level zero.
   * 2) After a compaction, (sum of capacities) - (sum of items) >= 1,
   *  so there is room for least 1 more item in level zero.
   * 3) There are no gaps except at the bottom, so if levels_[0] = 0, 
   *  the sketch is exactly filled to capacity and must be compacted.
   */

  private static final Random random = new Random();

  public static final int DEFAULT_K = 256;
  public static final int DEFAULT_M = 8;

  private final int k_;
  private final int m_; // minimum buffer "width"

  private long n_;
  private int numLevels_;
  private int[] levels_;
  private float[] items_;
  private float minValue_;
  private float maxValue_;
  private boolean isLevelZeroSorted_;

  public KllFloatsSketch() {
    this(DEFAULT_K);
  }

  public KllFloatsSketch(final int k) {
    this(k, DEFAULT_M);
  }

  private KllFloatsSketch(final int k, final int m) {
    k_ = k;
    m_ = m;
    // TODO: check k
    numLevels_ = 1;
    levels_ = new int[] {k, k};
    items_ = new float[k];
    minValue_ = Float.NaN;
    maxValue_ = Float.NaN;
    isLevelZeroSorted_ = false;
  }

  public long getN() {
    return n_;
  }

  public boolean isEmpty() {
    return n_ == 0;
  }

  public int getNumRetained() {
    return levels_[numLevels_] - levels_[0];
  }

  public boolean isEstimationMode() {
    return numLevels_ > 1;
  }

  public void update(final float value) {
    if (Float.isNaN(value)) { return; }
    if (n_ == 0) {
      minValue_ = value;
      maxValue_ = value;
    } else {
      if (value < minValue_) { minValue_ = value; }
      if (value > maxValue_) { maxValue_ = value; }
    }
    if (levels_[0] == 0) {
      compressWhileUpdating();
    }
    n_++;
    isLevelZeroSorted_ = false;
    final int nextPos = levels_[0] - 1;
    assert levels_[0] >= 0;
    levels_[0] = nextPos;
    items_[nextPos] = value;
    //if (nextPos == 0) {
    //  compressWhileUpdating();
    //}
  }

  public void merge(final KllFloatsSketch other) {
    final long finalN = n_ + other.n_;
    if (other.numLevels_ >= 1) {
      for (int i = other.levels_[0]; i < other.levels_[1]; i++) {
        update(other.items_[i]);
      }
    }
    if (other.numLevels_ >= 2) {
      mergeHigherLevels(other, finalN);
    }
    n_ = finalN;
    assert_correct_total_weight();
    minValue_ = Math.min(minValue_, other.minValue_);
    maxValue_ = Math.max(maxValue_, other.maxValue_);
  }

  public float getMinValue() {
    return minValue_;
  }

  public float getMaxValue() {
    return maxValue_;
  }

  public float getQuantile(final double fraction) {
    if (isEmpty()) { return Float.NaN; }
    if (fraction == 0.0) { return minValue_; }
    if (fraction == 1.0) { return maxValue_; }
    if ((fraction < 0.0) || (fraction > 1.0)) {
      throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
    }
    final KllFloatsQuantileCalculator quant = getQuantileCalculator();
    return quant.getQuantile(fraction);
  }

  public float[] getQuantiles(final double[] fractions) {
    if (isEmpty()) { return null; }
    Util.validateFractions(fractions);
    KllFloatsQuantileCalculator quant = null;
    final float[] quantiles = new float[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      final double fraction = fractions[i];
      if      (fraction == 0.0) { quantiles[i] = minValue_; }
      else if (fraction == 1.0) { quantiles[i] = maxValue_; }
      else {
        if (quant == null) {
          quant = getQuantileCalculator();
        }
        quantiles[i] = quant.getQuantile(fraction);
      }
    }
    return quantiles;
  }

  public double getRank(final float value) {
    if (isEmpty()) { return Double.NaN; }
    int level = 0;
    int weight = 1;
    long total = 0;
    while (level < numLevels_) {
      final int fromIndex = levels_[level];
      final int toIndex = levels_[level + 1]; // exclusive
      for (int i = fromIndex; i < toIndex; i++) {
        if (items_[i] < value) {
          total += weight;
        } else if (level > 0) {
          break; // levels above 0 are sorted, no point comparing further
        }
      }
      level++;
      weight *= 2;
    }
    return (double) total / n_;
  }

  public double[] getPMF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, false);
  }

  public double[] getCDF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, true);
  }

  public double getNormalizedRankError() {
    return getNormalizedRankError(k_);
  }

  // constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials
  public static double getNormalizedRankError(final int k) {
    return 2.446 / Math.pow(k, 0.943);
  }

  public int getSerializedSizeBytes() {
    return 16 + getNumRetained() * Float.BYTES + (numLevels_ + 1) * Integer.BYTES;
  }

  @Override
  public String toString() {
    return toString(false, false);
  }
  
  public String toString(final boolean withLevels, final boolean withData) {
    final String epsilonPct = String.format("%.3f%%", getNormalizedRankError() * 100);
    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL sketch summary:").append(Util.LS);
    sb.append("   K                    : ").append(k_).append(Util.LS);
    sb.append("   Normalized Rank Error: ").append(epsilonPct).append(Util.LS);
    sb.append("   M                    : ").append(m_).append(Util.LS);
    sb.append("   Empty                : ").append(isEmpty()).append(Util.LS);
    sb.append("   Estimation Mode      : ").append(isEstimationMode()).append(Util.LS);
    sb.append("   N                    : ").append(n_).append(Util.LS);
    sb.append("   Levels               : ").append(numLevels_).append(Util.LS);
    sb.append("   Sorted               : ").append(isLevelZeroSorted_).append(Util.LS);
    sb.append("   Buffer Capacity Items: ").append(items_.length).append(Util.LS);
    sb.append("   Retained Items       : ").append(getNumRetained()).append(Util.LS);
    sb.append("   Storage Bytes        : ").append(getSerializedSizeBytes()).append(Util.LS);
    sb.append("   Min Value            : ").append(minValue_).append(Util.LS);
    sb.append("   Max Value            : ").append(maxValue_).append(Util.LS);
    sb.append("### End sketch summary").append(Util.LS);

    if (withLevels) {
      sb.append("### KLL sketch levels:").append(Util.LS)
      .append("index: nominal capacity, actual size").append(Util.LS);
      for (int i = 0; i < numLevels_; i++) {
        sb.append("   ").append(i).append(": ").append(levelCapacity(k_, numLevels_, i, m_))
        .append(", ").append(safe_level_size(i)).append(Util.LS);
      }
      sb.append("### End sketch levels").append(Util.LS);
    }

    // withData is not implemented yet

    return sb.toString();
  }

  private KllFloatsQuantileCalculator getQuantileCalculator() {
    sortLevelZero(); // sort in the sketch to reuse if possible
    return new KllFloatsQuantileCalculator(items_, levels_, numLevels_, n_); 
  }

  private double[] getPmfOrCdf(final float[] splitPoints, boolean isCdf) {
    if (isEmpty()) { return null; }
    final double[] buckets = new double[splitPoints.length + 1];
    int level = 0;
    int weight = 1;
    while (level < numLevels_) {
      final int fromIndex = levels_[level];
      final int toIndex = levels_[level + 1]; // exclusive
      if (level == 0 && !isLevelZeroSorted_) {
        incrementBucketsUnsortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      } else {
        incrementBucketsSortedLevel(fromIndex, toIndex, weight, splitPoints, buckets);
      }
      level++;
      weight *= 2;
    }
    // normalize and, if CDF, convert to cumulative
    if (isCdf) {
      double subtotal = 0;
      for (int i = 0; i < buckets.length; i++) {
        subtotal += buckets[i];
        buckets[i] = subtotal / n_;
      }
    } else {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] /= n_;
      }
    }
    return buckets;
  }

  private void incrementBucketsUnsortedLevel(final int fromIndex, final int toIndex, final int weight, final float[] splitPoints, final double[] buckets) {
    for (int i = fromIndex; i < toIndex; i++) {
      int j;
      for (j = 0; j < splitPoints.length; j++) {
        if (items_[i] < splitPoints[j]) {
          break;
        }
      }
      buckets[j] += weight;
    }
  }

  private void incrementBucketsSortedLevel(final int fromIndex, final int toIndex, final int weight, final float[] splitPoints, final double[] buckets) {
    int i = fromIndex;
    int j = 0;
    while (i <  toIndex && j < splitPoints.length) {
      if (items_[i] < splitPoints[j]) {
        buckets[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket
      }
    }
    // now either i == toIndex (we are out of samples), or
    // j == numSplitPoints (we are out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case
    if (j == splitPoints.length) {
      buckets[j] += weight * (toIndex - i);
    }
  }

  // The following code is only valid in the special case of exactly reaching capacity while updating. 
  // It cannot be used while merging, while reducing k, or anything else.
  private void compressWhileUpdating() {
    final int level = findLevelToCompact();

    // It is important to do add the new top level right here. Be aware that this operation grows the buffer
    // and shifts the data and also the boundaries of the data and grows the levels array and increments numLevels_
    if (level == numLevels_ - 1) {
      addEmptyTopLevelToCompletelyFullSketch();
    }

    final int rawBeg = levels_[level];
    final int rawLim = levels_[level + 1];
    final int popAbove = levels_[level + 2] - rawLim; // +2 is OK because we already added a new top level if necessary
    final int rawPop = rawLim - rawBeg;
    final boolean oddPop = is_odd(rawPop);
    final int adjBeg = oddPop ? rawBeg + 1 : rawBeg;
    final int adjPop = oddPop ? rawPop - 1 : rawPop;
    final int halfAdjPop = adjPop / 2;

    // level zero might not be sorted, so we must sort it if we wish to compact it
    if (level == 0) {
      sortLevelZero();
    }
    if (popAbove == 0) {
      randomlyHalveUp(items_, adjBeg, adjPop);
    } else {
      randomlyHalveDown(items_, adjBeg, adjPop);
      mergeSortedArrays(items_, adjBeg, halfAdjPop, items_, rawLim, popAbove, items_, adjBeg + halfAdjPop);
    }
    levels_[level + 1] -= halfAdjPop; // adjust boundaries of the level above
    if (oddPop) {
      levels_[level] = levels_[level + 1] - 1; // the current level now contains one item
      items_[levels_[level]] = items_[rawBeg]; // namely this leftover guy
    } else {
      levels_[level] = levels_[level + 1]; // the current level is now empty
    }

    // verify that we freed up halfAdjPop array slots just below the current level
    assert levels_[level] == rawBeg + halfAdjPop;

    // finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero
    if (level > 0) {
      final int amount = rawBeg - levels_[0];
      System.arraycopy(items_, levels_[0], items_, levels_[0] + halfAdjPop, amount);
      for (int lvl = 0; lvl < level; lvl++) {
        levels_[lvl] += halfAdjPop;
      }
    }
  }

  private static void mergeSortedArrays(final float[] buf_a, final int start_a, final int len_a, final float[] buf_b,
      final int start_b, final int len_b, final float[] buf_c, final int start_c) {
    final int len_c = len_a + len_b;
    final int lim_a = start_a + len_a;
    final int lim_b = start_b + len_b;
    final int lim_c = start_c + len_c;

    int a = start_a;
    int b = start_b;

    for (int c = start_c; c < lim_c; c++) {
      if (a == lim_a) {
        buf_c[c] = buf_b[b];
        b++;
      } else if (b == lim_b) {
        buf_c[c] = buf_a[a];
        a++;
      } else if (buf_a[a] < buf_b[b]) { 
        buf_c[c] = buf_a[a];
        a++;
      } else {
        buf_c[c] = buf_b[b];
        b++;
      }
    }
    assert a == lim_a;
    assert b == lim_b;
  }

  private static void randomlyHalveDown(final float[] buf, final int start, final int length) {
    assert is_even(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);
    int j = start + offset;
    for (int i = start; i < start + half_length; i++) {
      buf[i] = buf[j];
      j += 2;
    }
  }

  private static void randomlyHalveUp(final float[] buf, final int start, final int length) {
    assert is_even(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);
    int j = start + length - 1 - offset;
    for (int i = start + length - 1; i >= start + half_length; i--) {
      buf[i] = buf[j];
      j -= 2;
    }
  }

  private static boolean is_even(final int value) {
    return (value & 1) == 0;
  }

  private static boolean is_odd(final int value) {
    return (value & 1) > 0;
  }

  private int findLevelToCompact() {
    int level = 0;
    while (true) {
      assert level < numLevels_;
      final int pop = levels_[level + 1] - levels_[level];
      final int cap = levelCapacity(k_, numLevels_, level, m_);
      if (pop >= cap) {
        return level;
      }
      level++;
    }
  }

  private void addEmptyTopLevelToCompletelyFullSketch() {
    final int cur_total_cap = levels_[numLevels_];

    // make sure that we are following a certain growth scheme
    assert levels_[0] == 0; 
    assert items_.length == cur_total_cap;

    // note that merging MIGHT over-grow levels_, in which case we might not have to grow it here
    if (levels_.length < numLevels_ + 2) {
      levels_ = grow_int_array(levels_, numLevels_ + 2);
    }

    final int delta_cap = levelCapacity(k_, numLevels_ + 1, 0, m_);
    final int new_total_cap = cur_total_cap + delta_cap;

    final float[] new_buf = new float[new_total_cap];

    // copy (and shift) the current data into the new buffer
    System.arraycopy(items_, levels_[0], new_buf, levels_[0] + delta_cap, cur_total_cap);
    items_ = new_buf;

    // this loop includes the old "extra" index at the top
    for (int i = 0; i <= numLevels_; i++) {
      levels_[i] += delta_cap;
    }

    assert levels_[numLevels_] == new_total_cap;

    numLevels_++;
    levels_[numLevels_] = new_total_cap; // initialize the new "extra" index at the top
  }

  private void sortLevelZero() {
    if (!isLevelZeroSorted_) {
      Arrays.sort(items_, levels_[0], levels_[1]);
      isLevelZeroSorted_ = true;
    }
  }

  private void mergeHigherLevels(final KllFloatsSketch other, final long final_n) {
    int tmp_space_needed = getNumRetained() + other.getNumRetainedAboveLevelZero();
    final float[] workbuf = new float[tmp_space_needed];
    final int ub = ub_on_num_levels(k_, final_n);
    final int[] worklevels = new int[ub + 2]; // ub+1 does not work
    final int[] outlevels  = new int[ub + 2];

    final int provisional_num_levels = Math.max(numLevels_, other.numLevels_);

    populate_work_arrays(other, workbuf, worklevels, provisional_num_levels);

    // notice that workbuf is being used as both the input and output here
    final int[] result = generalCompress(k_, m_, provisional_num_levels, workbuf, worklevels, workbuf,
        outlevels, isLevelZeroSorted_);
    final int final_num_levels = result[0];
    final int final_capacity = result[1];
    final int final_pop = result[2];

    assert (final_num_levels <= ub); // can sometimes be 3 bigger, which is a bit surprising

    // now we need to transfer the results back into the "self" sketch
    final float[] newbuf = final_capacity == items_.length ? items_ : new float[final_capacity];
    final int free_space_at_bottom = final_capacity - final_pop;
    System.arraycopy(workbuf, outlevels[0], newbuf, free_space_at_bottom, final_pop);
    final int the_shift = free_space_at_bottom - outlevels[0];

    if (levels_.length < (final_num_levels + 1)) {
      // either of the following lines of code should work
      levels_ = outlevels;
      // levels_ = new int[final_num_levels + 1];
    }

    for (int lvl = 0; lvl < final_num_levels + 1; lvl++) { // includes the "extra" index
      levels_[lvl] = outlevels[lvl] + the_shift;
    }

    items_ = newbuf;
    numLevels_ = final_num_levels;
  }

  private void populate_work_arrays(final KllFloatsSketch other, final float[] workbuf, final int[] worklevels, final int provisional_num_levels) {
    worklevels[0] = 0;

    // Note: the level zero data from "other" was already inserted into "self"
    final int self_pop_zero = safe_level_size(0);
    System.arraycopy(items_, levels_[0], workbuf, worklevels[0], self_pop_zero);
    worklevels[1] = worklevels[0] + self_pop_zero;

    for (int lvl = 1; lvl < provisional_num_levels; lvl++) {
      final int self_pop = safe_level_size(lvl);
      final int other_pop = other.safe_level_size(lvl);
      worklevels[lvl + 1] = worklevels[lvl] + self_pop + other_pop;

      if (self_pop > 0 && other_pop == 0) {
        System.arraycopy(items_, levels_[lvl], workbuf, worklevels[lvl], self_pop);
      } else if (self_pop == 0 && other_pop > 0) {
        System.arraycopy(other.items_, other.levels_[lvl], workbuf, worklevels[lvl], other_pop);
      } else if (self_pop > 0 && other_pop > 0) {
        mergeSortedArrays(items_, levels_[lvl], self_pop, other.items_, other.levels_[lvl], other_pop, workbuf, worklevels[lvl]);
      }
    }
  }

  private int safe_level_size(final int level) {
    if (level >= numLevels_) { return 0; }
    return levels_[level + 1] - levels_[level];
  }

  private static int ub_on_num_levels(final int k, final long n) {
    if (n < k) { return 1; }
    return 2 + floor_of_log2_of_fraction(n, k);
  }

  static int floor_of_log2_of_fraction(final long numer, long denom) {
    if (denom > numer) { return 0; }
    int count = 0;
    while (true) {
      denom <<= 1;
      if (denom > numer) { return count; }
      count++;
    }
  }

 private int getNumRetainedAboveLevelZero() {
    if (numLevels_ == 1) { return 0; }
    return levels_[numLevels_] - levels_[1];
  }

  private static int[] grow_int_array(final int[] old_arr, final int new_len) {
    final int old_len = old_arr.length;
    assert new_len > old_len;
    final int[] new_arr = new int[new_len];
    System.arraycopy(old_arr, 0, new_arr, 0, old_len);
    return new_arr;
  }

  private void assert_correct_total_weight() {
    long total = sum_the_sample_weights(numLevels_, levels_);
    assert total == n_;
  }

  private static long sum_the_sample_weights(final int num_levels, final int[] levels) {
    long total = 0;
    long weight = 1;
    for (int lvl = 0; lvl < num_levels; lvl++) {
      total += weight * (levels[lvl + 1] - levels[lvl]);
      weight *= 2;
    }
    return total;
  }

  // from IntGeneralCompress

  private static final double gloC = 2.0 / 3.0;

  private static int levelCapacity(final int k, final int numLevels, final int height, final int minWid) {
    assert height >= 0;
    assert height < numLevels;
    final int d = numLevels - height - 1;
    return Math.max(minWid, (int) Math.ceil(k * Math.pow(gloC, d)));
  }

  /*
     Here is what we do for each level:
     If it does not need to be compacted, then simply copy it over.

     Otherwise, it does need to be compacted, so...
       Copy zero or one guy over.
       If the level above is empty, halve up.
       Else the level above is nonempty, so...
            halve down, then merge up.
       Adjust the boundaries of the level above.

   * It can be proved that generalCompress returns a sketch that satisfies the space constraints no matter how much data is passed in
   * We are pretty sure that it works correctly when inBuf and outBuf are the same.
   * All levels except for level zero must be sorted before calling this, and will still be sorted afterwards.
   * Level zero is not required to be sorted before, and may not be sorted afterwards.

   * trashes inBuf and inLevels
   * modifies outBuf and outLevels

   * returns (finalNumLevels, finalCapacity, finalItemCount)
   */
  private static int[] generalCompress(final int k, final int m, final int numLevelsIn, final float[] inBuf,
      final int[] inLevels, final float[] outBuf, final int[] outLevels, final boolean isLevelZeroSorted) {
    assert numLevelsIn > 0; // things are too weird if zero levels are allowed
    int numLevels = numLevelsIn;
    int currentItemCount = inLevels[numLevels] - inLevels[0]; // decreases with each compaction
    int targetItemCount = computeTargetItemCount(k, m, numLevels); // increases if we add levels
    boolean done_yet = false;
    outLevels[0] = 0;
    int curLevel = -1;
    while (!done_yet) {
      curLevel++; // start out at level 0

      // If we are at the current top level, add an empty level above it for convenience,
      // but do not increment numLevels until later
      if (curLevel == numLevels - 1) {
        inLevels[curLevel + 2] = inLevels[curLevel + 1];
      }

      final int rawBeg = inLevels[curLevel];
      final int rawLim = inLevels[curLevel + 1];
      final int rawPop = rawLim - rawBeg;

      if (currentItemCount < targetItemCount || rawPop < levelCapacity(k, numLevels, curLevel, m)) {
        // copy level over as is
        // because inBuf and outBuf could be the same, make sure we are not moving data upwards!
        assert (rawBeg >= outLevels[curLevel]);
        System.arraycopy(inBuf, rawBeg, outBuf, outLevels[curLevel], rawPop);
        outLevels[curLevel + 1] = outLevels[curLevel] + rawPop;
      }
      else {
        // The sketch is too full AND this level is too full, so we compact it
        // Note: this can add a level and thus change the sketches capacities

        final int popAbove = inLevels[curLevel + 2] - rawLim;
        final boolean oddPop = is_odd(rawPop);
        final int adjBeg = oddPop ? 1 + rawBeg : rawBeg;
        final int adjPop = oddPop ? rawPop - 1 : rawPop;
        final int halfAdjPop = adjPop / 2;

        if (oddPop) { // copy one guy over
          outBuf[outLevels[curLevel]] = inBuf[rawBeg];
          outLevels[curLevel + 1] = outLevels[curLevel] + 1;
        } else { // copy zero guys over
          outLevels[curLevel + 1] = outLevels[curLevel];
        }

        // level zero might not be sorted, so we must sort it if we wish to compact it
        if (curLevel == 0 && !isLevelZeroSorted) {
          Arrays.sort(inBuf, adjBeg, adjBeg + adjPop);
        }

        if (popAbove == 0) { // Level above is empty, so halve up
          randomlyHalveUp(inBuf, adjBeg, adjPop);
        } else { // Level above is nonempty, so halve down, then merge up
          randomlyHalveDown(inBuf, adjBeg, adjPop);
          mergeSortedArrays(inBuf, adjBeg, halfAdjPop, inBuf, rawLim, popAbove, inBuf, adjBeg + halfAdjPop);
        }

        // track the fact that we just eliminated some data
        currentItemCount -= halfAdjPop;

        // Adjust the boundaries of the level above
        inLevels[curLevel + 1] = inLevels[curLevel + 1] - halfAdjPop;

        // Increment numLevels if we just compacted the old top level
        // This creates some more capacity (the size of the new bottom level)
        if (curLevel == numLevels - 1) {
          numLevels++;
          targetItemCount += levelCapacity(k, numLevels, 0, m);
        }

      } // end of code for compacting a level

      // determine whether we have processed all levels yet (including any new levels that we created)

      if (curLevel == numLevels - 1) { done_yet = true; }

    } // end of loop over levels

    assert outLevels[numLevels] - outLevels[0] == currentItemCount;

    return new int[] {numLevels, targetItemCount, currentItemCount};
  }

  private static int computeTargetItemCount(int k, int m, int numLevels) {
    int total = 0;
    for (int h = 0; h < numLevels - 1; h++) {
      total += levelCapacity(k, numLevels, h, m);
    }
    return total;
  }

  // for debugging
  int[] getLevelSizes() {
    final int[] levels = new int[numLevels_];
    for (int i = 0; i < numLevels_; i++) {
      levels[i] = levels_[i + 1] - levels_[i];
    }
    return levels;
  }

  int[] getNominalLevelCapacities() {
    final int[] caps = new int[numLevels_];
    for (int i = 0; i < numLevels_; i++) {
      caps[i] = levelCapacity(k_, numLevels_, i, m_);
    }
    return caps;
  }

}
