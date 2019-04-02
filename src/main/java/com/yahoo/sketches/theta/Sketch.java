/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.idToFamily;
import static com.yahoo.sketches.HashOperations.count;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.zeroPad;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.BinomialBoundsN;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * The top-level class for all sketches. This class is never constructed directly.
 * Use the UpdateSketch.builder() methods to create UpdateSketches.
 *
 * @author Lee Rhodes
 */
public abstract class Sketch {

  static final int DEFAULT_LG_RESIZE_FACTOR = 3;   //Unique to Heap

  Sketch() {}

  //public static factory constructor-type methods

  /**
   * Heapify takes the sketch image in Memory and instantiates an on-heap
   * Sketch using the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem an image of a Sketch where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Heap-based Sketch from the given Memory
   */
  public static Sketch heapify(final Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify takes the sketch image in Memory and instantiates an on-heap
   * Sketch using the given seed.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem an image of a Sketch where the image seed hash matches the given seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * Compact sketches store a 16-bit hash of the seed, but not the seed itself.
   * @return a Heap-based Sketch from the given Memory
   */
  public static Sketch heapify(final Memory srcMem, final long seed) {
    final int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer == 3) {
      final byte famID = srcMem.getByte(FAMILY_BYTE);
      final boolean ordered = (srcMem.getByte(FLAGS_BYTE) & ORDERED_FLAG_MASK) != 0;
      return constructHeapSketch(famID, ordered, srcMem, seed);
    }
    if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, seed);
    }
    if (serVer == 2) {
      return ForwardCompatibility.heapify2to3(srcMem, seed);
    }
    throw new SketchesArgumentException("Unknown Serialization Version: " + serVer);
  }

  /**
   * Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
   * the java heap.  Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct objects can be wrapped. This method assumes the
   * {@link Util#DEFAULT_UPDATE_SEED}.
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * @param srcMem an image of a Sketch where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Sketch backed by the given Memory
   */
  public static Sketch wrap(final Memory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
   * the java heap.  Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct objects can be wrapped.
   * An attempt to "wrap" earlier version sketches will result in a "heapified", normal
   * Java Heap version of the sketch where all data will be copied to the heap.
   * @param srcMem an image of a Sketch where the image seed hash matches the given seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * Compact sketches store a 16-bit hash of the seed, but not the seed itself.
   * @return a UpdateSketch backed by the given Memory
   */
  public static Sketch wrap(final Memory srcMem, final long seed) {
    final int  preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
    final int familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
    final Family family = Family.idToFamily(familyID);
    switch (family) {
      case QUICKSELECT: { //Hash Table structure
        if ((serVer == 3) && (preLongs == 3)) {
          return DirectQuickSelectSketchR.readOnlyWrap(srcMem, seed);
        } else {
          throw new SketchesArgumentException(
              "Corrupted: " + family + " family image: must have SerVer = 3 and preLongs = 3");
        }
      }
      case COMPACT: { //serVer 1, 2, or 3, preLongs = 1, 2, or 3
        if (serVer == 1) {
          return ForwardCompatibility.heapify1to3(srcMem, seed);
        }
        else if (serVer == 2) {
          return ForwardCompatibility.heapify2to3(srcMem, seed);
        }
        final int flags = srcMem.getByte(FLAGS_BYTE);
        final boolean compact = (flags & COMPACT_FLAG_MASK) > 0; //used for corruption check
        final boolean ordered = (flags & ORDERED_FLAG_MASK) > 0;
        if (compact) {
            return ordered ? DirectCompactOrderedSketch.wrapInstance(srcMem, seed)
                           : DirectCompactUnorderedSketch.wrapInstance(srcMem, seed);
        }
        throw new SketchesArgumentException(
            "Corrupted: " + family + " family image must have compact flag set");
      }
      default: throw new SketchesArgumentException(
          "Sketch cannot wrap family: " + family + " as a Sketch");
    }
  }

  //Sketch interface, defined here with Javadocs

  /**
   * Converts this sketch to an ordered CompactSketch on the Java heap.
   *
   * <p>If this sketch is already in compact form this operation returns <i>this</i>.
   *
   * @return this sketch as an ordered CompactSketch on the Java heap.
   */
  public abstract CompactSketch compact();

  /**
   * Convert this sketch to a CompactSketch in the chosen form.
   *
   * <p>If this sketch is already in compact form this operation returns <i>this</i>.
   *
   * <p>Otherwise, this compacting process converts the hash table form of an UpdateSketch to
   * a simple list of the valid hash values from the hash table.  Any hash values equal to or
   * greater than theta will be discarded.  The number of valid values remaining in the
   * Compact Sketch depends on a number of factors, but may be larger or smaller than
   * <i>Nominal Entries</i> (or <i>k</i>). It will never exceed 2<i>k</i>.  If it is critical
   * to always limit the size to no more than <i>k</i>, then <i>rebuild()</i> should be called
   * on the UpdateSketch prior to this.
   *
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   *
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   *
   * @return this sketch as a CompactSketch in the chosen form
   */
  public abstract CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem);

  /**
   * Gets the number of hash values less than the given theta.
   * @param theta the given theta as a double between zero and one.
   * @return the number of hash values less than the given theta.
   */
  public int getCountLessThanTheta(final double theta) {
    final long thetaLong = (long) (MAX_THETA_LONG_AS_DOUBLE * theta);
    return count(getCache(), thetaLong);
  }

  /**
   * Returns the number of storage bytes required for this Sketch in its current state.
   * @param compact if true, returns the bytes required for compact form.
   * If this sketch is already in compact form this parameter is ignored.
   * @return the number of storage bytes required for this sketch
   */
  public abstract int getCurrentBytes(boolean compact);

  /**
   * Gets the unique count estimate.
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public double getEstimate() {
    return estimate(getThetaLong(), getRetainedEntries(true), isEmpty());
  }

  /**
   * Returns the Family that this sketch belongs to
   * @return the Family that this sketch belongs to
   */
  public abstract Family getFamily();

  /**
   * Returns a HashIterator that can be used to iterate over the retained hash values of the
   * Theta sketch.
   * @return a HashIterator that can be used to iterate over the retained hash values of the
   * Theta sketch.
   */
  public abstract HashIterator iterator();

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public double getLowerBound(final int numStdDev) {
    return (isEstimationMode())
        ? lowerBound(getRetainedEntries(true), getThetaLong(), numStdDev, isEmpty())
        : getRetainedEntries(true);
  }

  /**
   * Returns the maximum number of storage bytes required for a CompactSketch with the given
   * number of actual entries.
   * @param numberOfEntries the actual number of entries stored with the CompactSketch.
   * @return the maximum number of storage bytes required for a CompactSketch with the given number
   * of entries.
   */
  public static int getMaxCompactSketchBytes(final int numberOfEntries) {
    return (numberOfEntries << 3) + (Family.COMPACT.getMaxPreLongs() << 3);
  }

  /**
   * Returns the maximum number of storage bytes required for an UpdateSketch with the given
   * number of nominal entries (power of 2).
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum number of storage bytes required for a UpdateSketch with the given
   * nomEntries
   */
  public static int getMaxUpdateSketchBytes(final int nomEntries) {
    final int nomEnt = ceilingPowerOf2(nomEntries);
    return (nomEnt << 4) + (Family.QUICKSELECT.getMaxPreLongs() << 3);
  }

  /**
   * Returns the number of valid entries that have been retained by the sketch.
   * @return the number of valid retained entries
   */
  public int getRetainedEntries() {
    return getRetainedEntries(true);
  }

  /**
   * Returns the number of entries that have been retained by the sketch.
   * @param valid if true, returns the number of valid entries, which are less than theta and used
   * for estimation.
   * Otherwise, return the number of all entries, valid or not, that are currently in the internal
   * sketch cache.
   * @return the number of retained entries
   */
  public abstract int getRetainedEntries(boolean valid);

  /**
   * Returns the serialization version from the given Memory
   * @param mem the sketch Memory
   * @return the serialization version from the Memory
   */
  public static int getSerializationVersion(final Memory mem) {
    return mem.getByte(SER_VER_BYTE);
  }

  /**
   * Gets the value of theta as a double with a value between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return getThetaLong() / MAX_THETA_LONG_AS_DOUBLE;
  }

  /**
   * Gets the value of theta as a long
   * @return the value of theta as a long
   */
  public abstract long getThetaLong();

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public double getUpperBound(final int numStdDev) {
    return (isEstimationMode())
        ? upperBound(getRetainedEntries(true), getThetaLong(), numStdDev, isEmpty())
        : getRetainedEntries(true);
  }

  /**
   * Returns true if this sketch's data structure is backed by Memory or WritableMemory.
   * @return true if this sketch's data structure is backed by Memory or WritableMemory.
   */
  public abstract boolean hasMemory();

  /**
   * Returns true if this sketch is in compact form.
   * @return true if this sketch is in compact form.
   */
  public abstract boolean isCompact();

  /**
   * Returns true if the this sketch's internal data structure is backed by direct (off-heap)
   * Memory.
   * @return true if the this sketch's internal data structure is backed by direct (off-heap)
   * Memory.
   */
  public abstract boolean isDirect();

  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public abstract boolean isEmpty();

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * This is true if theta &lt; 1.0 AND isEmpty() is false.
   * @return true if the sketch is in estimation mode.
   */
  public boolean isEstimationMode() {
    return estMode(getThetaLong(), isEmpty());
  }

  /**
   * Returns true if internal cache is ordered
   * @return true if internal cache is ordered
   */
  public abstract boolean isOrdered();

  /**
   * Returns true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>. The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   * @param that A different non-null object
   * @return true if the backing resource of <i>this</i> is the same as the backing resource
   * of <i>that</i>.
   */
  public boolean isSameResource(final Memory that) {
    return false;
  }

  /**
   * Serialize this sketch to a byte array form.
   * @return byte array of this sketch
   */
  public abstract byte[] toByteArray();

  /**
   * Returns a human readable summary of the sketch.  This method is equivalent to the parameterized
   * call:<br>
   * <i>Sketch.toString(sketch, true, false, 8, true);</i>
   * @return summary
   */
  @Override
  public String toString() {
    return toString(true, false, 8, true);
  }

  /**
   * Gets a human readable listing of contents and summary of the given sketch.
   * This can be a very long string.  If this sketch is in a "dirty" state there
   * may be values in the dataDetail view that are &ge; theta.
   *
   * @param sketchSummary If true the sketch summary will be output at the end.
   * @param dataDetail If true, includes all valid hash values in the sketch.
   * @param width The number of columns of hash values. Default is 8.
   * @param hexMode If true, hashes will be output in hex.
   * @return The result string, which can be very long.
   */
  public String toString(final boolean sketchSummary, final boolean dataDetail, final int width,
      final boolean hexMode) {
    final StringBuilder sb = new StringBuilder();

    final long[] cache = getCache();
    int nomLongs = 0;
    int arrLongs = cache.length;
    float p = 0;
    int rf = 0;
    final boolean updateSketch = (this instanceof UpdateSketch);

    final long thetaLong = getThetaLong();
    final int curCount = this.getRetainedEntries(true);

    if (updateSketch) {
      final UpdateSketch uis = (UpdateSketch)this;
      nomLongs = 1 << uis.getLgNomLongs();
      arrLongs = 1 << uis.getLgArrLongs();
      p = uis.getP();
      rf = uis.getResizeFactor().getValue();
    }

    if (dataDetail) {
      final int w = (width > 0) ? width : 8; // default is 8 wide
      if (curCount > 0) {
        sb.append("### SKETCH DATA DETAIL");
        for (int i = 0, j = 0; i < arrLongs; i++ ) {
          final long h;
          h = cache[i];
          if ((h <= 0) || (h >= thetaLong)) {
            continue;
          }
          if ((j % w) == 0) {
            sb.append(LS).append(String.format("   %6d", (j + 1)));
          }
          if (hexMode) {
            sb.append(" " + zeroPad(Long.toHexString(h), 16) + ",");
          }
          else {
            sb.append(String.format(" %20d,", h));
          }
          j++ ;
        }
        sb.append(LS).append("### END DATA DETAIL").append(LS + LS);
      }
    }

    if (sketchSummary) {
      final double thetaDbl = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
      final String thetaHex = zeroPad(Long.toHexString(thetaLong), 16);
      final String thisSimpleName = this.getClass().getSimpleName();
      final int seedHash = Short.toUnsignedInt(getSeedHash());

      sb.append(LS);
      sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
      if (updateSketch) {
        sb.append("   Nominal Entries (k)     : ").append(nomLongs).append(LS);
      }
      sb.append("   Estimate                : ").append(getEstimate()).append(LS);
      sb.append("   Upper Bound, 95% conf   : ").append(getUpperBound(2)).append(LS);
      sb.append("   Lower Bound, 95% conf   : ").append(getLowerBound(2)).append(LS);
      if (updateSketch) {
        sb.append("   p                       : ").append(p).append(LS);
      }
      sb.append("   Theta (double)          : ").append(thetaDbl).append(LS);
      sb.append("   Theta (long)            : ").append(thetaLong).append(LS);
      sb.append("   Theta (long) hex        : ").append(thetaHex).append(LS);
      sb.append("   EstMode?                : ").append(isEstimationMode()).append(LS);
      sb.append("   Empty?                  : ").append(isEmpty()).append(LS);
      if (updateSketch) {
        sb.append("   Resize Factor           : ").append(rf).append(LS);
      }
      sb.append("   Array Size Entries      : ").append(arrLongs).append(LS);
      sb.append("   Retained Entries        : ").append(curCount).append(LS);
      sb.append("   Seed Hash               : ").append(Integer.toHexString(seedHash))
        .append(" | ").append(seedHash).append(LS);
      sb.append("### END SKETCH SUMMARY").append(LS);

    }
    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a Theta Sketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a Theta Sketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.preambleToString(byteArr);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a Theta Sketch.
   * @param mem the given Memory object
   * @return a human readable string of the preamble of a Memory image of a Theta Sketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.preambleToString(mem);
  }

  //Restricted methods

  /**
   * Gets the internal cache array.
   * @return the internal cache array.
   */
  abstract long[] getCache();

  int getCurrentDataLongs(final boolean compact) {
    return (isCompact() || compact)
        ? getRetainedEntries(true)
        : (1 << ((UpdateSketch) this).getLgArrLongs());
  }

  /**
   * Returns preamble longs if stored in current state.
   * @param compact if true, returns the preamble longs required for compact form.
   * If this sketch is already in compact form this parameter is ignored.
   * @return preamble longs if stored.
   */
  abstract int getCurrentPreambleLongs(boolean compact);

  static final int computeCompactPreLongs(final long thetaLong, final boolean empty,
      final int curCount) {
    return (thetaLong < Long.MAX_VALUE) ? 3 : empty ? 1 : (curCount > 1) ? 2 : 1;
  }

  /**
   * Returns the Memory object if it exists, otherwise null.
   * @return the Memory object if it exists, otherwise null.
   */
  abstract Memory getMemory();

  /**
   * Gets the 16-bit seed hash
   * @return the seed hash
   */
  abstract short getSeedHash();

  /**
   * Returns true if given Family id is one of the theta sketches
   * @param id the given Family id
   * @return true if given Family id is one of the theta sketches
   */
  static final boolean isValidSketchID(final int id) {
    return (id == Family.ALPHA.getID())
        || (id == Family.QUICKSELECT.getID())
        || (id == Family.COMPACT.getID());
  }

  /**
   * Checks Ordered and Compact flags for integrity between sketch and Memory
   * @param sketch the given sketch
   */
  static final void checkSketchAndMemoryFlags(final Sketch sketch) {
    final Memory mem = sketch.getMemory();
    if (mem == null) { return; }
    final int flags = PreambleUtil.extractFlags(mem);
    if (((flags & COMPACT_FLAG_MASK) > 0) ^ sketch.isCompact()) {
      throw new SketchesArgumentException("Possible corruption: "
          + "Memory Compact Flag inconsistent with Sketch");
    }
    if (((flags & ORDERED_FLAG_MASK) > 0) ^ sketch.isOrdered()) {
      throw new SketchesArgumentException("Possible corruption: "
          + "Memory Ordered Flag inconsistent with Sketch");
    }
  }

  /*
   * The truth table for empty, curCount and theta on compact is as follows:
   * <pre>
   * Num Theta CurCount Empty State  Comments
   *  0    1.0     0      T     OK   The Normal Empty State
   *  1    1.0     0      F   Error  This conflicts with the normal empty state
   *  2    1.0    !0      T   Error  Empty and curCount !0 should never co-exist
   *  3    1.0    !0      F     OK   This corresponds to a sketch in exact mode
   *  4   <1.0     0      T   Error  This can be an initial UpdateSketch state if p < 1.0,
   *                                   but should compacted as {Th = 1.0, 0, T}.
   *  5   <1.0     0      F     OK   This can result from set operations
   *  6   <1.0    !0      T   Error  Empty and curCount !0 should never co-exist
   *  7   <1.0    !0      F     OK   This corresponds to a sketch in estimation mode
   * </pre>
   * <p>thetaOnCompact() checks for only #4 and corrects theta.
   * <p>emptyOnCompact() corrects for #1, 2, 6 if they occur
   * <p>First apply thetaOnCompact() then emptyOnCompact().
   */
  static final long thetaOnCompact(final boolean empty, final int curCount, final long thetaLong) {
    return (empty && (curCount == 0) && (thetaLong < Long.MAX_VALUE)) ? Long.MAX_VALUE : thetaLong;
  }

  static final boolean emptyOnCompact(final int curCount, final long thetaLong) {
    return ((curCount == 0) && (thetaLong == Long.MAX_VALUE));
  }

  static final double estimate(final long thetaLong, final int curCount, final boolean empty) {
    if (estMode(thetaLong, empty)) {
      final double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
      return curCount / theta;
    }
    return curCount;
  }

  static final double lowerBound(final int curCount, final long thetaLong, final int numStdDev,
      final boolean empty) {
    final double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
    return BinomialBoundsN.getLowerBound(curCount, theta, numStdDev, empty);
  }

  static final double upperBound(final int curCount, final long thetaLong, final int numStdDev,
      final boolean empty) {
    final double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
    return BinomialBoundsN.getUpperBound(curCount, theta, numStdDev, empty);
  }

  private static final boolean estMode(final long thetaLong, final boolean empty) {
    return (thetaLong < Long.MAX_VALUE) && !empty;
  }

  /**
   * Instantiates a Heap Sketch from Memory.
   * @param famID the Family ID
   * @param ordered true if the sketch is of the Compact family and ordered
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * The seed required to instantiate a non-compact sketch.
   * @return a Sketch
   */
  private static final Sketch constructHeapSketch(final byte famID, final boolean ordered,
      final Memory srcMem, final long seed) {
    final boolean compact = (srcMem.getByte(FLAGS_BYTE) & COMPACT_FLAG_MASK) != 0;
    final Family family = idToFamily(famID);
    switch (family) {
      case ALPHA: {
        if (compact) {
          throw new SketchesArgumentException("Possibly Corrupted " + family
              + " image: cannot be compact");
        }
        return HeapAlphaSketch.heapifyInstance(srcMem, seed);
      }
      case QUICKSELECT: {
        return HeapQuickSelectSketch.heapifyInstance(srcMem, seed);
      }
      case COMPACT: {
        if (!compact) {
          throw new SketchesArgumentException("Possibly Corrupted " + family
              + " image: must be compact");
        }
        return ordered ? HeapCompactOrderedSketch.heapifyInstance(srcMem, seed)
                       : HeapCompactUnorderedSketch.heapifyInstance(srcMem, seed);
      }
      default: {
        throw new SketchesArgumentException("Sketch cannot heapify family: " + family
            + " as a Sketch");
      }
    }
  }

}
