package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractItemsSeenCount;
import static com.yahoo.sketches.sampling.PreambleUtil.extractReservoirSize;
import static com.yahoo.sketches.sampling.PreambleUtil.extractResizeFactor;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerDeId;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.sampling.PreambleUtil.getAndCheckPreLongs;
import static com.yahoo.sketches.sampling.PreambleUtil.preambleToString;

import java.util.Arrays;
import java.util.Random;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;

/**
 * Created by jmalkin on 8/22/16.
 *
 * @author jmalkin
 * @author langk
 */
public class ReservoirLongsSketch {

    /**
     * The smallest sampling array allocated: 16
     */
    private static final int MIN_LG_ARR_LONGS = 4;

    /**
     * Using 48 bits to capture number of items seen, so sketch cannot process more after this many items
     * capacity
     */
    private static final long MAX_ITEMS_SEEN = (1 << 48) - 1;

    private static final int ARRAY_OF_LONGS_SERDE_ID = new ArrayOfLongsSerDe().getId();

    /**
     * Default sampling size multiple when reallocating storage: 8
     */
    private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

    public static final Random rand = new Random();

    private int reservoirSize_;      // max size of sampling
    private short encodedResSize_;   // compact encoding of reservoir size
    private int currItemsAlloc_;     // currently allocated array size
    private long itemsSeen_;         // number of items presented to sketch
    private ResizeFactor rf_;        // resize factor
    private long[] data_;            // stored sampling data

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     * @return A ReservoirLongsSketch initialized with maximum size k and the default resize factor.
     */
    public static ReservoirLongsSketch getInstance(final int k) {
        return new ReservoirLongsSketch(k, DEFAULT_RESIZE_FACTOR);
    }

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @return A ReservoirLongsSketch initialized with maximum size k and ResizeFactor rf.
     */
    public static ReservoirLongsSketch getInstance(final int k, ResizeFactor rf) {
        return new ReservoirLongsSketch(k, rf);
    }

    /**
     * Returns a sketch instance of this class from the given srcMem,
     * which must be a Memory representation of this sketch class.
     *
     * @param srcMem a Memory representation of a sketch of this class.
     * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
     * @return a sketch instance of this class
     */
    public static ReservoirLongsSketch getInstance(final Memory srcMem) {
        final int numPreLongs = getAndCheckPreLongs(srcMem);
        final long pre0 = srcMem.getLong(0);
        final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(pre0));
        final int serVer = extractSerVer(pre0);
        final int familyId = extractFamilyID(pre0);
        final boolean isEmpty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0;

        final short encodedResSize = extractReservoirSize(pre0);
        final int reservoirSize  = ReservoirSize.decodeValue(encodedResSize);


        // Check values
        final boolean preLongsEqMin = (numPreLongs == Family.RESERVOIR.getMinPreLongs());
        final boolean preLongsEqMax = (numPreLongs == Family.RESERVOIR.getMaxPreLongs());

        if (!preLongsEqMin & !preLongsEqMax) {
            throw new SketchesArgumentException(
                    "Possible corruption: Non-empty sketch with only " + Family.RESERVOIR.getMinPreLongs()
                            + "preLongs");
        }
        if (serVer != SER_VER) {
            throw new SketchesArgumentException(
                    "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
        }
        final int reqFamilyId = Family.RESERVOIR.getID();
        if (familyId != reqFamilyId) {
            throw new SketchesArgumentException(
                    "Possible Corruption: FamilyID must be " + reqFamilyId + ": " + familyId);
        }
        final short serDeId = extractSerDeId(pre0);
        if (serDeId != ARRAY_OF_LONGS_SERDE_ID) {
            throw new SketchesArgumentException(
                    "Possible Corruption: SerDeID must be " + ARRAY_OF_LONGS_SERDE_ID + ": " + serDeId);
        }

        if (isEmpty) {
            return new ReservoirLongsSketch(reservoirSize, rf);
        }

        //get rest of preamble
        final long pre1 = srcMem.getLong(8);
        final long itemsSeen = extractItemsSeenCount(pre1);

        int preLongBytes = numPreLongs << 3;
        int numSketchLongs = (int) Math.min(itemsSeen, reservoirSize);
        int allocatedSize = reservoirSize; // default to full reservoir
        if (itemsSeen < reservoirSize) {
            // under-full so determine size to allocate, using ceilingLog2(totalSeen) as minimum
            // casts to int are safe since under-full
            int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize), "getInstance");
            int minLgSize = Util.toLog2(Util.ceilingPowerOf2((int) itemsSeen), "getInstance");
            int initialLgSize = startingSubMultiple(reservoirSize, ceilingLgK,
                    Math.min(minLgSize, MIN_LG_ARR_LONGS));

            allocatedSize = getAdjustedSize(reservoirSize, 1 << initialLgSize);
        }

        long[] data = new long[allocatedSize];
        srcMem.getLongArray(preLongBytes, data, 0, numSketchLongs);

        return new ReservoirLongsSketch(data, itemsSeen, rf, encodedResSize);
    }

    /**
     * Thin wrapper around package-private constructor
     * @param data Reservoir data as long[]
     * @param itemsSeen Number of items presented to the sketch so far
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param encodedResSize Compact encoding of reservoir size
     * @return New sketch built with the provided inputs
     */
    static ReservoirLongsSketch getInstance(final long[] data, final long itemsSeen,
                                            final ResizeFactor rf, final short encodedResSize) {
        return new ReservoirLongsSketch(data, itemsSeen, rf, encodedResSize);
    }

    private ReservoirLongsSketch(final int k, ResizeFactor rf) {
        // required due to a theorem about lightness during merging
        if (k < 2) {
            throw new SketchesArgumentException("k must be at least 2");
        }

        encodedResSize_ = ReservoirSize.computeSize(k);

        reservoirSize_ = ReservoirSize.decodeValue(encodedResSize_);
        rf_ = rf;

        itemsSeen_ = 0;

        int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize_), "ReservoirLongsSketch");
        int initialSize = startingSubMultiple(reservoirSize_, ceilingLgK, MIN_LG_ARR_LONGS);

        currItemsAlloc_ = getAdjustedSize(reservoirSize_, initialSize);
        data_ = new long[currItemsAlloc_];
        java.util.Arrays.fill(data_,  0L);
    }

    /**
     * Creates a fully-populated sketch. Used internally to avoid extraneous array allocation when deserializing.
     * Uses size of data array to as initial array allocation.
     * @param data Reservoir data as long[]
     * @param itemsSeen Number of items presented to the sketch so far
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param encodedResSize Compact encoding of reservoir size
     */
    private ReservoirLongsSketch(final long[] data, final long itemsSeen,
                                 final ResizeFactor rf, final short encodedResSize) {
        int reservoirSize = ReservoirSize.decodeValue(encodedResSize);

        if (data == null) {
            throw new SketchesArgumentException("Instantiating sketch with null reservoir");
        }
        if (reservoirSize < 2) {
            throw new SketchesArgumentException("Cannot instantiate sketch with reservoir size less than 2");
        }
        if (reservoirSize < data.length) {
            throw new SketchesArgumentException("Instantiating sketch with max size less than array length: "
                    + reservoirSize + " max size, array of length " + data.length);
        }
        if ((itemsSeen >= reservoirSize && data.length < reservoirSize)
                || (itemsSeen < reservoirSize && data.length < itemsSeen)) {
            throw new SketchesArgumentException("Instantiating sketch with too few samples. Items seen: "
                    + itemsSeen + ", max reservoir size: " + reservoirSize + ", data array length: " + data.length);
        }

        // TODO: compute target current allocation to validate?
        encodedResSize_ = encodedResSize;
        reservoirSize_ = reservoirSize;
        currItemsAlloc_ = data.length;
        itemsSeen_ = itemsSeen;
        rf_ = rf;
        data_ = data;
    }

    /**
     * Fast constructor for full-specified sketch with no encoded/decoding size and no validation.
     * @param reservoirSize Maximum reservoir capacity
     * @param encodedResSize Maximum reservoir capacity encoded into fixed-point format
     * @param currItemsAlloc Current array size (assumed equal to data.length)
     * @param itemsSeen Total items seen by this sketch
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param data Data array backing the reservoir, will <em>not</em> be copied
     */
    private ReservoirLongsSketch(final int reservoirSize, final short encodedResSize, final int currItemsAlloc,
                                 final long itemsSeen, final ResizeFactor rf, final long[] data) {
        this.reservoirSize_ = reservoirSize;
        this.encodedResSize_ = encodedResSize;
        this.currItemsAlloc_ = currItemsAlloc;
        this.itemsSeen_ = itemsSeen;
        this.rf_ = rf;
        this.data_ = data;
    }


    /**
     * Returns the sketch's value of <i>k</i>, the maximum number of samples stored in the reservoir. The current
     * number of items in the sketch may be lower.
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
     * Returns the current number of items in the reservoir, which may be smaller than the reservoir capacity.
     * @return the number of items currently in the reservoir
     */
    public int getNumSamples() {
        return (int) Math.min(reservoirSize_, itemsSeen_);
    }

    /**
     * Returns a copy of the items in the reservoir. The returned array length may be smaller than the reservoir
     * capacity.
     * @return A copy of the reservoir array
     */
    public long[] getSamples() {
        if (itemsSeen_ == 0) {
            return null;
        }
        int numSamples = (int) Math.min(reservoirSize_, itemsSeen_);
        return java.util.Arrays.copyOf(data_, numSamples);
    }



    /**
     * Returns a byte array representation of this sketch
     * @return a byte array representation of this sketch
     */
    public byte[] toByteArray() {
        final int preLongs, outBytes;
        final boolean empty = itemsSeen_ == 0;
        final int numItems = (int) Math.min(reservoirSize_, itemsSeen_);

        if (empty) {
            preLongs = 1;
            outBytes = 8;
        } else {
            preLongs = Family.RESERVOIR.getMaxPreLongs();
            outBytes = (preLongs + numItems) << 3; // for longs, we know the size
        }
        final byte[] outArr = new byte[outBytes];
        final Memory mem = new NativeMemory(outArr);

        // build first preLong
        long pre0 = 0L;
        pre0 = PreambleUtil.insertPreLongs(preLongs, pre0);                  // Byte 0
        pre0 = PreambleUtil.insertResizeFactor(rf_.lg(), pre0);
        pre0 = PreambleUtil.insertSerVer(SER_VER, pre0);                     // Byte 1
        pre0 = PreambleUtil.insertFamilyID(Family.RESERVOIR.getID(), pre0);  // Byte 2
        pre0 = (empty) ? PreambleUtil.insertFlags(EMPTY_FLAG_MASK, pre0) : PreambleUtil.insertFlags(0, pre0); // Byte 3
        pre0 = PreambleUtil.insertReservoirSize(encodedResSize_, pre0);      // Bytes 4-5
        pre0 = PreambleUtil.insertSerDeId(ARRAY_OF_LONGS_SERDE_ID, pre0);    // Bytes 6-7

        if (empty) {
            mem.putLong(0, pre0);
        } else {
            // second preLong, only if non-empty
            long pre1 = 0L;
            pre1 = PreambleUtil.insertItemsSeenCount(itemsSeen_, pre1);

            final long[] preArr = new long[preLongs];
            preArr[0] = pre0;
            preArr[1] = pre1;
            mem.putLongArray(0, preArr, 0, preLongs);
            final int preBytes = preLongs << 3;
            mem.putLongArray(preBytes, data_, 0, numItems);
        }

        return outArr;
    }

    double getImplicitSampleWeight() {
        if (itemsSeen_ < reservoirSize_) {
            return 1.0;
        } else {
            return (1.0 * itemsSeen_ / reservoirSize_);
        }
    }


    /**
     * Randomly decide whether or not to include an item in the sample set.
     *
     * @param item a unit-weight (equivalently, unweighted) item of the set being sampled from
     */
    public void update(long item) {
        if (itemsSeen_ == MAX_ITEMS_SEEN) {
            throw new SketchesStateException("Sketch has exceeded capacity for total items seen: " + MAX_ITEMS_SEEN);
        }

        if (itemsSeen_ < reservoirSize_) { // code for initial phase where we take the first reservoirSize_ items
            if (itemsSeen_ >= currItemsAlloc_) {
                growReservoir();
            }
            assert itemsSeen_ < currItemsAlloc_;
            // we'll randomize replacement positions, so in-order should be valid for now
            data_[(int) itemsSeen_] = item; // since less than reservoir size, cast is safe
            ++itemsSeen_;
        } else { // code for steady state where we sample randomly
            ++itemsSeen_;
            // prob(keep_item) < k / n = reservoirSize_ / itemsSeen_
            // so multiply to get: keep if rand * itemsSeen_ < reservoirSize_
            if (rand.nextDouble() * itemsSeen_ < reservoirSize_) {
                int newSlot = rand.nextInt(reservoirSize_);
                data_[newSlot] = item;
            }
        }
    }

    /**
     * Increases allocated sampling size by (adjusted) ResizeFactor and copies data from old sampling.
     */
    private void growReservoir() {
        int newSize = getAdjustedSize(reservoirSize_, currItemsAlloc_ * rf_.getValue());
        long[] buffer = java.util.Arrays.copyOf(data_, newSize);

        currItemsAlloc_ = newSize;
        data_ = buffer;
    }

    /**
     * Checks if target sampling allocation is more than 50% of max sampling size. If so, returns max sampling size,
     * otherwise passes through the target size.
     *
     * @param maxSize Maximum allowed reservoir size, as from getK()
     * @param resizeTarget Next size based on a pure ResizeFactor scaling
     * @return (reservoirSize_ &lt; 2*resizeTarget ? reservoirSize_ : resizeTarget)
     */
    private static int getAdjustedSize(final int maxSize, final int resizeTarget) {
        if (maxSize - (resizeTarget << 1) < 0L) {
            return maxSize;
        }
        return resizeTarget;
    }

    static int startingSubMultiple(int lgTarget, int lgRf, int lgMin) {
        return (lgTarget <= lgMin) ? lgMin : (lgRf == 0) ? lgTarget : (lgTarget - lgMin) % lgRf + lgMin;
    }

    /**
     * Useful during union operations to avoid copying the data array around if only updating a few points.
     * @param pos The position from which to retrieve the element
     * @return The value in the reservoir at position <tt>pos</tt>
     */
    long getValueAtPosition(int pos) {
        if (itemsSeen_ == 0) {
            throw new SketchesArgumentException("Requested element from empty reservoir.");
        }
        else if (pos < 0 || pos >= getNumSamples()) {
            throw new SketchesArgumentException("Requested position must be between 0 and " + getNumSamples() + ", "
                    + "inclusive. Received: " + pos);
        }

        return data_[pos];
    }

    /**
     * Useful during union operation to force-insert a value into the union gadget. Does <em>NOT</em> increment count
     * of items seen.
     * @param value The entry to store in the reservoir
     * @param pos The position at which to store the entry
     */
    void insertValueAtPosition(long value, int pos) {
        if (itemsSeen_ == 0) {
            throw new SketchesArgumentException("Inserting element into unallocated, empty reservoir.");
        }
        else if (pos < 0 || pos >= getNumSamples()) {
            throw new SketchesArgumentException("Insert position must be between 0 and " + getNumSamples() + ", "
                    + "inclusive. Received: " + pos);
        }

        data_[pos] = value;
    }

    /**
     * Used during union operations to update count of items seen. Does <em>NOT</em> check sign, but will throw an
     * exception if the final result exceeds the maximum possible items seen value.
     * @param inc The value added
     */
    void forceIncrementItemsSeen(final long inc) {
        itemsSeen_ += inc;

        if (itemsSeen_ > MAX_ITEMS_SEEN) {
            throw new SketchesStateException("Sketch has exceeded capacity for total items seen. Limit: "
                    + MAX_ITEMS_SEEN + ", found: " + itemsSeen_);
        }
    }

    ReservoirLongsSketch copy() {
        long[] dataCopy = Arrays.copyOf(data_, currItemsAlloc_);
        return new ReservoirLongsSketch(reservoirSize_, encodedResSize_, currItemsAlloc_, itemsSeen_, rf_, dataCopy);
    }

    /*
    static String printBytesAsLongs(byte[] byteArr) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < byteArr.length; i += 8) {
            for (int j = i + 7; j >= i; --j) {
                String str = Integer.toHexString(byteArr[j] & 0XFF);
                sb.append(Util.zeroPad(str, 2));
            }
            sb.append(Util.LS);

        }

        return sb.toString();
    }
    */
}
