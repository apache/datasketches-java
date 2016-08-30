package com.yahoo.sketches.sampling;

import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import java.util.Random;

import static com.yahoo.sketches.sampling.PreambleUtil.*;

/**
 * Created by jmalkin on 8/22/16.
 *
 * @author jmalkin
 * @author langk
 */
public class ReservoirLongSketch {

    /**
     * The smallest sampling array allocated: 16
     */
    private static final int MIN_LG_ARR_LONGS = 4;

    private static final int ARRAY_OF_LONGS_SERDE_ID = new ArrayOfLongsSerDe().getId();

    /**
     * Default sampling size multiple when reallocating storage: 8
     */
    private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

    static Random rand = new Random();

    int reservoirSize_;      // max size of sampling
    int currItemsAlloc_; // currently allocated array size
    int currCount_;          // number of items in sampling
    long itemsSeen_;         // number of items presented to sketch
    ResizeFactor rf_;        // resize factor
    long[] data_;            // stored sampling data

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     */
    public static ReservoirLongSketch getInstance(final int k) {
        return new ReservoirLongSketch(k, DEFAULT_RESIZE_FACTOR);
    }

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     */
    public static ReservoirLongSketch getInstance(final int k, ResizeFactor rf) {
        return new ReservoirLongSketch(k, rf);
    }

    private ReservoirLongSketch(final int k, ResizeFactor rf) {
        // required due to a theorem about lightness during merging
        if (k < 2) {
            throw new IllegalArgumentException("k must be at least 2");
        }

        reservoirSize_ = k;
        rf_ = rf;

        currCount_ = 0;
        itemsSeen_ = 0;

        int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(k), "ReservoirLongSketch");
        int initialSize = startingSubMultiple(k, ceilingLgK, MIN_LG_ARR_LONGS);

        currItemsAlloc_ = getAdjustedSize(k, initialSize);
        data_ = new long[currItemsAlloc_];
        java.util.Arrays.fill(data_,  0L);
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
     * @return n, the number of stream items teh sketch has seen
     */
    public long getN() {
        return itemsSeen_;
    }

    /**
     * Returns the current number of items in the reservoir as
     * @return
     */
    public int getNumSamples() {
        return (int) Math.min((long) reservoirSize_, itemsSeen_);
    }

    public void setRandomSeed(long seed) {
        rand.setSeed(seed);
    }

    public long[] getSamples() {
        if (itemsSeen_ == 0) {
            return null;
        }
        int numSamples = (int) Math.min((long) reservoirSize_, itemsSeen_);
        return java.util.Arrays.copyOf(data_, numSamples);
    }


    /**
     * Returns a sketch instance of this class from the given srcMem,
     * which must be a Memory representation of this sketch class.
     *
     * @param srcMem a Memory representation of a sketch of this class.
     * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
     * @return a sketch instance of this class
     */
    public static ReservoirLongSketch getInstance(final Memory srcMem) {
        final int numPreLongs = getAndCheckPreLongs(srcMem);
        final long pre0 = srcMem.getLong(0);
        final long pre1 = srcMem.getLong(8);
        final int serVer = extractSerVer(pre0);
        final int familyId = extractFamilyID(pre0);
        final boolean isEmpty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0;
        final int reservoirSize = extractReservoirSize(pre0);

        // Check values
        final boolean preLongsEqMin = (numPreLongs == Family.RESERVOIR.getMinPreLongs());
        final boolean preLongsEqMax = (numPreLongs == Family.RESERVOIR.getMaxPreLongs());

        if (!preLongsEqMin & !preLongsEqMax) {
            throw new SketchesArgumentException(
                    "Possible corruption: Non-empty sketch with only " + Family.RESERVOIR.getMinPreLongs() +
                            "prelongs");
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
        final int serDeId = extractSerDeId(pre1);
        if (serDeId != ARRAY_OF_LONGS_SERDE_ID) {
            throw new SketchesArgumentException(
                    "Possible Corruption: SerDeID must be " + ARRAY_OF_LONGS_SERDE_ID + ": " + serDeId);
        }

        if (isEmpty) {
            return new ReservoirLongSketch(reservoirSize, DEFAULT_RESIZE_FACTOR);
        }

        //get full preamble
        final long[] preArr = new long[numPreLongs];
        srcMem.getLongArray(0, preArr, 0, numPreLongs);

        ReservoirLongSketch rls = new ReservoirLongSketch(reservoirSize, DEFAULT_RESIZE_FACTOR);
        rls.itemsSeen_ = extractItemsSeenCount(pre1);  // no mask needed

        int preLongBytes = numPreLongs << 3;
        if (rls.itemsSeen_ >= rls.reservoirSize_) {
            // full reservoir so allocate it all
            rls.data_ = new long[rls.reservoirSize_];

            srcMem.getLongArray(preLongBytes, rls.data_, 0, rls.reservoirSize_);
        } else {
            // under-full so determine size to allocate, using ceilingLog2(totalSeen) as minimum
            // casts to int are safe since under-full
            int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(rls.reservoirSize_), "getInstance");
            int minLgSize = Util.toLog2(Util.ceilingPowerOf2((int) rls.itemsSeen_), "getInstance");
            int initialSize = startingSubMultiple(rls.reservoirSize_, ceilingLgK,
                    Math.min(minLgSize, MIN_LG_ARR_LONGS));

            // initialize full allocation to zero, then load the relevant number of samples
            rls.currItemsAlloc_ = getAdjustedSize(rls.reservoirSize_, initialSize);
            rls.data_ = new long[rls.currItemsAlloc_];
            java.util.Arrays.fill(rls.data_,  0L);

            srcMem.getLongArray(preLongBytes, rls.data_, 0, (int) rls.itemsSeen_);
        }

        return rls;
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
        pre0 = PreambleUtil.insertPreLongs(preLongs, pre0);                  //Byte 0
        pre0 = PreambleUtil.insertSerVer(SER_VER, pre0);                     //Byte 1
        pre0 = PreambleUtil.insertFamilyID(Family.RESERVOIR.getID(), pre0);  //Byte 2
        pre0 = (empty) ? PreambleUtil.insertFlags(EMPTY_FLAG_MASK, pre0) : PreambleUtil.insertFlags(0, pre0); //Byte 3
        pre0 = PreambleUtil.insertReservoirSize(reservoirSize_, pre0);       //Bytes 4-7

        // second preLong needs SerDe ID, empty or not
        long pre1 = 0L;
        pre1 = PreambleUtil.insertItemsSeenCount(itemsSeen_, pre1);
        pre1 = PreambleUtil.insertSerDeId(ARRAY_OF_LONGS_SERDE_ID, pre1);

        final long[] preArr = new long[preLongs];
        preArr[0] = pre0;
        preArr[1] = pre1;
        mem.putLongArray(0, preArr, 0, preLongs);
        final int preBytes = preLongs << 3;
        mem.putLongArray(preBytes, data_, 0, numItems);

        return outArr;
    }

    double getImplicitSampleWeight() {
        if (itemsSeen_ < reservoirSize_) {
            return 1.0;
        } else {
            return ((double) itemsSeen_ / (double) reservoirSize_);
        }
    }


    /**
     * Randomly decide whether or not to include an item in the sample set.
     *
     * @param item a unit-weight (equivalently, unweighted) item of the set being sampled from
     */
    public void update (long item) {
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

    public static void main(String[] args) {
        ReservoirLongSketch rs = ReservoirLongSketch.getInstance(5);
        rs.setRandomSeed(11L);

        for (long i = 0; i < 3; ++i) {
            rs.update(i);
        }

        System.out.println("Samples after 3:");
        for (long l : rs.getSamples()) {
            System.out.println(l);
        }
        System.out.println("\n\n");

        for (long i = 3; i < 20; ++i) {
            rs.update(i);
        }

        System.out.println("Samples after 20:");
        for (long l : rs.getSamples()) {
            System.out.println(l);
        }
        System.out.println("\n\n");

        //rs = ReservoirLongSketch.getInstance(5);

        byte[] ser1 = rs.toByteArray();
        Memory mem = new NativeMemory(ser1);

        ReservoirLongSketch rs2 = ReservoirLongSketch.getInstance(mem);
        byte[] ser2 = rs2.toByteArray();

        /*
        System.out.println("Reconstructed samples:");
        for (long l : rs2.getSamples()) {
            System.out.println(l);
        }
        */

        System.out.println("Preamble:");
        System.out.println(preambleToString(ser1));

        System.out.println("Comparing byte[] versions:");
        System.out.println("\tSizes " + (ser1.length == ser2.length ? "match" : "differ"));

        System.out.println("Ser 1:");
        System.out.print(printBytesAsLongs(ser1) + Util.LS);

        System.out.println("Ser 2:");
        System.out.print(printBytesAsLongs(ser2) + Util.LS);

        for (int i = 0; i < ser1.length; ++i) {
            if (ser1[i] != ser2[i]) {
                System.out.println("Arrays differ at byte " + i);
                return;
            }
        }
        System.out.println("Arrays match!");
    }

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
}
