package com.yahoo.sketches.sampling;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;

import java.util.Random;

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

    /**
     * Default sampling size multiple when reallocating storage: 8
     */
    private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

    static Random rand = new Random();

    int reservoirSize_;      // max size of sampling
    int currReservoirAlloc_; // currently allocated array size
    int currCount_;          // number of items in sampling
    long totalSeen_;         // number of items presented to sketch
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
        totalSeen_ = 0;

        int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(k), "ReservoirLongSketch");
        int initialSize = startingSubMultiple(k, ceilingLgK, MIN_LG_ARR_LONGS);

        currReservoirAlloc_ = getAdjustedSize(k, initialSize);
        data_ = new long[currReservoirAlloc_];
        java.util.Arrays.fill(data_,  0L);
    }

    public int getK() {
        return reservoirSize_;
    }

    public long getN() {
        return totalSeen_;
    }

    public int getNumSamples() {
        return (int) Math.min((long) reservoirSize_, totalSeen_);
    }

    public void setRandomSeed(long seed) {
        rand.setSeed(seed);
    }

    public long[] getSamples() {
        if (totalSeen_ == 0) {
            return null;
        }
        int numSamples = (int) Math.min((long) reservoirSize_, totalSeen_);
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
        final int numPreLongs = PreambleUtil.getAndCheckPreLongs(srcMem);
        final long pre0 = srcMem.getLong(0);
        final int serVer = PreambleUtil.extractSerVer(pre0);
        final int familyId = PreambleUtil.extractFamilyID(pre0);
        final boolean isEmpty = (PreambleUtil.extractFlags(pre0) & EMPTY_FLAG_MASK) != 0;
        final int reservoirSize = PreambleUtil.extractReservoirSize(pre0);

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
        if (isEmpty ^ preLongsEqMin) {
            throw new SketchesArgumentException(
                    "Possible Corruption: (PreLongs == 1) ^ Empty == True.");
        }

        if (isEmpty) {
            return new ReservoirLongSketch(reservoirSize, DEFAULT_RESIZE_FACTOR);
        }

        //get full preamble
        final long[] preArr = new long[numPreLongs];
        srcMem.getLongArray(0, preArr, 0, numPreLongs);

        ReservoirLongSketch rls = new ReservoirLongSketch(reservoirSize, DEFAULT_RESIZE_FACTOR);
        rls.totalSeen_ = preArr[1];  // no mask needed

        int memOffset = numPreLongs << 3;
        if (rls.totalSeen_ >= rls.reservoirSize_) {
            // full reservoir so allocate it all
            rls.data_ = new long[rls.reservoirSize_];

            srcMem.getLongArray(memOffset, rls.data_, 0, rls.reservoirSize_);
        } else {
            // under-full so determine size to allocate, using ceilingLog2(totalSeen) as minimum
            // casts to int are safe since under-full
            int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(rls.reservoirSize_), "getInstance");
            int minLgSize = Util.toLog2(Util.ceilingPowerOf2((int) rls.totalSeen_), "getInstance");
            int initialSize = startingSubMultiple(rls.reservoirSize_, ceilingLgK,
                    Math.min(minLgSize, MIN_LG_ARR_LONGS));

            // initialize full allocation to zero, then load the relevant number of samples
            rls.currReservoirAlloc_ = getAdjustedSize(rls.reservoirSize_, initialSize);
            rls.data_ = new long[rls.currReservoirAlloc_];
            java.util.Arrays.fill(rls.data_,  0L);

            srcMem.getLongArray(memOffset, rls.data_, 0, (int) rls.totalSeen_);
        }

        return rls;
    }

    /**
     * Returns a byte array representation of this sketch
     * @return a byte array representation of this sketch
     */
    public byte[] toByteArray() {
        final int preLongs, outBytes;
        final boolean empty = totalSeen_ == 0;
        final int numItems = (int) Math.min(reservoirSize_, totalSeen_);

        if (empty) {
            preLongs = 1;
            outBytes = 8;
        } else {
            preLongs = Family.RESERVOIR.getMaxPreLongs();
            outBytes = (preLongs + numItems) << 3; // for longs, we know the size
        }
        final byte[] outArr = new byte[outBytes];
        final Memory mem = new NativeMemory(outArr);

        // build first preLong empty or not
        long pre0 = 0L;
        pre0 = PreambleUtil.insertPreLongs(preLongs, pre0);                  //Byte 0
        pre0 = PreambleUtil.insertSerVer(SER_VER, pre0);                     //Byte 1
        pre0 = PreambleUtil.insertFamilyID(Family.RESERVOIR.getID(), pre0);  //Byte 2
        pre0 = (empty) ? PreambleUtil.insertFlags(EMPTY_FLAG_MASK, pre0) : PreambleUtil.insertFlags(0, pre0); //Byte 3
        pre0 = PreambleUtil.insertReservoirSize(reservoirSize_, pre0);       //Bytes 4-7

        if (empty) {
            mem.putLong(0, pre0);
        } else {
            final long[] preArr = new long[preLongs];
            preArr[0] = pre0;
            preArr[1] = totalSeen_;
            mem.putLongArray(0, preArr, 0, preLongs);
            final int preBytes = preLongs << 3;
            mem.putLongArray(preBytes, data_, 0, numItems);
        }
        return outArr;
    }

    double getImplicitSampleWeight() {
        if (totalSeen_ < reservoirSize_) {
            return 1.0;
        } else {
            return ((double) totalSeen_ / (double) reservoirSize_);
        }
    }


    /**
     * Randomly decide whether or not to include an item in the sample set.
     *
     * @param item a unit-weight (equivalently, unweighted) item of the set being sampled from
     */
    public void update (long item) {
        if (totalSeen_ < reservoirSize_) { // code for initial phase where we take the first reservoirSize_ items
            if (totalSeen_ >= currReservoirAlloc_) {
                growReservoir();
            }
            assert totalSeen_< currReservoirAlloc_;
            // we'll randomize replacement positions, so in-order should be valid for now
            data_[(int) totalSeen_] = item; // since less than reservoir size, cast is safe
            ++totalSeen_;
        } else { // code for steady state where we sample randomly
            ++totalSeen_;
            // prob(keep_item) < k / n = reservoirSize_ / totalSeen_
            // so multiply to get: keep if rand * totalSeen_ < reservoirSize_
            if (rand.nextDouble() * totalSeen_ < reservoirSize_) {
                int newSlot = rand.nextInt(reservoirSize_);
                data_[newSlot] = item;
            }
        }
    }

    /**
     * Increases allocated sampling size by (adjusted) ResizeFactor and copies data from old sampling.
     */
    private void growReservoir() {
        int newSize = getAdjustedSize(reservoirSize_, currReservoirAlloc_ * rf_.getValue());
        long[] buffer = java.util.Arrays.copyOf(data_, newSize);

        currReservoirAlloc_ = newSize;
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

        System.out.println("Comparing byte[] veresions:");
        System.out.println("\tSizes " + (ser1.length == ser2.length ? "match" : "differ"));

        System.out.println("Ser 1:");
        for (int i = ser1.length - 1; i >= 0; --i) {
            System.out.print(Integer.toHexString(ser1[i]));
        }
        System.out.print(Util.LS);

        System.out.println("Ser 2:");
        for (int i = ser2.length - 1; i >= 0; --i) {
            System.out.print(Integer.toHexString(ser2[i]));
        }
        System.out.print(Util.LS);

        for (int i = 0; i < ser1.length; ++i) {
            if (ser1[i] != ser2[i]) {
                System.out.println("Arrays differ at byte " + i);
                return;
            }
        }
        System.out.println("Arrays match!");
    }
}
