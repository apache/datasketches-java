package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SERDE_ID_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesException;
import com.yahoo.sketches.SketchesStateException;

public class ReservoirLongsSketchTest {
    private static final double EPS = 1e-8;

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkInvalidK() {
        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(0);
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadSerVer() {
        Memory mem = getBasicSerializedRLS();
        mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

        ReservoirLongsSketch.getInstance(mem);
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadFamily() {
        Memory mem = getBasicSerializedRLS();
        mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

        ReservoirLongsSketch.getInstance(mem);
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadPreLongs() {
        Memory mem = getBasicSerializedRLS();
        mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

        ReservoirLongsSketch.getInstance(mem);
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadSerDeID() {
        Memory mem = getBasicSerializedRLS();

        mem.putByte(SERDE_ID_SHORT, (byte) 0);
        mem.putByte(SERDE_ID_SHORT + 1, (byte) 0);
        ReservoirLongsSketch.getInstance(mem);
        fail();
    }

    @Test
    public void checkEmptySketch() {
        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(5);
        assertTrue(rls.getSamples() == null);

        byte[] sketchBytes = rls.toByteArray();
        Memory mem = new NativeMemory(sketchBytes);

        // only minPreLongs bytes and should deserialize to empty
        assertEquals(sketchBytes.length, Family.RESERVOIR.getMinPreLongs() << 3);
        ReservoirLongsSketch loadedRls = ReservoirLongsSketch.getInstance(mem);
        assertEquals(loadedRls.getNumSamples(), 0);
    }

    @Test
    public void checkUnderFullReservoir() {
        int k = 128;
        int n = 64;

        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

        for (int i = 0; i < n; ++i) {
             rls.update(i);
        }
        assertEquals(rls.getNumSamples(), n);

        long[] data = rls.getSamples();
        assertEquals(rls.getNumSamples(), rls.getN());
        assertEquals(data.length, n);

        // items in submit order until reservoir at capacity so check
        for (int i = 0; i < n; ++i) {
            assertEquals(data[i], i);
        }

        validateSerializeAndDeserialize(rls);
    }

    @Test
    public void checkFullReservoir() {
        int k = 1000;
        int n = 2000;

        // specify smaller ResizeFactor to ensure multiple resizes
        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k, ResizeFactor.X2);

        for (int i = 0; i < n; ++i) {
            rls.update(i);
        }
        assertEquals(rls.getNumSamples(), rls.getK());

        validateSerializeAndDeserialize(rls);
    }

    @Test
    public void checkBadConstructorArgs() {
        long[] data = new long[128];
        for (int i = 0; i < 128; ++i) {
            data[i] = i;
        }

        ResizeFactor rf = ResizeFactor.X8;

        short encResSize256 = ReservoirSize.computeSize(256);
        short encResSize128 = ReservoirSize.computeSize(128);
        short encResSize64 = ReservoirSize.computeSize(64);
        short encResSize1 = ReservoirSize.computeSize(1);

        // no data
        try {
            new ReservoirLongsSketch(null, 128, rf, encResSize128);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("null reservoir"));
        }

        // size too small
        try {
            new ReservoirLongsSketch(data, 128, rf, encResSize1);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("size less than 2"));
        }

        // configured reservoir size smaller than data length
        try {
            new ReservoirLongsSketch(data, 128, rf, encResSize64);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("max size less than array length"));
        }

        // too many items seen vs data length, full sketch
        try {
            new ReservoirLongsSketch(data, 512, rf, encResSize256);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("too few samples"));
        }

        // too many items seen vs data length, under-full sketch
        try {
            new ReservoirLongsSketch(data, 256, rf, encResSize256);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("too few samples"));
        }
    }

    @Test
    public void checkSketchCapacity() {
        long[] data = new long[64];
        short encResSize = ReservoirSize.computeSize(64);
        long itemsSeen = (1 << 48) - 2;

        ReservoirLongsSketch rls = new ReservoirLongsSketch(data, itemsSeen,
                ResizeFactor.X8, encResSize);

        // this should work, the next should fail
        rls.update(0);

        try {
            rls.update(0);
            fail();
        } catch (SketchesStateException e) {
            assertTrue(e.getMessage().contains("Sketch has exceeded capacity for total items seen"));
        }
    }

    @Test
    public void checkSampleWeight() {
        int k = 32;
        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

        for (int i = 0; i < (k / 2); ++i) {
            rls.update(i);
        }
        assertEquals(rls.getImplicitSampleWeight(), 1.0); // should be exact value here

        // will have 3k/2 total samples when done
        for (int i = 0; i < k; ++i) {
            rls.update(i);
        }
        assertTrue(rls.getImplicitSampleWeight() - 1.5 < EPS);
    }

    private Memory getBasicSerializedRLS() {
        int k = 10;
        int n = 20;

        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);
        assertEquals(rls.getNumSamples(), 0);

        for (int i = 0; i < n; ++i) {
            rls.update(i);
        }
        assertEquals(rls.getNumSamples(), Math.min(n, k));
        assertEquals(rls.getN(), n);
        assertEquals(rls.getK(), k);

        byte[] sketchBytes = rls.toByteArray();
        return new NativeMemory(sketchBytes);
    }

    private void validateSerializeAndDeserialize(ReservoirLongsSketch rls) {
        byte[] sketchBytes = rls.toByteArray();
        assertEquals(sketchBytes.length, (Family.RESERVOIR.getMaxPreLongs() + rls.getNumSamples()) << 3);

        // ensure full reservoir rebuilds correctly
        Memory mem = new NativeMemory(sketchBytes);
        ReservoirLongsSketch loadedRls = ReservoirLongsSketch.getInstance(mem);

        validateReservoirEquality(rls, loadedRls);
    }

    private void validateReservoirEquality(ReservoirLongsSketch rls1, ReservoirLongsSketch rls2) {
        assertEquals(rls1.getNumSamples(), rls2.getNumSamples());

        if (rls1.getNumSamples() == 0) { return; }

        long[] samples1 = rls1.getSamples();
        long[] samples2 = rls2.getSamples();
        assertEquals(samples1.length, samples2.length);

        for (int i = 0; i < samples1.length; ++i) {
            assertEquals(samples1[i], samples2[i]);
        }
    }

}
