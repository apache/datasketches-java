package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SERDE_ID_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesException;
import com.yahoo.sketches.SketchesStateException;

public class ReservoirItemsSketchTest {
    private static final double EPS = 1e-8;

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkInvalidK() {
        ReservoirItemsSketch<Integer> ris = ReservoirItemsSketch.<Integer>getInstance(0);
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadSerVer() {
        Memory mem = getBasicSerializedLongsRIS();
        mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

        ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadFamily() {
        Memory mem = getBasicSerializedLongsRIS();
        mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

        ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadPreLongs() {
        Memory mem = getBasicSerializedLongsRIS();
        mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

        ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadSerDeID() {
        Memory mem = getBasicSerializedLongsRIS();

        mem.putByte(SERDE_ID_SHORT, (byte) 0);
        mem.putByte(SERDE_ID_SHORT + 1, (byte) 0);
        ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());
        fail();
    }

    @Test
    public void checkEmptySketch() {
        ReservoirItemsSketch<String> ris = ReservoirItemsSketch.<String>getInstance(5);
        assertTrue(ris.getSamples() == null);

        byte[] sketchBytes = ris.toByteArray(new ArrayOfStringsSerDe());
        Memory mem = new NativeMemory(sketchBytes);

        // only minPreLongs bytes and should deserialize to empty
        assertEquals(sketchBytes.length, Family.RESERVOIR.getMinPreLongs() << 3);
        ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
        ReservoirItemsSketch<String> loadedRis = ReservoirItemsSketch.<String>getInstance(mem, serDe);
        assertEquals(loadedRis.getNumSamples(), 0);
    }

    @Test
    public void checkUnderFullReservoir() {
        int k = 128;
        int n = 64;

        ReservoirItemsSketch<String> ris = ReservoirItemsSketch.<String>getInstance(k);
        int expectedLength = 0;

        for (int i = 0; i < n; ++i) {
            String intStr = Integer.toString(i);
            expectedLength += intStr.length() + Integer.BYTES;
            ris.update(intStr);
        }
        assertEquals(ris.getNumSamples(), n);

        String[] data = ris.getSamples();
        assertEquals(ris.getNumSamples(), ris.getN());
        assertEquals(data.length, n);

        // items in submit order until reservoir at capacity so check
        for (int i = 0; i < n; ++i) {
            assertEquals(data[i], Integer.toString(i));
        }

        // not using validateSerializeAndDeserialize() to check with a non-Long
        expectedLength += Family.RESERVOIR.getMaxPreLongs() << 3;
        byte[] sketchBytes = ris.toByteArray(new ArrayOfStringsSerDe());
        assertEquals(sketchBytes.length, expectedLength);

        // ensure full reservoir rebuilds correctly
        Memory mem = new NativeMemory(sketchBytes);
        ReservoirItemsSketch<String> loadedRis = ReservoirItemsSketch.<String>getInstance(mem,
                new ArrayOfStringsSerDe());

        validateReservoirEquality(ris, loadedRis);
    }

    @Test
    public void checkFullReservoir() {
        int k = 1000;
        int n = 2000;

        // specify smaller ResizeFactor to ensure multiple resizes
        ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.<Long>getInstance(k, ResizeFactor.X2);

        for (int i = 0; i < n; ++i) {
            ris.update((long) i);
        }
        assertEquals(ris.getNumSamples(), ris.getK());

        validateSerializeAndDeserialize(ris);
    }

    @Test
    public void checkPolymorphicType() {
        ReservoirItemsSketch<Number> ris = ReservoirItemsSketch.<Number>getInstance(6);

        Number[] data = ris.getSamples(Number.class);
        assertNull(data);

        // using mixed types
        ris.update(1);
        ris.update(2L);
        ris.update(3.0);
        ris.update((short) (44023 & 0xFFFF));
        ris.update((byte) (68 & 0xFF));
        ris.update(4.0F);

        data = ris.getSamples(Number.class);
        assertNotNull(data);
        assertEquals(data.length, 6);

        // copying samples without specifying Number.class should fail
        try {
            ris.getSamples();
            fail();
        } catch (ArrayStoreException e) {
            // expected
        }

        // likewise for toByteArray() (which uses getSamples() internally for type handling)
        ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();
        try {
            ris.toByteArray(serDe);
            fail();
        } catch (ArrayStoreException e) {
            // expected
        }

        byte[] sketchBytes = ris.toByteArray(serDe, Number.class);
        assertEquals(sketchBytes.length, 49);

        Memory mem = new NativeMemory(sketchBytes);
        ReservoirItemsSketch<Number> loadedRis = ReservoirItemsSketch.<Number>getInstance(mem, serDe);

        assertEquals(ris.getNumSamples(), loadedRis.getNumSamples());

        Number[] samples1 = ris.getSamples(Number.class);
        Number[] samples2 = loadedRis.getSamples(Number.class);
        assertEquals(samples1.length, samples2.length);

        for (int i = 0; i < samples1.length; ++i) {
            assertEquals(samples1[i], samples2[i]);
        }
    }

    @Test
    public void checkBadConstructorArgs() {
        String[] data = new String[128];
        for (int i = 0; i < 128; ++i) {
            data[i] = Integer.toString(i);
        }

        ResizeFactor rf = ResizeFactor.X8;

        short encResSize256 = ReservoirSize.computeSize(256);
        short encResSize128 = ReservoirSize.computeSize(128);
        short encResSize64 = ReservoirSize.computeSize(64);
        short encResSize1 = ReservoirSize.computeSize(1);

        // no data
        try {
            new ReservoirItemsSketch<Byte>(null, 128, rf, encResSize128);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("null reservoir"));
        }

        // size too small
        try {
            new ReservoirItemsSketch(data, 128, rf, encResSize1);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("size less than 2"));
        }

        // configured reservoir size smaller than data length
        try {
            new ReservoirItemsSketch(data, 128, rf, encResSize64);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("max size less than array length"));
        }

        // too many items seen vs data length, full sketch
        try {
            new ReservoirItemsSketch(data, 512, rf, encResSize256);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("too few samples"));
        }

        // too many items seen vs data length, under-full sketch
        try {
            new ReservoirItemsSketch(data, 256, rf, encResSize256);
            fail();
        } catch (SketchesException e) {
            assertTrue(e.getMessage().contains("too few samples"));
        }
    }

    @Test
    public void checkSketchCapacity() {
        Long[] data = new Long[64];
        short encResSize = ReservoirSize.computeSize(64);
        long itemsSeen = (1 << 48) - 2;

        ReservoirItemsSketch<Long> ris = new ReservoirItemsSketch<Long>(data, itemsSeen, ResizeFactor.X8, encResSize);

        // this should work, the next should fail
        ris.update(0L);

        try {
            ris.update(0L);
            fail();
        } catch (SketchesStateException e) {
            assertTrue(e.getMessage().contains("Sketch has exceeded capacity for total items seen"));
        }
    }

    @Test
    public void checkSampleWeight() {
        int k = 32;
        ReservoirItemsSketch<Integer> ris = ReservoirItemsSketch.<Integer>getInstance(k);

        for (int i = 0; i < (k / 2); ++i) {
            ris.update(i);
        }
        assertEquals(ris.getImplicitSampleWeight(), 1.0); // should be exact value here

        // will have 3k/2 total samples when done
        for (int i = 0; i < k; ++i) {
            ris.update(i);
        }
        assertTrue(ris.getImplicitSampleWeight() - 1.5 < EPS);
    }

    private Memory getBasicSerializedLongsRIS() {
        int k = 10;
        int n = 20;

        ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.<Long>getInstance(k);
        assertEquals(ris.getNumSamples(), 0);

        for (int i = 0; i < n; ++i) {
            ris.update((long) i);
        }
        assertEquals(ris.getNumSamples(), Math.min(n, k));
        assertEquals(ris.getN(), n);
        assertEquals(ris.getK(), k);

        byte[] sketchBytes = ris.toByteArray(new ArrayOfLongsSerDe());
        return new NativeMemory(sketchBytes);
    }

    private void validateSerializeAndDeserialize(ReservoirItemsSketch<Long> ris) {
        byte[] sketchBytes = ris.toByteArray(new ArrayOfLongsSerDe());
        assertEquals(sketchBytes.length, (Family.RESERVOIR.getMaxPreLongs() + ris.getNumSamples()) << 3);

        // ensure full reservoir rebuilds correctly
        Memory mem = new NativeMemory(sketchBytes);
        ReservoirItemsSketch loadedRis = ReservoirItemsSketch.getInstance(mem, new ArrayOfLongsSerDe());

        validateReservoirEquality(ris, loadedRis);
    }

    private <T> void validateReservoirEquality(ReservoirItemsSketch<T> ris1, ReservoirItemsSketch<T> ris2) {
        assertEquals(ris1.getNumSamples(), ris2.getNumSamples());

        if (ris1.getNumSamples() == 0) { return; }

        Object[] samples1 = ris1.getSamples();
        Object[] samples2 = ris2.getSamples();
        assertEquals(samples1.length, samples2.length);

        for (int i = 0; i < samples1.length; ++i) {
            assertEquals(samples1[i], samples2[i]);
        }
    }

}
