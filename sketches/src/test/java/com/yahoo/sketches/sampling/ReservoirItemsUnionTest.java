package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.SketchesArgumentException;

// Tests mostly focus on Long since other types are already tested in ReservoirItemsSketchTest.
public class ReservoirItemsUnionTest {
    @Test
    public void checkEmptyUnion() {
        ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(1024);
        byte[] unionBytes = riu.toByteArray(new ArrayOfLongsSerDe());

        // will intentionally break if changing empty union serialization
        assertEquals(unionBytes.length, 8);

        println(riu.toString());
    }

    @Test
    public void checkInstantiation() {
        int n = 100;
        int k = 25;

        // create empty unions
        ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
        assertNull(riu.getResult());
        riu.update(5L);
        assertNotNull(riu.getResult());

        // pass in a sketch, as both an object and memory
        ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(k);
        for (long i = 0; i < n; ++i) {
            ris.update(i);
        }

        riu = ReservoirItemsUnion.getInstance(ris.getK());
        riu.update(ris);
        assertNotNull(riu.getResult());

        ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
        byte[] sketchBytes = ris.toByteArray(serDe); // only the gadget is serialized
        Memory mem = new NativeMemory(sketchBytes);
        riu = ReservoirItemsUnion.getInstance(ris.getK());
        riu.update(mem, serDe);
        assertNotNull(riu.getResult());

        println(riu.toString());
    }

    @Test
    public void checkSerialization() {
        int n = 100;
        int k = 25;

        ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
        for (long i = 0; i < n; ++i) { riu.update(i); }

        ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
        byte[] unionBytes = riu.toByteArray(serDe);
        Memory mem = new NativeMemory(unionBytes);

        ReservoirItemsUnion<Long> rebuiltUnion = ReservoirItemsUnion.getInstance(mem, serDe);
        assertEquals(riu.getMaxK(), rebuiltUnion.getMaxK());
        ReservoirItemsSketchTest.validateReservoirEquality(riu.getResult(), rebuiltUnion.getResult());
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void checkNullMemoryInstantiation() {
        ReservoirItemsUnion.getInstance(null, new ArrayOfStringsSerDe());
    }

    @Test
    public void checkDownsampledUpdate() {
        int bigK = 1024;
        int smallK = 256;
        int n = 2048;
        ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n, smallK);
        ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n, bigK);

        ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(smallK);
        assertEquals(riu.getMaxK(), smallK);

        riu.update(sketch1);
        assertEquals(riu.getResult().getK(), smallK);

        riu.update(sketch2);
        assertEquals(riu.getResult().getK(), smallK);
        assertEquals(riu.getResult().getNumSamples(), smallK);
    }

    @Test
    public void checkStandardMergeNoCopy() {
        int k = 1024;
        int n1 = 256;
        int n2 = 256;
        ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
        ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

        ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
        riu.update(sketch1);
        riu.update(sketch2);

        assertEquals(riu.getResult().getK(), k);
        assertEquals(riu.getResult().getN(), n1 + n2);
        assertEquals(riu.getResult().getNumSamples(), n1 + n2);

        // creating from Memory should avoid a copy
        int n3 = 2048;
        ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
        ReservoirItemsSketch<Long> sketch3 = getBasicSketch(n3, k);
        byte[] sketch3Bytes = sketch3.toByteArray(serDe);
        Memory mem = new NativeMemory(sketch3Bytes);
        riu.update(mem, serDe);

        assertEquals(riu.getResult().getK(), k);
        assertEquals(riu.getResult().getN(), n1 + n2 + n3);
        assertEquals(riu.getResult().getNumSamples(), k);
    }

    @Test
    public void checkStandardMergeWithCopy() {
        // this will check the other code route to a standard merge,
        // but will copy sketch2 to be non-destructive.
        int k = 1024;
        int n1 = 768;
        int n2 = 2048;
        ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
        ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

        ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
        riu.update(sketch1);
        riu.update(sketch2);
        riu.update(10L);

        assertEquals(riu.getResult().getK(), k);
        assertEquals(riu.getResult().getN(), n1 + n2 + 1);
        assertEquals(riu.getResult().getNumSamples(), k);
    }

    @Test
    public void checkWeightedMerge() {
        int k = 1024;
        int n1 = 16384;
        int n2 = 2048;
        ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
        ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

        ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
        riu.update(sketch1);
        riu.update(sketch2);

        assertEquals(riu.getResult().getK(), k);
        assertEquals(riu.getResult().getN(), n1 + n2);
        assertEquals(riu.getResult().getNumSamples(), k);

        // now merge into the sketch for updating -- results should match
        riu = ReservoirItemsUnion.getInstance(k);
        riu.update(sketch2);
        riu.update(sketch1);

        assertEquals(riu.getResult().getK(), k);
        assertEquals(riu.getResult().getN(), n1 + n2);
        assertEquals(riu.getResult().getNumSamples(), k);

    }

    @Test
    public void checkPolymorphicType() {
        int k = 4;

        ReservoirItemsUnion<Number> rlu = ReservoirItemsUnion.getInstance(k);
        rlu.update(2.2);
        rlu.update(6L);

        ReservoirItemsSketch<Number> rls = ReservoirItemsSketch.getInstance(k);
        rls.update(1);
        rls.update(3.7f);

        rlu.update(rls);

        ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();
        byte[] sketchBytes = rlu.toByteArray(serDe, Number.class);
        Memory mem = new NativeMemory(sketchBytes);

        ReservoirItemsUnion<Number> rebuiltRlu = ReservoirItemsUnion.getInstance(mem, serDe);

        // validateReservoirEquality can't handle abstract base class
        assertEquals(rlu.getResult().getNumSamples(), rebuiltRlu.getResult().getNumSamples());

        Number[] samples1 = rlu.getResult().getSamples(Number.class);
        Number[] samples2 = rebuiltRlu.getResult().getSamples(Number.class);
        assertEquals(samples1.length, samples2.length);

        for (int i = 0; i < samples1.length; ++i) {
            assertEquals(samples1[i], samples2[i]);
        }
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadPreLongs() {
        ReservoirItemsUnion<Number> riu = ReservoirItemsUnion.getInstance(1024);
        Memory mem = new NativeMemory(riu.toByteArray(new ArrayOfNumbersSerDe()));
        mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

        ReservoirItemsUnion.getInstance(mem, new ArrayOfNumbersSerDe());
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadSerVer() {
        ReservoirItemsUnion<String> riu = ReservoirItemsUnion.getInstance(1024);
        Memory mem = new NativeMemory(riu.toByteArray(new ArrayOfStringsSerDe()));
        mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

        ReservoirItemsUnion.getInstance(mem, new ArrayOfStringsSerDe());
        fail();
    }

    @Test(expectedExceptions = SketchesArgumentException.class)
    public void checkBadFamily() {
        ReservoirItemsUnion<Double> rlu = ReservoirItemsUnion.getInstance(1024);
        Memory mem = new NativeMemory(rlu.toByteArray(new ArrayOfDoublesSerDe()));
        mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

        ReservoirItemsUnion.getInstance(mem, new ArrayOfDoublesSerDe());
        fail();
    }

    private static ReservoirItemsSketch<Long> getBasicSketch(final int n, final int k) {
        ReservoirItemsSketch<Long> rls = ReservoirItemsSketch.getInstance(k);

        for (long i = 0; i < n; ++i) {
            rls.update(i);
        }

        return rls;
    }

    /**
     * Wrapper around System.out.println() allowing a simple way to disable logging in tests
     * @param msg The message to print
     */
    private static void println(String msg) {
        //System.out.println(msg);
    }
}
