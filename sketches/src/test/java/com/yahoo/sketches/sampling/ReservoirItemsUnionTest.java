package com.yahoo.sketches.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.ResizeFactor;
import org.testng.annotations.Test;

// Tests mostly focus on Long since other types are already tested in ReservoirItemsSketchTest.
public class ReservoirItemsUnionTest {
    @Test
    public void checkInstantiation() {
        int n = 100;
        int k = 25;

        // crate empty unions
        ReservoirItemsUnion<Long> rlu = new ReservoirItemsUnion<>(k);
        ReservoirItemsSketch gadget = rlu.getResult();
        assertNotNull(gadget);
        assertNull(gadget.getSamples());

        rlu = new ReservoirItemsUnion<>(k, ResizeFactor.X4);
        gadget = rlu.getResult();
        assertNotNull(gadget);
        assertNull(gadget.getSamples());


        // pass in a sketch, as both an object and memory
        ReservoirItemsSketch<Long> rls = ReservoirItemsSketch.getInstance(k);
        for (long i = 0; i < n; ++i) {
            rls.update(i);
        }

        rlu = new ReservoirItemsUnion<>(rls);
        assertNotNull(rlu.getResult());

        ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

        byte[] sketchBytes = rlu.toByteArray(serDe); // only the gadget is serialized
        Memory mem = new NativeMemory(sketchBytes);

        ReservoirItemsUnion<Long> rebuiltGadget = new ReservoirItemsUnion<>(mem, serDe);

        ReservoirItemsSketchTest.validateReservoirEquality(rlu.getResult(), rebuiltGadget.getResult());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void checkNullGadgetInstantiation() {
        new ReservoirItemsUnion<>(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void checkNullMemoryInstantiation() {
        new ReservoirItemsUnion<>(null, new ArrayOfStringsSerDe());
    }

    @Test
    public void checkStandardMergeNoCopy() {
        int k = 1024;
        int n1 = 256;
        int n2 = 256;
        ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
        ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

        ReservoirItemsUnion<Long> rlu = new ReservoirItemsUnion<>(sketch1);
        rlu.update(sketch2);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2);
        assertEquals(rlu.getResult().getNumSamples(), n1 + n2);

        // creating from Memory should avoid a copy
        int n3 = 2048;
        ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
        ReservoirItemsSketch<Long> sketch3 = getBasicSketch(n3, k);
        byte[] sketch3Bytes = sketch3.toByteArray(serDe);
        Memory mem = new NativeMemory(sketch3Bytes);
        rlu.update(mem, serDe);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2 + n3);
        assertEquals(rlu.getResult().getNumSamples(), k);
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

        ReservoirItemsUnion<Long> rlu = new ReservoirItemsUnion<>(sketch1);
        rlu.update(sketch2);
        rlu.update(10L);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2 + 1);
        assertEquals(rlu.getResult().getNumSamples(), k);
    }

    @Test
    public void checkWeightedMerge() {
        int k = 1024;
        int n1 = 16384;
        int n2 = 2048;
        ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
        ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

        ReservoirItemsUnion<Long> rlu = new ReservoirItemsUnion<>(sketch1);
        rlu.update(sketch2);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2);
        assertEquals(rlu.getResult().getNumSamples(), k);


        // now merge into the sketch for updating -- results should match
        rlu = new ReservoirItemsUnion<>(sketch2);
        rlu.update(sketch1);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2);
        assertEquals(rlu.getResult().getNumSamples(), k);

    }

    @Test
    public void checkPolymorphicType() {
        int k = 4;

        ReservoirItemsUnion<Number> rlu = new ReservoirItemsUnion<>(k);
        rlu.update(2.2);
        rlu.update(6L);

        ReservoirItemsSketch<Number> rls = ReservoirItemsSketch.getInstance(k);
        rls.update(1);
        rls.update(3.7f);

        rlu.update(rls);

        ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();
        byte[] sketchBytes = rlu.toByteArray(serDe, Number.class);
        Memory mem = new NativeMemory(sketchBytes);

        ReservoirItemsUnion<Number> rebuiltRlu = new ReservoirItemsUnion<>(mem, serDe);

        // validateReservoirEquality can't handle abstract base class
        assertEquals(rlu.getResult().getNumSamples(), rebuiltRlu.getResult().getNumSamples());

        Object[] samples1 = rlu.getResult().getSamples(Number.class);
        Object[] samples2 = rebuiltRlu.getResult().getSamples(Number.class);
        assertEquals(samples1.length, samples2.length);

        for (int i = 0; i < samples1.length; ++i) {
            assertEquals(samples1[i], samples2[i]);
        }
    }


    private static ReservoirItemsSketch<Long> getBasicSketch(final int n, final int k) {
        ReservoirItemsSketch<Long> rls = ReservoirItemsSketch.getInstance(k);

        for (long i = 0; i < n; ++i) {
            rls.update(i);
        }

        return rls;
    }

}
