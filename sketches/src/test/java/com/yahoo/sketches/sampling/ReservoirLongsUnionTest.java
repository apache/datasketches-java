package com.yahoo.sketches.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ResizeFactor;


public class ReservoirLongsUnionTest {
    @Test
    public void checkInstantiation() {
        int n = 100;
        int k = 25;

        // crate empty unions
        ReservoirLongsUnion rlu = new ReservoirLongsUnion(k);
        ReservoirLongsSketch gadget = rlu.getResult();
        assertNotNull(gadget);
        assertNull(gadget.getSamples());

        rlu = new ReservoirLongsUnion(k, ResizeFactor.X4);
        gadget = rlu.getResult();
        assertNotNull(gadget);
        assertNull(gadget.getSamples());


        // pass in a sketch, as both an object and memory
        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);
        for (int i = 0; i < n; ++i) {
            rls.update(i);
        }

        rlu = new ReservoirLongsUnion(rls);
        assertNotNull(rlu.getResult());

        byte[] sketchBytes = rlu.toByteArray(); // only the gadget is serialized
        Memory mem = new NativeMemory(sketchBytes);

        ReservoirLongsUnion rebuiltGadget = new ReservoirLongsUnion(mem);

        ReservoirLongsSketchTest.validateReservoirEquality(rlu.getResult(), rebuiltGadget.getResult());
    }

    @Test(expectedExceptions = java.lang.NullPointerException.class)
    public void checkNullGadgetInstantiation() {
        new ReservoirLongsUnion((ReservoirLongsSketch) null);
    }

    @Test(expectedExceptions = java.lang.NullPointerException.class)
    public void checkNullMemoryInstantiation() {
        new ReservoirLongsUnion((Memory) null);
    }

    @Test
    public void checkStandardMergeNoCopy() {
        int k = 1024;
        int n1 = 256;
        int n2 = 256;
        ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
        ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

        ReservoirLongsUnion rlu = new ReservoirLongsUnion(sketch1);
        rlu.update(sketch2);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2);
        assertEquals(rlu.getResult().getNumSamples(), n1 + n2);

        // creating from Memory should avoid a copy
        int n3 = 2048;
        ReservoirLongsSketch sketch3 = getBasicSketch(n3, k);
        byte[] sketch3Bytes = sketch3.toByteArray();
        Memory mem = new NativeMemory(sketch3Bytes);
        rlu.update(mem);

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
        ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
        ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

        ReservoirLongsUnion rlu = new ReservoirLongsUnion(sketch1);
        rlu.update(sketch2);
        rlu.update(10);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2 + 1);
        assertEquals(rlu.getResult().getNumSamples(), k);
    }

    @Test
    public void checkWeightedMerge() {
        int k = 1024;
        int n1 = 16384;
        int n2 = 2048;
        ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
        ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

        ReservoirLongsUnion rlu = new ReservoirLongsUnion(sketch1);
        rlu.update(sketch2);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2);
        assertEquals(rlu.getResult().getNumSamples(), k);


        // now merge into the sketch for updating -- results should match
        rlu = new ReservoirLongsUnion(sketch2);
        rlu.update(sketch1);

        assertEquals(rlu.getResult().getK(), k);
        assertEquals(rlu.getResult().getN(), n1 + n2);
        assertEquals(rlu.getResult().getNumSamples(), k);

    }


    private static ReservoirLongsSketch getBasicSketch(final int n, final int k) {
        ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

        for (int i = 0; i < n; ++i) {
            rls.update(i);
        }

        return rls;
    }

}
