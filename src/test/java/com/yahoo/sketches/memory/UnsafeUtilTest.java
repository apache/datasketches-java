package com.yahoo.sketches.memory;

import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * @author <a href="mailto:jaredsburrows@gmail.com">Jared Burrows</a>
 */
public class UnsafeUtilTest {

    @Test(expectedExceptions = AssertionError.class,
            expectedExceptionsMessageRegExp = "offset: " + -1 + ", reqLength: " + 1 + ", size: " + 1)
    public void testAssertBoundsBadRequestOffset() {
        UnsafeUtil.assertBounds(-1, 1, 1);
    }

    @Test(expectedExceptions = AssertionError.class,
            expectedExceptionsMessageRegExp = "offset: " + 1 + ", reqLength: " + -1 + ", size: " + 1)
    public void testAssertBoundsBadRequestLength() {
        UnsafeUtil.assertBounds(1, -1, 1);
    }

    @Test(expectedExceptions = AssertionError.class,
            expectedExceptionsMessageRegExp = "offset: " + 1 + ", reqLength: " + 1 + ", size: " + -1)
    public void testAssertBoundsBadAllocatedSize() {
        UnsafeUtil.assertBounds(1, 1, -1);
    }

    @Test
    public void testAssertBounds() {
        UnsafeUtil.assertBounds(1, 1, 2);
    }

    @Test
    public void testCheckOverlapShouldReturnTrueWithNoOverlap() {
        assertTrue(UnsafeUtil.checkOverlap(-1, 1, 0)); // -1 + 0 <= 1, true
    }

    @Test
    public void testCheckOverlapShouldReturnFalseWithOverlap() {
        assertFalse(UnsafeUtil.checkOverlap(1, 1, 1)); // 1 + 1 <= 1, false
    }

    @Test
    public void testJDKDetect() throws Exception {
        // Get Java version
        final String specVersion = Runtime.class.getPackage().getSpecificationVersion();
        final double version = Double.parseDouble(specVersion);

        // Enclose class instance of 'UnsafeUtil'
        final Constructor<UnsafeUtil> unsafeUtilConstructor = UnsafeUtil.class.getDeclaredConstructor();
        unsafeUtilConstructor.setAccessible(true);
        final UnsafeUtil unsafeUtil = unsafeUtilConstructor.newInstance();

        // Get 'compatibilityMethods' value
        final Field field = unsafeUtil.getClass().getDeclaredField("compatibilityMethods");
        field.setAccessible(true);
        final Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        // Get the name of the class initialized
        final String className = field.get(null).getClass().getSimpleName();

        if (version >= 1.8) {
            assertEquals("JDK8Compatible", className);
        } else if (version == 1.7) {
            assertEquals("JDK7Compatible", className);
        } else {
            fail();
        }
    }
}

