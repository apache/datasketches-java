package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


/**
 * Empty essentially means that the sketch has never seen data.
 *
 * @author Lee Rhodes
 */
public class EmptyTest {

  @Test
  public void checkEmpty() {
    UpdateSketch sk1 = Sketches.updateSketchBuilder().build();
    UpdateSketch sk2 = Sketches.updateSketchBuilder().build();
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();

    int u = 100;
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.update(sk1);
    inter.update(sk2);

    CompactSketch csk = inter.getResult();
    //The intersection of two disjoint, exact-mode sketches is empty, T == 1.0.
    println(csk.toString());
    assertTrue(csk.isEmpty());

    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    aNotB.update(csk, sk1);
    CompactSketch csk2 = aNotB.getResult();
    //The AnotB of an empty, T == 1.0 sketch with another exact-mode sketch is empty, T == 1.0
    assertTrue(csk2.isEmpty());
  }

  @Test
  public void checkNotEmpty() {
    UpdateSketch sk1 = Sketches.updateSketchBuilder().build();
    UpdateSketch sk2 = Sketches.updateSketchBuilder().build();
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();

    int u = 10000;
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.update(sk1);
    inter.update(sk2);

    CompactSketch csk = inter.getResult();
    println(csk.toString());
    //The intersection of two disjoint, est-mode sketches is not-empty, T < 1.0.
    assertFalse(csk.isEmpty());

    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    aNotB.update(csk, sk1); //empty, T < 1.0; with est-mode sketch
    CompactSketch csk2 = aNotB.getResult();
    println(csk2.toString());
    //The AnotB of an empty, T < 1.0 sketch with another exact-mode sketch is not-empty.
    assertFalse(csk2.isEmpty());

    UpdateSketch sk3 = Sketches.updateSketchBuilder().build();
    aNotB = Sketches.setOperationBuilder().buildANotB();
    aNotB.update(sk3, sk1); //empty, T == 1.0; with est-mode sketch
    CompactSketch csk3 = aNotB.getResult();
    println(csk3.toString());
    //the AnotB of an empty, T == 1.0 sketch with another est-mode sketch is empty, T < 1.0
    assertTrue(csk3.isEmpty());
  }

  @Test
  public void checkPsampling() {
    UpdateSketch sk1 = Sketches.updateSketchBuilder().setP(.5F).build();
    assertTrue(sk1.isEmpty());
    //However, an empty P-sampling sketch where T < 1.0 and has never seen data is also empty
    // and will have a full preamble of 24 bytes.
    assertEquals(sk1.compact().toByteArray().length, 24);
  }

  /**
   * @param s
   *          value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
