package org.apache.datasketches.tuple.aninteger;

import org.apache.datasketches.tuple.AnotB;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Intersection;
import org.testng.annotations.Test;

/**
 * Issue #368, from Mikhail Lavrinovich 12 OCT 2021
 * The failure was AnotB(estimating {<1.0,1,F}, Intersect(estimating{<1.0,1,F}, newDegenerative{<1.0,0,T},
 * Which should be equal to AnotB(estimating{<1.0,1,F}, new{1.0,0,T} = estimating{<1.0, 1, F}. The AnotB
 * threw a null pointer exception because it was not properly handling sketches with zero entries.
 */
public class MikhailsBugTuple {

  @Test
  public void mikhailsBug() {
    IntegerSketch x = new IntegerSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    IntegerSketch y = new IntegerSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    x.update(1L, 1);
    IntegerSummarySetOperations setOperations =
        new IntegerSummarySetOperations(IntegerSummary.Mode.Min, IntegerSummary.Mode.Min);
    Intersection<IntegerSummary> intersection = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> intersect = intersection.intersect(x, y);
    AnotB.aNotB(x, intersect); // NPE was here
  }

  //@Test
  public void withTuple() {
    IntegerSketch x = new IntegerSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    IntegerSketch y = new IntegerSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    x.update(1L, 1);
    println("Tuple x: Estimating {<1.0,1,F}");
    println(x.toString());
    println("Tuple y: NewDegenerative {<1.0,0,T}");
    println(y.toString());
    IntegerSummarySetOperations setOperations =
        new IntegerSummarySetOperations(IntegerSummary.Mode.Min, IntegerSummary.Mode.Min);
    Intersection<IntegerSummary> intersection = new Intersection<>(setOperations);
    CompactSketch<IntegerSummary> intersect = intersection.intersect(x, y);
    println("Tuple Intersect(Estimating, NewDegen) = new {1.0, 0, T}");
    println(intersect.toString());
    CompactSketch<IntegerSummary> csk = AnotB.aNotB(x, intersect);
    println("Tuple AnotB(Estimating, New) = estimating {<1.0, 1, F}");
    println(csk.toString());
  }

  /**
   * Println an object
   * @param o object to print
   */
  private static void println(Object o) {
    //System.out.println(o.toString()); //disable here
  }
}