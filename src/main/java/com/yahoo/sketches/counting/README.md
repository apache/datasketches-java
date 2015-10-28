# Frequency counting

This kind of sketches implement several variations of a very simple (yet surprizing and useful) idea.

The algorithm is most commonly known as the "Misra-Gries algorithm", "frequent items" or "space-saving".
It was discovered and rediscovered and redesigned several times over the years.

* *Finding repeated elements, Misra, Gries, 1982* <br>
* *Frequency estimation of internet packet streams with limited space, Demaine, Lopez-Ortiz, Munro, 2002* <br>
* *A simple algorithm for finding frequent elements in streams and bags, Karp, Shenker, Papadimitriou, 2003* <br>
* *Efficient Computation of Frequent and Top-k Elements in Data Streams, Metwally, Agrawal, Abbadi, 2006* <br>


## What it doeas

The frequent-items sketch is iniialized with a maxSize paramter. The sketch will keep only maxSize counts at any given time.
Upon update, the sketch is given a key (long). The frequency of a key f(key) is the number of times the key was added to the sketch (using update).

At any point, the sketch can return g(key) for any key (using get(key)) such that

f(key) > g(key) > f(key) - n/maxSize

where n is the number of updates to the sketch.


