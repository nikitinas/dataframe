package org.jetbrains.dataframe

import org.junit.Ignore
import org.junit.Test
import kotlin.random.Random
import kotlin.system.measureTimeMillis

class PerformanceTests {

    @Test @Ignore
    fun `compare filter`() {
        val nrow = 1000000
        val ncol = 50
        val rand = Random(100)
        val columns = (0 until ncol).map { column("col$it", (0 until nrow).map { rand.nextInt() }) }

        val df = columns.asDataFrame<Unit>()

        println("start computing")
        val n = 10
        val t1 = (0..n).map { measureTimeMillis { df.filter { Math.abs(it["col2"] as Int + it["col5"] as Int) < 0 } } }
        println(t1)
    }
}
