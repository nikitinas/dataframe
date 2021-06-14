package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.junit.Test

class PivotTests {

    val a by columnOf(0, 1, 1)
    val b by columnOf("q", "q", "w")
    val c by columnOf('x', 'y', 'z')

    val df = dataFrameOf(a, b, c)

    @Test
    fun `simple pivot`(){

        val pivoted = df.pivot(b).withIndex(a).values(c)
        pivoted.columnNames() shouldBe listOf("a", "q", "w")
        pivoted.nrow shouldBe 2
        pivoted["q"].values() shouldBe listOf('x', 'y')
        pivoted["w"].values() shouldBe listOf(null, 'z')
    }

    @Test
    fun `pivot with rename`(){

        val pivoted = df.pivot(b).withIndex(a).values { c and (c named "d") }
        pivoted.columnNames() shouldBe listOf("a", "q", "w")
        pivoted.nrow shouldBe 2
        pivoted.print()

        pivoted["q"]["c"].values() shouldBe listOf('x', 'y')
        pivoted["q"]["d"].values() shouldBe listOf('x', 'y')
        pivoted["w"]["c"].values() shouldBe listOf(null, 'z')
        pivoted["w"]["d"].values() shouldBe listOf(null, 'z')
    }

    @Test
    fun `pivot aggregate with default`() {

        val pivoted = df.pivot(b).withIndex(a).aggregate {
            get(c).first() default '-' into "first"
            get(c).last() into "last" default '?'
        }
        pivoted.ncol() shouldBe 3
        pivoted.nrow() shouldBe 2
        val cols = pivoted.getColumns { except(a).dfs() }
        cols.size shouldBe 4
        cols.forEach {
            it.type() shouldBe getType<Char>()
        }
        pivoted["w"]["first"][0] shouldBe '-'
        pivoted["w"]["last"][0] shouldBe '?'
    }
}