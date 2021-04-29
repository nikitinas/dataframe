package org.jetbrains.dataframe

import io.kotlintest.shouldBe
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.junit.Test

class MoveTests {

    val columnNames = listOf("q", "a.b", "b.c", "w", "a.c.d", "e.f", "b.d", "r")
    val columns = columnNames.map { column(it, emptyList<Int>()) }
    val df = columns.asDataFrame<Unit>()
    val grouped = df.move { cols { it.name.contains(".") } }.into { it.name.split(".") }

    @Test
    fun batchGrouping(){

        grouped.columnNames() shouldBe listOf("q", "a", "b", "w", "e", "r")
        grouped["a"].asFrame().columnNames() shouldBe listOf("b", "c")
        grouped["a"]["c"].asFrame().columnNames() shouldBe listOf("d")
        grouped["b"].asFrame().columnNames() shouldBe listOf("c", "d")
        grouped["e"].asFrame().columnNames() shouldBe listOf("f")
    }

    @Test
    fun `select all`(){

        grouped.getColumnsWithPaths { all() }.map { it.path.joinToString(".") } shouldBe grouped.columnNames()
    }

    @Test
    fun `select all dfs`() {

        val selected = grouped.getColumnsWithPaths { all().colsDfs() }.map { it.path.joinToString(".") }
        selected shouldBe listOf("a.b", "a.c", "a.c.d", "b.c", "b.d", "e.f")
    }

    @Test
    fun batchUngrouping(){

        val ungrouped = grouped.move { colsDfs { it.depth() > 0 && !it.isGroup() } }.into { path(it.path.joinToString(".")) }
        ungrouped.columnNames() shouldBe listOf("q", "a.b", "a.c.d", "b.c", "b.d", "w", "e.f", "r")
    }

    @Test
    fun `ungroup one`(){

        val ungrouped = grouped.remove("b").ungroup { it["a"] }
        ungrouped.columnNames() shouldBe listOf("q", "b", "c", "w", "e", "r")
        ungrouped["c"].asFrame().columnNames() shouldBe listOf("d")
    }

    @Test
    fun `flatten one`(){

        val flattened = grouped.flatten { it["a"] }
        flattened.columnNames() shouldBe listOf("q", "a_b", "a_c_d", "b", "w", "e", "r")
    }

    @Test
    fun `flatten several`(){

        val flattened = grouped.flatten { it["a"]["c"] and it["a"] and it["b"] }
        flattened.columnNames() shouldBe listOf("q", "a_b", "a_c_d", "b_c", "b_d", "w", "e", "r")
    }

    @Test
    fun `flatten all`(){

        val flattened = grouped.flatten(".")
        flattened.columnNames() shouldBe listOf("q", "a.b", "a.c.d", "b.c", "b.d", "w", "e.f", "r")
    }

    @Test
    fun `selectDfs`() {

        val selected = grouped.select { it["a"].colsDfs { !it.isGroup() }}
        selected.columnNames() shouldBe listOf("b", "d")
    }

    @Test
    fun `columnsWithPath in selector`() {

        val selected = grouped.getColumnsWithPaths { it["a"] }
        val actual = grouped.getColumnsWithPaths { selected.map { it.colsDfs() }.toColumnSet() }
        actual.map { it.path.joinToString(".") } shouldBe listOf("a.b", "a.c", "a.c.d")
    }
}