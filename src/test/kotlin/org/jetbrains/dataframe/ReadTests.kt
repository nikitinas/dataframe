package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.io.readJsonStr
import org.junit.Test

class ReadTests {

    @Test
    fun parseJson1() {
        val json = """[
                {"a":1, "b":"text"},
                {"a":2, "b":5, "c":4.5}
            ]
        """.trimIndent()
        val df = DataFrame.readJsonStr(json)
        df.ncol() shouldBe 3
        df.nrow() shouldBe 2
        df["a"].type() shouldBe getType<Int>()
        df["b"].type() shouldBe getType<Comparable<*>>()
        df["c"].type() shouldBe getType<Double?>()
    }

    @Test
    fun parseJson2() {
        val json = """[
                {"a":"text"},
                {"a":{"b":2}},
                {"a":[6,7,8]}
            ]
        """.trimIndent()
        val df = DataFrame.readJsonStr(json)
        println(df)
        df.ncol() shouldBe 1
        df.nrow() shouldBe 3
        val group = df["a"] as ColumnGroup<*>
        group.ncol() shouldBe 3
        group["b"].type() shouldBe getType<Int?>()
        group["value"].type() shouldBe getType<String?>()
        group["array"].type() shouldBe getType<Many<Int>>()
    }

    @Test
    fun parseJson3() {
        val json = """[
                {"a":[3, 5]},
                {},
                {"a":[3.4, 5.6]}
            ]
        """.trimIndent()
        val df = DataFrame.readJsonStr(json)
        df.ncol() shouldBe 1
        df.nrow() shouldBe 3
        df["a"].type() shouldBe getType<Many<Number>>()
        df[1]["a"] shouldBe emptyList<Int>()
    }

    @Test
    fun parseJson4() {
        val json = """[
                {"a":[ {"b":2}, {"c":3} ]},
                {"a":[ {"b":4}, {"d":5} ]}
            ]
        """.trimIndent()
        val df = DataFrame.readJsonStr(json)
        df.ncol() shouldBe 1
        df.nrow() shouldBe 2
        println(df)
        val group = df["a"] as FrameColumn<*>
    }
}
