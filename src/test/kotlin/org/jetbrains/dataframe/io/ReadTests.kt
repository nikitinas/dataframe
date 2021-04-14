package org.jetbrains.dataframe.io

import io.kotlintest.shouldBe
import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.columns.allNulls
import org.jetbrains.dataframe.emptyDataFrame
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.ncol
import org.jetbrains.dataframe.nrow
import org.jetbrains.dataframe.print
import org.junit.Test

class ReadTests {

    @Test
    fun ghost(){
        DataFrame.read("data/ghost.json")
    }

    @Test
    fun readJsonNulls(){
        val data = """
            [{"a":null, "b":1},{"a":null, "b":2}]
        """.trimIndent()

        val df = DataFrame.readJsonStr(data)
        df.ncol shouldBe 2
        df.nrow shouldBe 2
        df["a"].hasNulls shouldBe true
        df["a"].allNulls() shouldBe true
        df.all { it["a"] == null } shouldBe true
        df["a"].type shouldBe getType<Any?>()
        df["b"].hasNulls shouldBe false
    }

    @Test
    fun read2(){
        DataFrame.read("data/2020-11-02.json")
    }

    @Test
    fun readFrameColumnEmptySlice(){
        val data = """
            [ [], [ {"a": [{"q":2},{"q":3}] } ] ]
        """.trimIndent()

        val df = DataFrame.readJsonStr(data)
        df.nrow() shouldBe 2
        df.ncol() shouldBe 1
        val empty = df[0][0] as AnyFrame
        empty.nrow() shouldBe 0
        empty.ncol() shouldBe 1
    }
}