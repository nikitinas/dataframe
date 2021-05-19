package org.jetbrains.dataframe.io

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.internal.schema.ColumnSchema
import org.junit.Test
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLConnection

class ReadTests {

    @Test
    fun ghost() {
        DataFrame.read("data/ghost.json")
    }

    @Test
    fun readJsonNulls() {
        val data = """
            [{"a":null, "b":1},{"a":null, "b":2}]
        """.trimIndent()

        val df = DataFrame.readJsonStr(data)
        df.ncol shouldBe 2
        df.nrow shouldBe 2
        df["a"].hasNulls() shouldBe true
        df["a"].allNulls() shouldBe true
        df.all { it["a"] == null } shouldBe true
        df["a"].type() shouldBe getType<Any?>()
        df["b"].hasNulls() shouldBe false
    }

    @Test
    fun readFrameColumn() {
        val data = """
            [{"a":[{"b":[]}]},{"a":[]},{"a":[{"b":[{"c":1}]}]}]
        """.trimIndent()
        val df = DataFrame.readJsonStr(data)
        df.nrow() shouldBe 3
        val a = df["a"].asTable()
        a[1]!!.nrow shouldBe 0
        a[0]!!.nrow shouldBe 1
        a[2]!!.nrow shouldBe 1
        val schema = a.schema.value
        schema.columns.size shouldBe 1
        val schema2 = schema.columns["b"] as ColumnSchema.Frame
        schema2.schema.columns.size shouldBe 1
        schema2.schema.columns["c"]!!.kind shouldBe ColumnKind.Value
    }

    @Test
    fun readFrameColumnEmptySlice() {
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


    @Test
    fun `read big decimal`() {
        val data = """
            [[3452345234345, 7795.34000000], [12314123532, 7795.34000000]]
        """.trimIndent()
        val df = DataFrame.readJsonStr(data)
        println(df.getColumn<List<Number>>("array")[0][1].javaClass)
    }

    @Test
    fun `http error`() {
        val url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT"
        val df = dataFrame(url)
        df.nrow() shouldBe 1
        df.columnNames() shouldBe listOf("code", "msg")
    }
}