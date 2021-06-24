package org.jetbrains.dataframe.rendering

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.columnOf
import org.jetbrains.dataframe.dataFrameOf
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.io.renderToStringTable
import org.jetbrains.dataframe.parse
import org.jetbrains.dataframe.toDataFrame
import org.junit.Test
import java.net.URL

class RenderingTests {

    @Test
    fun `render row with unicode values as table`(){

        val value = "Шёл Шива по шоссе, сокрушая сущее.\nА на встречу Саша шла, круглое сосущая"
        val col by columnOf(value)
        val df = col.toDataFrame()
        val rendered = df[0].renderToStringTable()
        println(rendered)
        rendered.contains("Шива") shouldBe true
        rendered.contains("\n") shouldBe false
        rendered.contains("А") shouldBe true
        rendered.contains("...") shouldBe true
        rendered.contains("Саша") shouldBe false
    }

    @Test
    fun `parse url`(){
        val df = dataFrameOf("url")("http://www.google.com").parse()
        df["url"].type() shouldBe getType<URL>()
    }
}