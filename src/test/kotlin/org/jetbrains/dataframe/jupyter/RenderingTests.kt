package org.jetbrains.dataframe.jupyter

import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.intellij.lang.annotations.Language
import org.jetbrains.dataframe.test.containNTimes
import org.junit.Test

class RenderingTests : AbstractReplTest() {
    @Test
    fun `dataframe is rendered to html`() {
        @Language("kts")
        val html = execHtml(
            """
            val name by column<String>()
            val height by column<Int>()
            val df = dataFrameOf(name, height)(
                "Bill", 135,
                "Mark", 160
            ).typed<Unit>()
            df
            """.trimIndent()
        )
        html shouldContain "Bill"

        @Language("kts")
        val useRes = exec(
            """
            USE {
                render<Int> { (it * 2).toString() }
            }
            """.trimIndent()
        )
        useRes shouldBe Unit

        val html2 = execHtml("df")
        html2 shouldContain (135 * 2).toString()
        html2 shouldContain (160 * 2).toString()
    }

    @Test
    fun `rendering options`() {
        @Language("kts")
        val html1 = execHtml(
            """
            data class Person(val age: Int, val name: String)
            val df = (1..70).map { Person(it, "A".repeat(it)) }.toDataFrameByProperties()
            df
            """.trimIndent()
        )
        html1 should containNTimes("<tr>", 21)

        @Language("kts")
        val html2 = execHtml(
            """
            dataFrameConfig.display.rowsLimit = 50
            df
            """.trimIndent()
        )
        html2 should containNTimes("<tr>", 51)
    }

    @Test
    fun htmlTagsAreEscaped() {
        @Language("kts")
        val res = execHtml(
            """
            dataFrameOf("name", "int")("<Air France> (12)", 1)
            """.trimIndent()
        )
        res shouldContain "&#60;Air France&#62;"
    }
}
