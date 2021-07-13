package org.jetbrains.dataframe.jupyter

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.intellij.lang.annotations.Language
import org.jetbrains.dataframe.columns.ValueColumn
import org.jetbrains.kotlinx.jupyter.api.MimeTypedResult
import org.junit.Test

class JupyterCodegenTests : AbstractReplTest() {
    @Test
    fun `codegen for enumerated frames`() {
        @Language("kts")
        val res1 = exec(
            """
            val names = (0..2).map { it.toString() }
            val df = dataFrameOf(names)(1, 2, 3)
            """.trimIndent()
        )
        res1 shouldBe Unit

        val res2 = execWrapped("$WRAP(df.`1`)")
        res2.shouldBeInstanceOf<ValueColumn<*>>()
    }

    @Test
    fun `codegen for complex column names`() {
        @Language("kts")
        val res1 = exec(
            """
            val df = DataFrame.readDelimStr("[a], (b), {c}\n1, 2, 3")
            df
            """.trimIndent()
        )
        res1.shouldBeInstanceOf<MimeTypedResult>()

        val res2 = exec(
            """listOf(df.`{a}`[0], df.`{b}`[0], df.`{c}`[0])"""
        )
        res2 shouldBe listOf(1, 2, 3)
    }
}
