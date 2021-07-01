package org.jetbrains.dataframe.jupyter

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.intellij.lang.annotations.Language
import org.jetbrains.dataframe.columns.ValueColumn
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
}
