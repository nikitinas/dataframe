package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.junit.Test

class BasicMathTests {

    @Test
    fun `mean with nulls`() {
        columnOf(10, 20, null).mean() shouldBe 15
        columnOf(10, 20, null).mean(skipNa = false) shouldBe Double.NaN
    }
}