package org.jetbrains.dataframe.aggregation

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.columnOf
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.mean
import org.junit.Test

class BasicMathTests {

    @Test
    fun `type for column with mixed numbers`() {
        val col = columnOf(10, 10.0, null)
        col.type() shouldBe getType<Number?>()
    }

    @Test
    fun `mean with nans and nulls`() {
        columnOf(10, 20, Double.NaN, null).mean() shouldBe Double.NaN
        columnOf(10, 20, Double.NaN, null).mean(skipNa = true) shouldBe 15
    }
}
