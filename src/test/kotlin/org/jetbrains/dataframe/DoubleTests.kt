package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.junit.Test

class DoubleTests {

    @Test
    fun `filter not null with nans`(){
        val age by columnOf(2.3, Double.NaN, 1.0, "asd", 3, 'a')
        val df = dataFrameOf(age)
        df.filter { age() == Double.NaN }.nrow() shouldBe 1
        df.filter { age eq Double.NaN }.nrow() shouldBe 1
        //df.filter { age().isNaN() }.nrow() shouldBe 1
        //df.filter { it[age].isNaN() }.nrow() shouldBe 1
    }
}