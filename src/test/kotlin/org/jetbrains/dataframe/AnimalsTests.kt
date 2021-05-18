package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.junit.Test

class AnimalsTests {

    val animal by columnOf("cat", "cat", "snake", "dog", "dog", "cat", "snake", "cat", "dog", "dog")
    val age by columnOf(2.5, 3.0, 0.5, Double.NaN, 5.0, 2.0, 4.5, Double.NaN, 7.0, 3.0)
    val visits by columnOf(1, 3, 2, 3, 2, 3, 1, 1, 2, 1)
    val priority by columnOf("yes", "yes", "no", "yes", "no", "no", "no", "yes", "no", "no")

    val df = dataFrameOf(animal, age, visits, priority)

    @Test
    fun `ignore nans`() {
        df.mean("age") shouldBe 3.4375
    }

    @Test
    fun mean() {
        val mean = df.mean().transpose()
        mean.ncol() shouldBe 2
        mean.nrow() shouldBe 2
        mean.column(1).type() shouldBe getType<Double>()
        mean.column(0).values() shouldBe listOf("age", "visits")
    }

    @Test
    fun `mean of empty`(){
        val cleared = df.update { age }.with(Double.NaN).update { visits }.withNull()
        val mean = cleared.mean()
        mean[age.name()] shouldBe Double.NaN
        mean[visits.name()] shouldBe Double.NaN
    }
}