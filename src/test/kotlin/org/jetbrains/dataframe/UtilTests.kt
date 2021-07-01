package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.junit.Test
import java.io.Serializable

class UtilTests {

    @Test
    fun commonParentsTests() {
        commonParents(Int::class, Int::class) shouldBe listOf(Int::class)
        commonParents(Double::class, Int::class) shouldBe listOf(Number::class, Comparable::class)
        commonParents(Int::class, String::class) shouldBe listOf(Serializable::class, Comparable::class)
        commonParents(IllegalArgumentException::class, UnsupportedOperationException::class) shouldBe listOf(RuntimeException::class)
    }

    @Test
    fun commonParentTests() {
        commonParent(Int::class, Int::class) shouldBe Int::class
        commonParent(Double::class, Int::class) shouldBe Number::class
        commonParent(Int::class, String::class) shouldBe Serializable::class
    }
}
