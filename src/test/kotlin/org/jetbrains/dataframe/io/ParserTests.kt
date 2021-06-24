package org.jetbrains.dataframe.io

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.*
import org.junit.Test
import java.time.LocalDateTime

class ParserTests {

    @Test
    fun `parse datetime with custom format`() {
        val col by columnOf("04.02.2021 19:44:32")
        col.tryParse().type() shouldBe getType<String>()
        DataFrame.parser.addDateTimeFormat("dd.MM.uuuu HH:mm:ss")
        val parsed = col.parse()
        parsed.type() shouldBe getType<LocalDateTime>()
        parsed.castTo<LocalDateTime>()[0].year shouldBe 2021
    }

    @Test(expected = IllegalStateException::class)
    fun `parse should throw`() {
        val col by columnOf("a", "b")
        col.parse()
    }

    @Test(expected = IllegalStateException::class)
    fun `converter should throw`() {
        val col by columnOf("a", "b")
        col.convertTo<Int>()
    }

    @Test(expected = IllegalStateException::class)
    fun `converter for mixed column should throw`() {
        val col by columnOf(1, "a")
        col.convertTo<Int>()
    }

    @Test
    fun `convert mixed column`() {
        val col by columnOf(1.0, "1")
        val converted = col.convertTo<Int>()
        converted.type() shouldBe getType<Int>()
        converted[0] shouldBe 1
        converted[1] shouldBe 1
    }
}
