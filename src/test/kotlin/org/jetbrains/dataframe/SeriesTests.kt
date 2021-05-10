package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.ColumnReference
import org.junit.Test

class SeriesTests {

    val df = dataFrameOf("city", "day", "temp")(
            "Moscow", 1, 14,
            "London", 1, 10,
            "Moscow", 3, 18,
            "London", 3, 16,
            "Moscow", 6, 16,
            "London", 6, 23,
            "Moscow", 4, 13,
            "London", 4, 22,
            "Moscow", 2, 20,
            "London", 2, 15,
            "Moscow", 5, 10,
            "London", 5, 18
    )

    // Generated code

    @DataSchema
    interface Weather {
        val city: String
        val day: Int
        val temp: Int
    }

    val DataRowBase<Weather>.city get() = this["city"] as String
    val DataRowBase<Weather>.day get() = this["day"] as Int
    val DataRowBase<Weather>.temp get() = this["temp"] as Int
    val DataFrameBase<Weather>.city get() = this["city"] as ColumnReference<String>
    val DataFrameBase<Weather>.day get() = this["day"] as ColumnReference<Int>
    val DataFrameBase<Weather>.temp get() = this["temp"] as ColumnReference<Int>

    val typed = df.typed<Weather>()

    @Test
    fun `diff test`() {
        val withDiff = typed
                .sortBy { city and day }
                .groupBy { city }
                .add("diff") { diff { it.temp } }
                .ungroup()

        val srcData = typed.map { (city to day) to temp }.toMap()
        val expected = typed.sortBy {city and day}.map { row -> srcData[city to (day-1)]?.let { row.temp - it} ?: 0 }
        withDiff["diff"].values shouldBe expected
    }

    @Test
    fun `movingAverage`() {
        val k = 3
        val withMa = typed
                .groupBy { city }
                .sortBy { city and day }
                .add("ma_temp") { it.movingAverage(k) { it.temp } }
                .ungroup()

        val srcData = typed.map { (city to day) to temp }.toMap()
        val expected = typed
                .sortBy { city and day }
                .map { (0 until k).map { srcData[city to day - it] }.filterNotNull().let { it.sum().toDouble() / it.size } }

        withMa["ma_temp"].values shouldBe expected
    }
}