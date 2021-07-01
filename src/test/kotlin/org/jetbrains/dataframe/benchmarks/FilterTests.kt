package org.jetbrains.dataframe.benchmarks

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.io.read
import org.junit.Ignore
import org.junit.Test
import kotlin.system.measureTimeMillis

class FilterTests {

    val path = "data/census.csv"
    val df = DataFrame.read(path)

    interface DataRecord {
        val Referer: String?
    }

    val DataFrameBase<DataRecord>.Referer: DataColumn<String?> @JvmName("DataRecord_Referer") get() = this["Referer"] as DataColumn<String?>
    val DataRowBase<DataRecord>.Referer: String? @JvmName("DataRecord_Referer") get() = this["Referer"] as String?

    val typed = df.typed<DataRecord>()

    val n = 100

    @Test
    @Ignore
    fun slow() {
        measureTimeMillis {
            for (i in 0..n)
                typed.filter { Referer != null }
        }.let { println(it) }
    }

    @Test
    @Ignore
    fun fast() {
        measureTimeMillis {
            for (i in 0..n)
                typed.filterFast { Referer neq null }
        }.let { println(it) }
    }
}
