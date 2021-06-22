package org.jetbrains.dataframe.jupyter

import io.kotest.matchers.shouldNotBe
import org.jetbrains.dataframe.DataFrame
import org.junit.Test
import java.io.BufferedReader
import java.io.InputStreamReader

class ResourcesTest {

    @Test
    fun `resources available`() {
        val res = DataFrame::class.java.getResourceAsStream("/table.html")
        println(InputStreamReader(res).readText())

        res shouldNotBe null
    }
}