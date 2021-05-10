package org.jetbrains.dataframe.internal.codeGen

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.dataframe.io.readJsonStr
import org.junit.Test

class MatchSchemeTests {

    @DataSchema(isOpen = false)
    interface Snippet {
        val position: Int
        val info: String
    }

    @DataSchema(isOpen = false)
    interface Item {
        val kind: String
        val id: String
        val snippet: DataRow<Snippet>
    }

    @DataSchema(isOpen = false)
    interface PageInfo {
        val totalResults: Int
        val resultsPerPage: Int
        val snippets: DataFrame<Snippet>
    }

    @DataSchema
    interface DataRecord {
        val kind: String
        val items: DataFrame<Item>
        val pageInfo: DataRow<PageInfo>
    }

    val json = """
        {
            "kind": "qq",
            "pageInfo": {
                "totalResults": 2,
                "resultsPerPage": 3,
                "snippets": [
                    {
                        "position": 3,
                        "info": "str"
                    },
                    {
                        "position": 5,
                        "info": "txt"
                    }
                ]
            },
            "items": [
                {
                    "kind": "asd",
                    "id": "zxc",
                    "snippet": {
                        "position": 2,
                        "info": "qwe"
                    }
                }
            ]
        }
    """.trimIndent()

    val df = DataFrame.readJsonStr(json)

    val typed = df.typed<DataRecord>()

    @Test
    fun `marker is reused`(){

        val codeGen = ReplCodeGenerator.create()
        codeGen.process(DataRecord::class)
        codeGen.process(typed, :: typed).hasConverter shouldBe false
        val generated = codeGen.process(df, :: df)
        generated.declarations.split("\n").size shouldBe 1
    }

    val modified = df.add("new"){4}

    @Test
    fun `marker is implemented`(){

        val codeGen = ReplCodeGenerator.create()
        codeGen.process(DataRecord::class)
        val generated = codeGen.process(modified, ::modified)!!
        generated.declarations.contains(DataRecord::class.simpleName!!) shouldBe true
    }

    @Test
    fun printSchema(){
        val res = df.generateCode(false, true)
        println(res)
    }
}