package org.jetbrains.dataframe.internal.codeGen

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.DataFrameBase
import org.jetbrains.dataframe.DataRowBase
import org.jetbrains.dataframe.annotations.DataSchema
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.filter
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGeneratorImpl
import org.jetbrains.dataframe.impl.codeGen.process
import org.jetbrains.dataframe.person.BaseTest
import org.jetbrains.dataframe.select
import org.junit.Test

class ReplCodeGenTests : BaseTest() {

    class Test1 {
        @DataSchema(isOpen = false)
        interface _DataFrameType

        @DataSchema(isOpen = false)
        interface _DataFrameType1 : _DataFrameType

        @DataSchema(isOpen = false)
        interface _DataFrameType2 : _DataFrameType
    }

    class Test2 {
        @DataSchema(isOpen = false)
        interface _DataFrameType

        @DataSchema(isOpen = false)
        interface _DataFrameType1

        @DataSchema(isOpen = false)
        interface _DataFrameType2 : _DataFrameType, _DataFrameType1
    }

    @Test
    fun `process derived markers`() {
        val repl = ReplCodeGenerator.create()
        val code = repl.process(df).declarations

        val dfName = (DataFrameBase::class).qualifiedName
        val dfRowName = (DataRowBase::class).qualifiedName
        val dataCol = (DataColumn::class).qualifiedName!!
        val marker = ReplCodeGeneratorImpl.markerInterfacePrefix
        val markerFull = Test1._DataFrameType::class.qualifiedName!!

        val expected = """
            @DataSchema(isOpen = false)
            interface $marker
            val $dfName<$marker>.age: $dataCol<kotlin.Int> @JvmName("${marker}_age") get() = this["age"] as $dataCol<kotlin.Int>
            val $dfRowName<$marker>.age: kotlin.Int @JvmName("${marker}_age") get() = this["age"] as kotlin.Int
            val $dfName<$marker>.city: $dataCol<kotlin.String?> @JvmName("${marker}_city") get() = this["city"] as $dataCol<kotlin.String?>
            val $dfRowName<$marker>.city: kotlin.String? @JvmName("${marker}_city") get() = this["city"] as kotlin.String?
            val $dfName<$marker>.name: $dataCol<kotlin.String> @JvmName("${marker}_name") get() = this["name"] as $dataCol<kotlin.String>
            val $dfRowName<$marker>.name: kotlin.String @JvmName("${marker}_name") get() = this["name"] as kotlin.String
            val $dfName<$marker>.weight: $dataCol<kotlin.Int?> @JvmName("${marker}_weight") get() = this["weight"] as $dataCol<kotlin.Int?>
            val $dfRowName<$marker>.weight: kotlin.Int? @JvmName("${marker}_weight") get() = this["weight"] as kotlin.Int?
        """.trimIndent()
        code shouldBe expected

        val code2 = repl.process<Test1._DataFrameType>()
        code2 shouldBe ""

        val df3 = typed.filter { city != null }
        val code3 = repl.process(df3).declarations
        val marker3 = marker + "1"
        val expected3 = """
            @DataSchema(isOpen = false)
            interface $marker3 : $markerFull
            val $dfName<$marker3>.city: $dataCol<kotlin.String> @JvmName("${marker3}_city") get() = this["city"] as $dataCol<kotlin.String>
            val $dfRowName<$marker3>.city: kotlin.String @JvmName("${marker3}_city") get() = this["city"] as kotlin.String
        """.trimIndent()

        code3 shouldBe expected3

        val code4 = repl.process<Test1._DataFrameType1>()
        code4 shouldBe ""

        val df5 = typed.filter { weight != null }
        val code5 = repl.process(df5).declarations
        val marker5 = marker + "2"
        val expected5 = """
            @DataSchema(isOpen = false)
            interface $marker5 : $markerFull
            val $dfName<$marker5>.weight: $dataCol<kotlin.Int> @JvmName("${marker5}_weight") get() = this["weight"] as $dataCol<kotlin.Int>
            val $dfRowName<$marker5>.weight: kotlin.Int @JvmName("${marker5}_weight") get() = this["weight"] as kotlin.Int
        """.trimIndent()
        code5 shouldBe expected5

        val code6 = repl.process<Test1._DataFrameType2>()
        code6 shouldBe ""
    }

    @Test
    fun `process markers union`() {
        val repl = ReplCodeGenerator.create()
        repl.process(typed.select { age and name })
        repl.process<Test2._DataFrameType>() shouldBe ""
        repl.process(typed.select { city and weight })
        repl.process<Test2._DataFrameType1>() shouldBe ""

        val expected = """
            @DataSchema(isOpen = false)
            interface ${Test2._DataFrameType2::class.simpleName!!} : ${Test2._DataFrameType::class.qualifiedName}, ${Test2._DataFrameType1::class.qualifiedName}
        """.trimIndent()

        val code = repl.process(typed).declarations.trimIndent()
        code shouldBe expected
    }

    @Test
    fun `process wrong marker inheritance`() {
        val repl = ReplCodeGenerator.create()
        repl.process(typed.select { age and name })
        repl.process<Test2._DataFrameType>() shouldBe ""
        repl.process(typed.select { city and weight })
        repl.process<Test1._DataFrameType1>() shouldBe "" // processed wrong marker (doesn't implement Test2.DataFrameType)

        val dfName = (DataFrameBase::class).qualifiedName
        val dfRowName = (DataRowBase::class).qualifiedName
        val dataCol = (DataColumn::class).qualifiedName!!
        val marker = Test2._DataFrameType2::class.simpleName!!
        val expected = """
            @DataSchema(isOpen = false)
            interface $marker : ${Test2._DataFrameType::class.qualifiedName}
            val $dfName<$marker>.city: $dataCol<kotlin.String?> @JvmName("${marker}_city") get() = this["city"] as $dataCol<kotlin.String?>
            val $dfRowName<$marker>.city: kotlin.String? @JvmName("${marker}_city") get() = this["city"] as kotlin.String?
            val $dfName<$marker>.weight: $dataCol<kotlin.Int?> @JvmName("${marker}_weight") get() = this["weight"] as $dataCol<kotlin.Int?>
            val $dfRowName<$marker>.weight: kotlin.Int? @JvmName("${marker}_weight") get() = this["weight"] as kotlin.Int?
        """.trimIndent()

        val code = repl.process(typed).declarations.trimIndent()
        code shouldBe expected
    }
}
