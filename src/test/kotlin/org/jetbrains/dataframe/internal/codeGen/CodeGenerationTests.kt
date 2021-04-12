package org.jetbrains.dataframe.internal.codeGen

import io.kotlintest.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import org.jetbrains.dataframe.impl.codeGen.InterfaceGenerationMode
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.dataframe.impl.codeGen.generate
import org.jetbrains.dataframe.internal.schema.extractSchema
import org.jetbrains.dataframe.person.BaseTest
import org.jetbrains.dataframe.person.DataFrameTests
import org.junit.Test
import kotlin.reflect.full.memberProperties

class CodeGenerationTests : BaseTest(){

    val personClassName = Person::class.qualifiedName

    val personShortName = Person::class.simpleName!!

    @Test
    fun `generate marker interface`() {
        val property = DataFrameTests::class.memberProperties.first { it.name == "df" }
        val generated = ReplCodeGenerator.create().process(df, property)!!
        val expectedDeclaration = """
            @DataSchema(isOpen = false)
            interface DataFrameType{
                val age: kotlin.Int
                val city: kotlin.String?
                val name: kotlin.String
                val weight: kotlin.Int?
            }""".trimIndent()

        val expectedConverter = "it.typed<DataFrameType>()"

        generated.declarations shouldBe expectedDeclaration
        generated.converter("it") shouldBe expectedConverter
    }

    @Test
    fun `generate marker interface for nested data frame`() {
        val property = DataFrameTests::class.memberProperties.first { it.name == "df" }
        val grouped = df.move { name and city }.under("nameAndCity")
        val generated = ReplCodeGenerator.create().process(grouped, property)!!
        val rowType = DataRow::class.qualifiedName
        val declaration1 = """
            @DataSchema(isOpen = false)
            interface DataFrameType1{
                val city: kotlin.String?
                val name: kotlin.String
            }""".trimIndent()

        val declaration2 = """
            @DataSchema(isOpen = false)
            interface DataFrameType{
                val age: kotlin.Int
                val nameAndCity: $rowType<DataFrameType1>
                val weight: kotlin.Int?
            }""".trimIndent()

        val expectedConverter = "it.typed<DataFrameType>()"

        generated.declarations shouldBe declaration1 + "\n" + declaration2
        generated.converter("it") shouldBe expectedConverter
    }

    @Test
    fun `generate extension properties`() {

        val dfName = (DataFrameBase::class).qualifiedName
        val dfRowName = (DataRowBase::class).qualifiedName
        val dataCol = (DataColumn::class).qualifiedName!!
        val personClass = (Person::class).qualifiedName!!
        val expected = """
            @DataSchema
            interface $personClass
            val $dfName<$personClassName>.age: $dataCol<kotlin.Int> @JvmName("${personShortName}_age") get() = this["age"] as $dataCol<kotlin.Int>
            val $dfRowName<$personClassName>.age: kotlin.Int @JvmName("${personShortName}_age") get() = this["age"] as kotlin.Int
            val $dfName<$personClassName>.city: $dataCol<kotlin.String?> @JvmName("${personShortName}_city") get() = this["city"] as $dataCol<kotlin.String?>
            val $dfRowName<$personClassName>.city: kotlin.String? @JvmName("${personShortName}_city") get() = this["city"] as kotlin.String?
            val $dfName<$personClassName>.name: $dataCol<kotlin.String> @JvmName("${personShortName}_name") get() = this["name"] as $dataCol<kotlin.String>
            val $dfRowName<$personClassName>.name: kotlin.String @JvmName("${personShortName}_name") get() = this["name"] as kotlin.String
            val $dfName<$personClassName>.weight: $dataCol<kotlin.Int?> @JvmName("${personShortName}_weight") get() = this["weight"] as $dataCol<kotlin.Int?>
            val $dfRowName<$personClassName>.weight: kotlin.Int? @JvmName("${personShortName}_weight") get() = this["weight"] as kotlin.Int?
        """.trimIndent()

        val code = CodeGenerator.create().generate<Person>(InterfaceGenerationMode.NoFields, extensionProperties = true).declarations
        code shouldBe expected
    }

    @Test
    fun `frame to markers`(){
        val f = SchemaProcessor.create("Temp")
        val marker = f.process(df.extractSchema(), true)
        marker.isOpen shouldBe true
        f.generatedMarkers shouldBe listOf(marker)
    }

    @Test
    fun `generate derived interface`() {
        val codeGen = CodeGenerator.create()
        val schema = df.filterNotNull { all() }.extractSchema()
        val generated = codeGen.generate(schema, "ValidPerson", true, true, isOpen = true, listOf(ClassMarkers.get<Person>())).first
        val expected = """
            @DataSchema
            interface ValidPerson : $personClassName{
                override val city: kotlin.String
                override val weight: kotlin.Int
            }
            val org.jetbrains.dataframe.DataFrameBase<ValidPerson>.city: org.jetbrains.dataframe.columns.DataColumn<kotlin.String> @JvmName("ValidPerson_city") get() = this["city"] as org.jetbrains.dataframe.columns.DataColumn<kotlin.String>
            val org.jetbrains.dataframe.DataRowBase<ValidPerson>.city: kotlin.String @JvmName("ValidPerson_city") get() = this["city"] as kotlin.String
            val org.jetbrains.dataframe.DataFrameBase<ValidPerson>.weight: org.jetbrains.dataframe.columns.DataColumn<kotlin.Int> @JvmName("ValidPerson_weight") get() = this["weight"] as org.jetbrains.dataframe.columns.DataColumn<kotlin.Int>
            val org.jetbrains.dataframe.DataRowBase<ValidPerson>.weight: kotlin.Int @JvmName("ValidPerson_weight") get() = this["weight"] as kotlin.Int
        """.trimIndent()
        generated.declarations shouldBe expected
    }

    @Test
    fun `generate empty interface`(){
        val codeGen = CodeGenerator.create()
        val generated = codeGen.generate(df.extractSchema(), "Person", false, true, true).first
        val expected = """
            @DataSchema
            interface Person
            val org.jetbrains.dataframe.DataFrameBase<Person>.age: org.jetbrains.dataframe.columns.DataColumn<kotlin.Int> @JvmName("Person_age") get() = this["age"] as org.jetbrains.dataframe.columns.DataColumn<kotlin.Int>
            val org.jetbrains.dataframe.DataRowBase<Person>.age: kotlin.Int @JvmName("Person_age") get() = this["age"] as kotlin.Int
            val org.jetbrains.dataframe.DataFrameBase<Person>.city: org.jetbrains.dataframe.columns.DataColumn<kotlin.String?> @JvmName("Person_city") get() = this["city"] as org.jetbrains.dataframe.columns.DataColumn<kotlin.String?>
            val org.jetbrains.dataframe.DataRowBase<Person>.city: kotlin.String? @JvmName("Person_city") get() = this["city"] as kotlin.String?
            val org.jetbrains.dataframe.DataFrameBase<Person>.name: org.jetbrains.dataframe.columns.DataColumn<kotlin.String> @JvmName("Person_name") get() = this["name"] as org.jetbrains.dataframe.columns.DataColumn<kotlin.String>
            val org.jetbrains.dataframe.DataRowBase<Person>.name: kotlin.String @JvmName("Person_name") get() = this["name"] as kotlin.String
            val org.jetbrains.dataframe.DataFrameBase<Person>.weight: org.jetbrains.dataframe.columns.DataColumn<kotlin.Int?> @JvmName("Person_weight") get() = this["weight"] as org.jetbrains.dataframe.columns.DataColumn<kotlin.Int?>
            val org.jetbrains.dataframe.DataRowBase<Person>.weight: kotlin.Int? @JvmName("Person_weight") get() = this["weight"] as kotlin.Int?
        """.trimIndent()
        generated.declarations shouldBe expected
    }
}