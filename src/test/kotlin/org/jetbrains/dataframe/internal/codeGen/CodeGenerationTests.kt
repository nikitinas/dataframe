package org.jetbrains.dataframe.internal.codeGen

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.ColumnGroup
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

    val personClassName = Person::class.qualifiedName!!

    val personShortName = Person::class.simpleName!!

    val dfName = (DataFrameBase::class).qualifiedName
    val dfRowName = (DataRowBase::class).qualifiedName
    val dataCol = (DataColumn::class).qualifiedName!!
    val dataRow = (DataRow::class).qualifiedName!!
    val colGroup = (ColumnGroup::class).qualifiedName!!

    fun expectedProperties(fullTypeName: String, shortTypeName: String) = """
            val $dfName<$fullTypeName>.age: $dataCol<kotlin.Int> @JvmName("${shortTypeName}_age") get() = this["age"] as $dataCol<kotlin.Int>
            val $dfRowName<$fullTypeName>.age: kotlin.Int @JvmName("${shortTypeName}_age") get() = this["age"] as kotlin.Int
            val $dfName<$fullTypeName>.city: $dataCol<kotlin.String?> @JvmName("${shortTypeName}_city") get() = this["city"] as $dataCol<kotlin.String?>
            val $dfRowName<$fullTypeName>.city: kotlin.String? @JvmName("${shortTypeName}_city") get() = this["city"] as kotlin.String?
            val $dfName<$fullTypeName>.name: $dataCol<kotlin.String> @JvmName("${shortTypeName}_name") get() = this["name"] as $dataCol<kotlin.String>
            val $dfRowName<$fullTypeName>.name: kotlin.String @JvmName("${shortTypeName}_name") get() = this["name"] as kotlin.String
            val $dfName<$fullTypeName>.weight: $dataCol<kotlin.Int?> @JvmName("${shortTypeName}_weight") get() = this["weight"] as $dataCol<kotlin.Int?>
            val $dfRowName<$fullTypeName>.weight: kotlin.Int? @JvmName("${shortTypeName}_weight") get() = this["weight"] as kotlin.Int?
    """.trimIndent()

    @Test
    fun `generate marker interface`() {
        val property = DataFrameTests::class.memberProperties.first { it.name == "df" }
        val generated = ReplCodeGenerator.create().process(df, property)
        val typeName = "DataFrameType"
        val expectedDeclaration = """
            @DataSchema(isOpen = false)
            interface $typeName
            """.trimIndent() + "\n" + expectedProperties(typeName, typeName)

        val expectedConverter = "it.typed<$typeName>()"

        generated.declarations shouldBe expectedDeclaration
        generated.converter("it") shouldBe expectedConverter
    }

    @Test
    fun `generate marker interface for nested data frame`() {
        val property = DataFrameTests::class.memberProperties.first { it.name == "df" }
        val grouped = df.move { name and city }.under("nameAndCity")
        val generated = ReplCodeGenerator.create().process(grouped, property)
        val type1 = "DataFrameType1"
        val type2 = "DataFrameType"
        val declaration1 = """
            @DataSchema(isOpen = false)
            interface $type1
            val $dfName<$type1>.city: $dataCol<kotlin.String?> @JvmName("${type1}_city") get() = this["city"] as $dataCol<kotlin.String?>
            val $dfRowName<$type1>.city: kotlin.String? @JvmName("${type1}_city") get() = this["city"] as kotlin.String?
            val $dfName<$type1>.name: $dataCol<kotlin.String> @JvmName("${type1}_name") get() = this["name"] as $dataCol<kotlin.String>
            val $dfRowName<$type1>.name: kotlin.String @JvmName("${type1}_name") get() = this["name"] as kotlin.String
            """.trimIndent()

        val declaration2 = """
            @DataSchema(isOpen = false)
            interface DataFrameType
            val $dfName<$type2>.age: $dataCol<kotlin.Int> @JvmName("${type2}_age") get() = this["age"] as $dataCol<kotlin.Int>
            val $dfRowName<$type2>.age: kotlin.Int @JvmName("${type2}_age") get() = this["age"] as kotlin.Int
            val $dfName<$type2>.nameAndCity: $colGroup<$type1> @JvmName("${type2}_nameAndCity") get() = this["nameAndCity"] as $colGroup<$type1>
            val $dfRowName<$type2>.nameAndCity: $dataRow<$type1> @JvmName("${type2}_nameAndCity") get() = this["nameAndCity"] as $dataRow<$type1>
            val $dfName<$type2>.weight: $dataCol<kotlin.Int?> @JvmName("${type2}_weight") get() = this["weight"] as $dataCol<kotlin.Int?>
            val $dfRowName<$type2>.weight: kotlin.Int? @JvmName("${type2}_weight") get() = this["weight"] as kotlin.Int?
            """.trimIndent()

        val expectedConverter = "it.typed<DataFrameType>()"

        generated.declarations shouldBe declaration1 + "\n" + declaration2
        generated.converter("it") shouldBe expectedConverter
    }

    @Test
    fun `generate extension properties`() {

        val personClass = (Person::class).qualifiedName!!
        val expected = """
            @DataSchema
            interface $personClass
        """.trimIndent() + "\n" + expectedProperties(personClassName, personShortName)

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
        val code = codeGen.generate(schema, "ValidPerson", true, true, isOpen = true, listOf(MarkersExtractor.get<Person>())).code.declarations
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
        code shouldBe expected
    }

    @Test
    fun `empty interface with properties`(){
        val codeGen = CodeGenerator.create()
        val code = codeGen.generate(df.extractSchema(), "Person", false, true, true).code.declarations
        val expected = """
            @DataSchema
            interface Person
            """.trimIndent() + "\n" + expectedProperties("Person", "Person")
        code shouldBe expected
    }

    @Test
    fun `interface with fields`(){
        val repl = CodeGenerator.create()
        val code = repl.generate(typed.extractSchema(), "DataType", true, false, false).code.declarations
        code shouldBe """
            @DataSchema(isOpen = false)
            interface DataType{
                val age: kotlin.Int
                val city: kotlin.String?
                val name: kotlin.String
                val weight: kotlin.Int?
            }
        """.trimIndent()
    }
}