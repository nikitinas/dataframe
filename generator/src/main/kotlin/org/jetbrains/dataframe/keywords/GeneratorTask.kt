package org.jetbrains.dataframe.keywords

import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.jetbrains.kotlin.com.intellij.psi.tree.TokenSet
import org.jetbrains.kotlin.lexer.KtKeywordToken
import org.jetbrains.kotlin.lexer.KtTokens
import java.io.File

open class GeneratorTask : DefaultTask() {

    @OutputDirectory
    lateinit var srcDir: File

    private val taskPackageName = "org.jetbrains.dataframe.keywords"

    override fun getGroup() = "codegen"

    @TaskAction
    fun generate() {
        srcDir.deleteRecursively()
        generateKeywordEnums()
    }

    private fun generateKeywordEnums() {
        listOf(
            "HardKeywords" to KtTokens.KEYWORDS,
            "SoftKeywords" to KtTokens.SOFT_KEYWORDS,
            "ModifierKeywords" to KtTokens.MODIFIER_KEYWORDS
        ).forEach { (name, set) ->
            generateKeywordsEnum(name, set)
        }
    }

    private fun generateKeywordsEnum(name: String, tokenSet: TokenSet) {
        buildKwEnum(name, getKeywords(tokenSet)).writeTo(srcDir)
    }

    private fun getKeywords(tokenSet: TokenSet): List<EnumEntry> {
        fun id(value: String) = value.toUpperCase().replace("!", "NOT_")

        return tokenSet.types.map { t ->
            t as KtKeywordToken
            EnumEntry(id(t.value), t.value)
        }
    }

    private fun buildKwEnum(name: String, values: List<EnumEntry>): FileSpec {
        val fileBuilder = FileSpec.builder(taskPackageName, name)
        val valList = mutableListOf<String>()

        val enumBuilder = TypeSpec.enumBuilder(name).apply {
            primaryConstructor(
                FunSpec.constructorBuilder()
                    .addParameter("value", String::class)
                    .build()
            )

            values.forEach { entry ->
                valList.add("\"${entry.strValue}\"")
                addEnumConstant(
                    entry.name, TypeSpec.anonymousClassBuilder()
                        .addSuperclassConstructorParameter("%S", entry.strValue)
                        .build()
                )
            }

            val compObj = TypeSpec.companionObjectBuilder().addProperty(
                PropertySpec
                    .builder("VALUES", List::class.parameterizedBy(String::class))
                    .initializer(valList.joinToString(", ", "listOf(", ")"))
                    .build()
            ).build()

            addType(compObj)
        }

        fileBuilder.addType(enumBuilder.build())

        return fileBuilder.build()
    }

    companion object {
        const val NAME = "generateKeywordsSrc"
    }
}
