package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub
import org.jetbrains.kotlinx.jupyter.api.Code
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

internal interface CodeGenerator {

    fun generate(df: AnyFrame, property: KProperty<*>? = null): GeneratedCode?
    fun generate(stub: DataFrameToListNamedStub): GeneratedCode
    fun generate(stub: DataFrameToListTypedStub): GeneratedCode

    fun generateExtensionProperties(marker: KClass<*>): Code?

    var mode: CodeGenerationMode

    companion object {
        fun create(): CodeGenerator = CodeGeneratorImpl()
    }
}