package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.internal.codeGen.GeneratedCode
import org.jetbrains.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.dataframe.stubs.DataFrameToListTypedStub
import org.jetbrains.kotlinx.jupyter.api.Code
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

internal enum class ReplCodeGenerationMode {
    EmptyInterfaceWithExtensionProperties,
    InterfaceWithFields
}

internal interface ReplCodeGenerator {

    fun process(df: AnyFrame, property: KProperty<*>? = null): GeneratedCode?

    fun process(marker: KClass<*>): Code

    fun process(stub: DataFrameToListNamedStub): GeneratedCode

    fun process(stub: DataFrameToListTypedStub): GeneratedCode

    var generationMode: ReplCodeGenerationMode

    companion object {
        fun create(): ReplCodeGenerator = ReplCodeGeneratorImpl()
    }
}

internal inline fun <reified T> ReplCodeGenerator.process(): Code = process(T::class)