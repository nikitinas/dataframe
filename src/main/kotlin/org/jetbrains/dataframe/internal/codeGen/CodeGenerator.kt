package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.dataframe.internal.codeGen.ClassMarkers
import org.jetbrains.dataframe.internal.codeGen.GeneratedCode
import org.jetbrains.dataframe.internal.codeGen.Marker
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import kotlin.reflect.KClass

enum class InterfaceGenerationMode {
    NoFields,
    WithFields,
    None
}

internal interface CodeGenerator {

    fun generate(
        schema: DataFrameSchema,
        name: String,
        fields: Boolean,
        extensionProperties: Boolean,
        isOpen: Boolean,
        knownMarkers: Iterable<Marker> = emptyList()
    ): Pair<GeneratedCode, List<Marker>>

    fun generate(marker: Marker, interfaceMode: InterfaceGenerationMode, extensionProperties: Boolean): GeneratedCode

    companion object {
        fun create(): CodeGenerator = CodeGeneratorImpl()
    }
}

internal fun CodeGenerator.generate(markerClass: KClass<*>, interfaceMode: InterfaceGenerationMode, extensionProperties: Boolean) = generate(
    ClassMarkers.get(markerClass), interfaceMode, extensionProperties)

internal inline fun <reified T> CodeGenerator.generate(interfaceMode: InterfaceGenerationMode, extensionProperties: Boolean) = generate(T::class, interfaceMode, extensionProperties)
