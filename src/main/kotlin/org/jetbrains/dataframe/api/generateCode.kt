package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import org.jetbrains.dataframe.internal.schema.extractSchema

public inline fun <reified T> DataFrame<T>.generateCode(fields: Boolean = true, extensionProperties: Boolean = true): String {
    val name = if (T::class.isAbstract) {
        T::class.simpleName!!
    } else "DataEntry"
    return generateCode(name, fields, extensionProperties)
}

public fun <T> DataFrame<T>.generateCode(
    markerName: String,
    fields: Boolean = true,
    extensionProperties: Boolean = true
): String {
    val codeGen = CodeGenerator.create()
    return codeGen.generate(
        extractSchema(),
        markerName,
        fields = fields,
        extensionProperties = extensionProperties,
        isOpen = true,
    ).code.declarations
}

public inline fun <reified T> DataFrame<T>.generateInterfaces(): String = generateCode(
    fields = true,
    extensionProperties = false
)

public fun <T> DataFrame<T>.generateInterfaces(markerName: String): String = generateCode(
    markerName,
    fields = true,
    extensionProperties = false
)
