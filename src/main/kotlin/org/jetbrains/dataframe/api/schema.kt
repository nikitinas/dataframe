package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import org.jetbrains.dataframe.internal.schema.extractSchema

fun AnyFrame.schema(markerName: String? = null): String {
    return CodeGenerator.create().generate(
        extractSchema(),
        markerName ?: "DataRecord",
        fields = true,
        extensionProperties = false,
        isOpen = true
    ).code.declarations
}