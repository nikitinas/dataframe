package org.jetbrains.dataframe.internal.codeGen

import org.jetbrains.kotlinx.jupyter.api.Code
import org.jetbrains.kotlinx.jupyter.api.VariableName

public data class CodeWithConverter(val declarations: Code, val converter: (VariableName) -> Code) {

    val hasDeclarations: Boolean get() = declarations.isNotBlank()

    val hasConverter: Boolean get() = converter("it").trim() != "it"

    public fun with(name: VariableName): Code {
        if (!hasConverter) return declarations
        if (!hasDeclarations) return converter(name)
        return declarations + "\n" + converter(name)
    }
}
