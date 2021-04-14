package org.jetbrains.dataframe.internal.codeGen

import org.jetbrains.kotlinx.jupyter.api.Code
import org.jetbrains.kotlinx.jupyter.api.VariableName

data class CodeWithConverter(val declarations: Code, val converter: (VariableName) -> Code) {

    val hasDeclarations: Boolean get() = declarations.isNotBlank()

    val hasConverter: Boolean get() = converter("it").trim() != "it"

    fun with(name: VariableName): Code = declarations + "\n" + converter(name)
}