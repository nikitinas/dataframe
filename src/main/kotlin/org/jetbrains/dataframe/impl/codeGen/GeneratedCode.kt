package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.kotlinx.jupyter.api.Code
import org.jetbrains.kotlinx.jupyter.api.VariableName

data class GeneratedCode(val declarations: Code, val converter: (VariableName) -> Code) {

    fun with(name: VariableName): Code = declarations + "\n" + converter(name)
}