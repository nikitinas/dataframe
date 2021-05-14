package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ValueColumn
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.columns.values
import org.jetbrains.dataframe.impl.renderType

fun AnyFrame.schema(): String {
    val sb = StringBuilder()
    val indentSequence = "    "
    fun print(indent: Int, df: DataFrameBase<*>){
        df.columns().forEach {
            sb.append(indentSequence.repeat(indent))
            sb.append(it.name + ":")
            when(it) {
                is ColumnGroup<*> -> {
                    sb.appendLine()
                    print(indent + 1, it.df)
                }
                is FrameColumn<*> -> {
                    sb.appendLine(" *")
                    val child = it.values.firstOrNull { it != null }
                    if(child != null)
                        print(indent + 1, child)
                }
                is ValueColumn<*> -> {
                    sb.appendLine(" ${renderType(it.type)}")
                }
            }
        }
    }
    print(0, this)

    return sb.toString()
}

fun AnyRow.schema() = owner.schema()