package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.internal.schema.ColumnSchema
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.columns.type
import kotlin.reflect.KType

internal fun String.truncate(limit: Int) = if (limit in 1 until length) {
    if (limit < 4) substring(0, limit)
    else substring(0, limit - 3) + "..."
} else {
    this
}

internal fun renderSchema(df: AnyFrame): String =
        df.columns().map { "${it.name()}:${renderType(it)}"}.joinToString()

internal fun renderSchema(schema: DataFrameSchema): String =
    schema.columns.map { "${it.key}:${renderType(it.value)}"}.joinToString()

internal fun renderType(column: ColumnSchema) =
    when(column) {
        is ColumnSchema.Value -> {
            val type = column.type
            val result = type.toString()
            if (result.startsWith("kotlin.")) result.substring(7)
            else result
        }
        is ColumnSchema.Frame -> {
            "[${renderSchema(column.schema)}]"
        }
        is ColumnSchema.Map -> {
            "{${renderSchema(column.schema)}}"
        }
        else -> throw NotImplementedError()
    }

internal fun renderType(type: KType): String{
    val result = type.toString()
    return if (result.startsWith("kotlin.")) result.substring(7)
    else result
}

internal fun renderType(column: AnyCol) =
    when(column.kind()) {
        ColumnKind.Value -> renderType(column.type)
        ColumnKind.Frame -> {
            val table = column.asTable()
            "[${renderSchema(table.schema.value)}]"
        }
        ColumnKind.Map -> {
            val group = column.asGroup()
            "{${renderSchema(group.df)}}"
        }
    }


