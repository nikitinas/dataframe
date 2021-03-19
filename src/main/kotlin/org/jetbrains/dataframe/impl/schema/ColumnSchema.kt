package org.jetbrains.dataframe.impl.schema

import org.jetbrains.dataframe.AnyCol
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.ValueColumn
import org.jetbrains.dataframe.impl.columns.internal
import kotlin.reflect.KType

internal abstract class ColumnSchema {

    abstract val kind: ColumnKind

    abstract val nullable: Boolean

    class Value(val type: KType) : ColumnSchema() {
        override val kind = ColumnKind.Value
        override val nullable = type.isMarkedNullable
    }

    class Map(val schema: DataFrameSchema) : ColumnSchema() {
        override val kind = ColumnKind.Map
        override val nullable = false
    }

    class Frame(val schema: DataFrameSchema, override val nullable: Boolean) : ColumnSchema() {
        override val kind = ColumnKind.Frame
    }

    override fun equals(other: Any?): Boolean {
        val otherType = other as? ColumnSchema ?: return false
        if(otherType.kind != kind) return false
        if(otherType.nullable != nullable) return false
        when(this){
            is Value -> return type == (otherType as Value).type
            is Map -> return schema == (otherType as Map).schema
            is Frame -> return schema == (otherType as Frame).schema
            else -> throw NotImplementedError()
        }
    }
}

internal fun AnyCol.getColumnType(): ColumnSchema = when (this) {
    is ValueColumn<*> -> ColumnSchema.Value(type)
    is MapColumn<*> -> ColumnSchema.Map(df.extractSchema())
    is FrameColumn<*> -> ColumnSchema.Frame(internal().schema, hasNulls)
    else -> throw RuntimeException()
}