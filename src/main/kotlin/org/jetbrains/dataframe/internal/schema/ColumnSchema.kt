package org.jetbrains.dataframe.internal.schema

import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.columns.*
import org.jetbrains.dataframe.impl.columns.internal
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.isSupertypeOf

internal abstract class ColumnSchema {

    abstract val kind: ColumnKind

    abstract val nullable: Boolean

    class Value(val type: KType) : ColumnSchema() {
        override val kind = ColumnKind.Value
        override val nullable = type.isMarkedNullable

        fun compare(other: Value): CompareResult = when {
            type == other.type -> CompareResult.Equals
            type.isSubtypeOf(other.type) -> CompareResult.IsDerived
            type.isSupertypeOf(other.type) -> CompareResult.IsSuper
            else -> CompareResult.None
        }
    }

    class Map(val schema: DataFrameSchema) : ColumnSchema() {
        override val kind = ColumnKind.Group
        override val nullable = false

        fun compare(other: Map): CompareResult = schema.compare(other.schema)
    }

    class Frame(val schema: DataFrameSchema, override val nullable: Boolean) : ColumnSchema() {
        override val kind = ColumnKind.Frame

        fun compare(other: Frame): CompareResult =
            schema.compare(other.schema).combine(CompareResult.compareNullability(nullable, other.nullable))
    }

    override fun equals(other: Any?): Boolean {
        val otherType = other as? ColumnSchema ?: return false
        if (otherType.kind != kind) return false
        if (otherType.nullable != nullable) return false
        when (this) {
            is Value -> return type == (otherType as Value).type
            is Map -> return schema == (otherType as Map).schema
            is Frame -> return schema == (otherType as Frame).schema
            else -> throw NotImplementedError()
        }
    }

    fun compare(other: ColumnSchema): CompareResult {
        if (kind != other.kind) return CompareResult.None
        if (this === other) return CompareResult.Equals
        return when (this) {
            is Value -> compare(other as Value)
            is Map -> compare(other as Map)
            is Frame -> compare(other as Frame)
            else -> throw NotImplementedError()
        }
    }
}

internal fun AnyCol.getColumnType(): ColumnSchema = when (this) {
    is ValueColumn<*> -> ColumnSchema.Value(type)
    is ColumnGroup<*> -> ColumnSchema.Map(df.extractSchema())
    is FrameColumn<*> -> ColumnSchema.Frame(internal().schema.value, hasNulls)
    else -> throw RuntimeException()
}
