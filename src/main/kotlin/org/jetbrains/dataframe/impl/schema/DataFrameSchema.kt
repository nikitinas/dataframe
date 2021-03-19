package org.jetbrains.dataframe.impl.schema

import org.jetbrains.dataframe.AnyFrame
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.baseType
import org.jetbrains.dataframe.getType
import kotlin.reflect.full.withNullability

internal inline class DataFrameSchema(val columns: Map<String, ColumnSchema>)

internal fun AnyFrame.extractSchema() = DataFrameSchema(columns().map { it.name() to it.getColumnType() }.toMap())

internal fun Iterable<DataFrameSchema>.intersectSchemas(): DataFrameSchema {
    val collectedTypes = mutableMapOf<String, MutableSet<ColumnSchema>>()
    var first = true
    val toUpdate = mutableSetOf<String>()
    forEach { schema ->
        if (first) {
            schema.columns.forEach { collectedTypes.put(it.key, mutableSetOf(it.value)) }
            first = false
        }
        else {
            collectedTypes.forEach { entry ->
                val otherType = schema.columns[entry.key]
                if (otherType == null) {
                    toUpdate.add(entry.key)
                } else entry.value.add(otherType)
            }
            toUpdate.forEach { collectedTypes.remove(it) }
            toUpdate.clear()
        }
    }
    val result = collectedTypes.mapValues {
        val columnKinds = it.value.map { it.kind }.distinct()
        val kind = columnKinds.first()
        when {
            columnKinds.size > 1 -> ColumnSchema.Value(getType<Any>().withNullability(it.value.any { it.nullable }))
            kind == ColumnKind.Value -> ColumnSchema.Value(baseType(it.value.map { (it as ColumnSchema.Value).type }
                .toSet()))
            kind == ColumnKind.Frame -> ColumnSchema.Frame(
                it.value.map { (it as ColumnSchema.Frame).schema }.intersectSchemas(),
                it.value.any { it.nullable })
            kind == ColumnKind.Map -> ColumnSchema.Map(it.value.map { (it as ColumnSchema.Map).schema }.intersectSchemas())
            else -> throw RuntimeException()
        }
    }
    return DataFrameSchema(result)
}