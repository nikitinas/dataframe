package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.createType
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.internal.schema.extractSchema
import org.jetbrains.dataframe.internal.schema.intersectSchemas
import java.lang.UnsupportedOperationException
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

internal class FrameColumnImpl<T> constructor(name: String, values: List<DataFrame<T>?>, columnSchema: Lazy<DataFrameSchema>? = null)
    : DataColumnImpl<DataFrame<T>?>(values, name, createType<AnyFrame>().withNullability(values.any { it == null })),
    FrameColumnInternal<T> {

    constructor(name: String, df: DataFrame<T>, startIndices: Sequence<Int>) : this(name, df.splitByIndices(startIndices).toList())

    override fun rename(newName: String) = FrameColumnImpl(newName, values, schema)

    override fun defaultValue() = null

    override fun addParent(parent: MapColumn<*>) = FrameColumnWithParent(parent, this)

    override fun createWithValues(values: List<DataFrame<T>?>, hasNulls: Boolean?): DataColumn<DataFrame<T>?> {
        return DataColumn.create(name, values)
    }

    override fun changeType(type: KType) = throw UnsupportedOperationException()

    override fun distinct(): FrameColumn<T> {
        return DataColumn.create(name, values.distinct())
    }

    override val schema = columnSchema ?: lazy {
        values.mapNotNull { it?.extractSchema() }.intersectSchemas()
    }
}