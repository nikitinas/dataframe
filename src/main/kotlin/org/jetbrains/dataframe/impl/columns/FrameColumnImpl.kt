package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.createType
import java.lang.Exception
import java.lang.UnsupportedOperationException
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

internal class FrameColumnImpl<T> constructor(override val df: DataFrame<T>, name: String, values: List<DataFrame<T>?>)
    : DataColumnImpl<DataFrame<T>?>(values, name, createType<AnyFrame?>().withNullability(values.any { it == null })), FrameColumn<T> {

    constructor(name: String, df: DataFrame<T>, startIndices: Sequence<Int>) : this(df, name, df.splitByIndices(startIndices).toList())

    override fun rename(newName: String) = FrameColumnImpl(df, newName, values)

    override fun defaultValue() = null

    override fun addParent(parent: MapColumn<*>) = FrameColumnWithParent(parent, this)

    override fun createWithValues(values: List<DataFrame<T>?>, hasNulls: Boolean?): DataColumn<DataFrame<T>?> {
        return DataColumn.create(name, values, values.getBaseSchema())
    }

    override fun changeType(type: KType) = throw UnsupportedOperationException()

    override fun distinct(): FrameColumn<T> {
        return DataColumn.create(name, values.distinct())
    }
}