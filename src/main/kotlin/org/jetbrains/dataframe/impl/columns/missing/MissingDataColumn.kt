package org.jetbrains.dataframe.impl.columns.missing

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.columns.DataColumnInternal
import kotlin.reflect.KType

internal abstract class MissingDataColumn<T> : DataColumnInternal<T> {

    val name: String
        get() = throw UnsupportedOperationException()

    override fun values() = throw UnsupportedOperationException()

    override fun type() = throw UnsupportedOperationException()

    override fun ndistinct() = throw UnsupportedOperationException()

    override fun size() = throw UnsupportedOperationException()

    override fun name() = name

    override fun get(index: Int) = throw UnsupportedOperationException()

    override fun defaultValue() = throw UnsupportedOperationException()

    override fun slice(range: IntRange) = throw UnsupportedOperationException()

    override fun get(columnName: String) = throw UnsupportedOperationException()

    override fun slice(indices: Iterable<Int>) = throw UnsupportedOperationException()

    override fun slice(mask: BooleanArray) = throw UnsupportedOperationException()

    override fun toSet() = throw UnsupportedOperationException()

    override fun resolve(context: ColumnResolutionContext) = emptyList<ColumnWithPath<T>>()

    override fun changeType(type: KType) = throw UnsupportedOperationException()

    override fun rename(newName: String) = throw UnsupportedOperationException()

    override fun addParent(parent: ColumnGroup<*>) = throw UnsupportedOperationException()

    override fun forceResolve() = throw UnsupportedOperationException()
}
