package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ValueColumn
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

internal open class ValueColumnImpl<T>(values: List<T>, name: String, type: KType, val defaultValue: T? = null, distinct: Lazy<Set<T>>? = null)
    : DataColumnImpl<T>(values, name, type, distinct), ValueColumn<T> {

    override fun distinct() = ValueColumnImpl(toSet().toList(), name, type, defaultValue, distinct)

    override fun rename(newName: String) = ValueColumnImpl(values, newName, type, defaultValue, distinct)

    override fun changeType(type: KType) = ValueColumnImpl(values, name, type, defaultValue, distinct)

    override fun addParent(parent: ColumnGroup<*>): DataColumn<T> = ValueColumnWithParent(parent, this)

    override fun createWithValues(values: List<T>, hasNulls: Boolean?): DataColumn<T> {
        val nulls = hasNulls ?: values.any { it == null}
        return DataColumn.create(name, values, type.withNullability(nulls))
    }

    override fun defaultValue() = defaultValue

    override fun forceResolve() = ResolvingValueColumn(values, name, type, defaultValue, distinct)
}

internal class ResolvingValueColumn<T>(values: List<T>, name: String, type: KType, defaultValue: T? = null, distinct: Lazy<Set<T>>? = null): ValueColumnImpl<T>(values, name, type, defaultValue, distinct){

    override fun resolveSingle(context: ColumnResolutionContext) = context.df.getColumn<T>(name, context.unresolvedColumnsPolicy)?.addPath(context.df)
}