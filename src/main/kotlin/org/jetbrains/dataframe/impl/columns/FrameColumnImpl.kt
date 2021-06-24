package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.internal.schema.DataFrameSchema
import org.jetbrains.dataframe.internal.schema.extractSchema
import org.jetbrains.dataframe.internal.schema.intersectSchemas
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

internal open class FrameColumnImpl<T> constructor(
    name: String,
    values: List<DataFrame<T>?>,
    hasNulls: Boolean?,
    columnSchema: Lazy<DataFrameSchema>? = null,
    distinct: Lazy<Set<DataFrame<T>?>>? = null
) :
    DataColumnImpl<DataFrame<T>?>(
        values,
        name,
        createTypeWithArgument<AnyFrame>()
            .withNullability(hasNulls ?: values.any { it == null }),
        distinct
    ),
    FrameColumnInternal<T> {

    constructor(name: String, df: DataFrame<T>, startIndices: Sequence<Int>, emptyToNull: Boolean) : this(name, df.splitByIndices(startIndices, emptyToNull).toList(), hasNulls = if (emptyToNull) null else false)

    override fun rename(newName: String) = FrameColumnImpl(newName, values, hasNulls, schema, distinct)

    override fun defaultValue() = null

    override fun addParent(parent: ColumnGroup<*>) = FrameColumnWithParent(parent, this)

    override fun createWithValues(values: List<DataFrame<T>?>, hasNulls: Boolean?): DataColumn<DataFrame<T>?> {
        return DataColumn.create(name, values, hasNulls)
    }

    override fun changeType(type: KType) = throw UnsupportedOperationException()

    override fun distinct() = FrameColumnImpl(name, distinct.value.toList(), hasNulls, schema, distinct)

    override val schema = columnSchema ?: lazy {
        values.mapNotNull { it?.takeIf { it.nrow > 0 }?.extractSchema() }.intersectSchemas()
    }

    override fun forceResolve() = ResolvingFrameColumn(name, values, hasNulls, schema, distinct)
}

internal class ResolvingFrameColumn<T>(
    name: String,
    values: List<DataFrame<T>?>,
    hasNulls: Boolean,
    columnSchema: Lazy<DataFrameSchema>,
    distinct: Lazy<Set<DataFrame<T>?>>
) :
    FrameColumnImpl<T>(name, values, hasNulls, columnSchema, distinct) {

    override fun resolveSingle(context: ColumnResolutionContext) = context.df.getColumn<DataFrame<T>>(name, context.unresolvedColumnsPolicy)?.addPath(context.df)
}
