package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.AnyCol
import org.jetbrains.dataframe.ColumnKind
import org.jetbrains.dataframe.ColumnPath
import org.jetbrains.dataframe.ColumnPosition
import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.ColumnsSelector
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataFrameBase
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.SelectReceiverImpl
import org.jetbrains.dataframe.SortColumnsSelector
import org.jetbrains.dataframe.SortReceiverImpl
import org.jetbrains.dataframe.UnresolvedColumnsPolicy
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnSet
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.ValueColumn
import org.jetbrains.dataframe.columns.definition
import org.jetbrains.dataframe.getType
import org.jetbrains.dataframe.impl.TreeNode
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.equalsByElement
import org.jetbrains.dataframe.impl.rollingHash
import org.jetbrains.dataframe.isGroup
import org.jetbrains.dataframe.name
import org.jetbrains.dataframe.toColumnDef
import org.jetbrains.dataframe.toColumns
import org.jetbrains.dataframe.typed
import kotlin.reflect.KProperty
import kotlin.reflect.full.isSubtypeOf

internal fun <T> DataColumn<T>.checkEquals(other: Any?): Boolean {
    if (this === other) return true

    if (!(other is DataColumn<*>)) return false

    if (name() != other.name()) return false
    if (type != other.type) return false
    return values.equalsByElement(other.values)
}

internal fun <T> DataColumn<T>.getHashCode(): Int {
    var result = values.rollingHash()
    result = 31 * result + name().hashCode()
    result = 31 * result + type.hashCode()
    return result
}

internal fun <C> TreeNode<ColumnPosition>.toColumnWithPath(df: DataFrameBase<*>) =
    (data.column as DataColumn<C>).addPath(pathFromRoot(), df)

@JvmName("toColumnWithPathAnyCol")
internal fun <C> TreeNode<DataColumn<C>>.toColumnWithPath(df: DataFrameBase<*>) = data.addPath(pathFromRoot(), df)

internal fun <T> DataColumn<T>.addPath(path: ColumnPath, df: DataFrameBase<*>): ColumnWithPath<T> =
    ColumnWithPathImpl(this, path, df)

internal fun <T> ColumnWithPath<T>.changePath(path: ColumnPath): ColumnWithPath<T> = data.addPath(path, df)

internal fun <T> DataColumn<T>.addParentPath(path: ColumnPath, df: DataFrameBase<*>) = addPath(path + name, df)

internal fun <T> DataColumn<T>.addPath(df: DataFrameBase<*>): ColumnWithPath<T> = addPath(listOf(name), df)

internal fun ColumnPath.depth() = size - 1

internal fun ColumnWithPath<*>.asGroup() = if (data.isGroup()) data.asGroup() else null
internal fun <T> AnyCol.typed() = this as DataColumn<T>
internal fun <T> ColumnWithPath<*>.typed() = this as ColumnWithPath<T>
internal fun <T> AnyCol.asValues() = this as ValueColumn<T>
internal fun <T> ValueColumn<*>.typed() = this as ValueColumn<T>
internal fun <T> FrameColumn<*>.typed() = this as FrameColumn<T>
internal fun <T> MapColumn<*>.typed() = this as MapColumn<T>
internal fun <T> AnyCol.grouped() = this as ColumnGroup<T>
internal fun <T> MapColumn<*>.withDf(newDf: DataFrame<T>) = DataColumn.create(name(), newDf)
internal fun AnyCol.asGroup(): MapColumn<*> = this as MapColumn<*>

@JvmName("asGroupedT")
internal fun <T> DataColumn<DataRow<T>>.asGroup(): MapColumn<T> = this as MapColumn<T>
internal fun AnyCol.asTable(): FrameColumnInternal<*> = this as FrameColumnInternal<*>

@JvmName("asTableT")
internal fun <T> DataColumn<DataFrame<T>?>.asTable(): FrameColumn<T> = this as FrameColumnInternal<T>
internal fun AnyCol.isTable(): Boolean = kind() == ColumnKind.Frame
internal fun <T> DataColumn<T>.assertIsComparable(): DataColumn<T> {
    if (!type.isSubtypeOf(getType<Comparable<*>?>()))
        throw RuntimeException("Column '$name' has type '$type' that is not Comparable")
    return this
}

internal fun <A, B> ColumnSet<A>.transform(transform: (List<ColumnWithPath<A>>) -> List<ColumnWithPath<B>>): ColumnSet<B> {

    class TransformedColumnSet<A, B>(
        val src: ColumnSet<A>,
        val transform: (List<ColumnWithPath<A>>) -> List<ColumnWithPath<B>>
    ) :
        ColumnSet<B> {

        override fun resolve(context: ColumnResolutionContext) = transform(src.resolve(context))
    }

    return TransformedColumnSet(this, transform)
}

internal fun Array<out String>.toColumns(): ColumnSet<Any?> = map { it.toColumnDef() }.toColumnSet()
internal fun <C> Iterable<ColumnSet<C>>.toColumnSet(): ColumnSet<C> = ColumnsList(asList())
internal fun <C> Array<out KProperty<C>>.toColumns() = map { it.toColumnDef() }.toColumnSet()
internal fun <T> Array<out ColumnReference<T>>.toColumns() = map { it.definition() }.toColumnSet()
internal fun <T, C> ColumnsSelector<T, C>.toColumns(): ColumnSet<C> = toColumns {
    SelectReceiverImpl(
        it.df.typed(),
        it.allowMissingColumns
    )
}

@JvmName("toColumnSetForSort")
internal fun <T, C> SortColumnsSelector<T, C>.toColumns(): ColumnSet<C> = toColumns {
    SortReceiverImpl(
        it.df.typed(),
        it.allowMissingColumns
    )
}

internal fun <C> DataFrameBase<*>.getColumn(name: String, policy: UnresolvedColumnsPolicy) =
    tryGetColumn(name)?.typed()
        ?: when (policy) {
            UnresolvedColumnsPolicy.Fail ->
                error("Column not found: $name")
            UnresolvedColumnsPolicy.Skip -> null
            UnresolvedColumnsPolicy.Create -> DataColumn.empty().typed<C>()
        }