package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.Columns
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.DataFrameReceiver
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.columns.assertIsComparable
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.columns.typed
import kotlin.reflect.KProperty

interface SortReceiver<out T> : SelectReceiver<T> {

    val <C> Columns<C>.desc: Columns<C> get() = addFlag(SortFlag.Reversed)
    val String.desc: Columns<Comparable<*>?> get() = cast<Comparable<*>>().desc
    val <C> KProperty<C>.desc: Columns<C> get() = toColumnDef().desc

    val <C> Columns<C?>.nullsLast: Columns<C?> get() = addFlag(SortFlag.NullsLast)
    val String.nullsLast: Columns<Comparable<*>?> get() = cast<Comparable<*>>().nullsLast
    val <C> KProperty<C?>.nullsLast: Columns<C?> get() = toColumnDef().nullsLast
}

typealias SortColumnsSelector<T, C> = Selector<SortReceiver<T>, Columns<C>>

fun <T,C> DataFrame<T>.sortBy(selector: SortColumnsSelector<T, C>) = doSortBy(selector, UnresolvedColumnsPolicy.Fail)
fun <T> DataFrame<T>.sortBy(cols: Iterable<ColumnReference<Comparable<*>?>>) = sortBy { cols.toColumnSet() }
fun <T> DataFrame<T>.sortBy(vararg cols: ColumnReference<Comparable<*>?>) = sortBy { cols.toColumns() }
fun <T> DataFrame<T>.sortBy(vararg cols: String) = sortBy { cols.toColumns() }
fun <T> DataFrame<T>.sortBy(vararg cols: KProperty<Comparable<*>?>) = sortBy { cols.toColumns() }

fun <T,C> DataFrame<T>.sortByDesc(selector: SortColumnsSelector<T, C>): DataFrame<T> {
    val set = selector.toColumns()
    return doSortBy({ set.desc })
}

fun <T> DataFrame<T>.sortByDesc(vararg columns: KProperty<Comparable<*>?>) = sortByDesc { columns.toColumns() }
fun <T> DataFrame<T>.sortByDesc(vararg columns: String) = sortByDesc { columns.toColumns() }
fun <T> DataFrame<T>.sortByDesc(vararg columns: ColumnReference<Comparable<*>?>) = sortByDesc { columns.toColumns() }
fun <T> DataFrame<T>.sortByDesc(columns: Iterable<ColumnReference<Comparable<*>?>>) = sortByDesc { columns.toColumnSet() }

fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: String) = sortBy { cols.toColumns() }
fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: ColumnReference<Comparable<*>?>) = sortBy { cols.toColumns() }
fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: KProperty<Comparable<*>?>) = sortBy { cols.toColumns() }
fun <T, G, C> GroupedDataFrame<T, G>.sortBy(selector: SortColumnsSelector<G, C>) = doSortBy(selector)

internal fun <T, C> DataFrame<T>.doSortBy(selector: SortColumnsSelector<T, C>, unresolvedColumnsPolicy: UnresolvedColumnsPolicy = UnresolvedColumnsPolicy.Fail): DataFrame<T> {

    val columns = extractSortColumns(selector, unresolvedColumnsPolicy)

    val compChain = columns.map {
        when (it.direction) {
            SortDirection.Asc -> it.column.createComparator(it.nullsLast)
            SortDirection.Desc -> it.column.createComparator(it.nullsLast).reversed()
        }
    }.reduce { a, b -> a.then(b) }

    val permutation = (0 until nrow()).sortedWith(compChain)

    return this[permutation]
}

internal fun AnyCol.createComparator(nullsLast: Boolean): java.util.Comparator<Int> {

    assertIsComparable()

    return Comparator<Any?> { left, right ->
        (left as Comparable<Any?>).compareTo(right)
    }.let { if (nullsLast) nullsLast(it) else nullsFirst(it) }
            .let { Comparator { left, right -> it.compare(get(left), get(right)) } }
}

internal class SortReceiverImpl<T>(df: DataFrameBase<T>, allowMissingColumns: Boolean) : DataFrameReceiver<T>(df, allowMissingColumns), SortReceiver<T>

internal fun <T, C> DataFrame<T>.extractSortColumns(selector: SortColumnsSelector<T, C>, unresolvedColumnsPolicy: UnresolvedColumnsPolicy): List<SortDescriptorColumn<*>> {
    return selector.toColumns().resolve(ColumnResolutionContext(this, unresolvedColumnsPolicy))
            .map {
                when (val col = it.data) {
                    is SortDescriptorColumn<*> -> col
                    else -> SortDescriptorColumn(col)
                }
            }
}

enum class SortDirection { Asc, Desc }

fun SortDirection.reversed() = when (this) {
    SortDirection.Asc -> SortDirection.Desc
    SortDirection.Desc -> SortDirection.Asc
}

class SortDescriptorColumn<C>(val column: DataColumn<C>, val direction: SortDirection = SortDirection.Asc, val nullsLast: Boolean = false) : DataColumn<C> by column

internal fun <T, G> GroupedDataFrame<T, G>.doSortBy(selector: SortColumnsSelector<G, *>): GroupedDataFrame<T, G> {

    return plain()
            .update { groups }
            .with { it?.doSortBy(selector, UnresolvedColumnsPolicy.Skip) }
            .doSortBy(selector as SortColumnsSelector<T, *>, UnresolvedColumnsPolicy.Skip)
            .toGrouped { it.frameColumn(groups.name()).typed() }
}

internal enum class SortFlag { Reversed, NullsLast }

internal fun <C> Columns<C>.addFlag(flag: SortFlag) = ColumnsWithSortFlag(this, flag)

internal fun <C> ColumnWithPath<C>.addFlag(flag: SortFlag): ColumnWithPath<C> {
    val col = data
    return when (col) {
        is SortDescriptorColumn -> {
            when (flag) {
                SortFlag.Reversed -> SortDescriptorColumn(col.column, col.direction.reversed(), col.nullsLast)
                SortFlag.NullsLast -> SortDescriptorColumn(col.column, col.direction, true)
            }
        }
        else -> {
            when (flag) {
                SortFlag.Reversed -> SortDescriptorColumn(col, SortDirection.Desc)
                SortFlag.NullsLast -> SortDescriptorColumn(col, SortDirection.Asc, true)
            }
        }
    }.addPath(path, df)
}

internal class ColumnsWithSortFlag<C>(val column: Columns<C>, val flag: SortFlag) : Columns<C> {
    override fun resolve(context: ColumnResolutionContext) = column.resolve(context).map { it.addFlag(flag) }
}