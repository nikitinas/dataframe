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

fun <T,C> DataFrame<T>.sortBy(selector: SortColumnsSelector<T, C>) = doSortBy(UnresolvedColumnsPolicy.Fail, selector)
fun <T> DataFrame<T>.sortBy(cols: Iterable<ColumnReference<Comparable<*>?>>) = sortBy { cols.toColumnSet() }
fun <T> DataFrame<T>.sortBy(vararg cols: ColumnReference<Comparable<*>?>) = sortBy { cols.toColumns() }
fun <T> DataFrame<T>.sortBy(vararg cols: String) = sortBy { cols.toColumns() }
fun <T> DataFrame<T>.sortBy(vararg cols: KProperty<Comparable<*>?>) = sortBy { cols.toColumns() }

fun <T> DataFrame<T>.sortWith(comparator: Comparator<DataRow<T>>): DataFrame<T> {
    val permutation = rows().sortedWith(comparator).asSequence().map { it.index }.asIterable()
    return this[permutation]
}

fun <T> DataFrame<T>.sortWith(comparator: (DataRow<T>, DataRow<T>)->Int) = sortWith(Comparator(comparator))

fun <T,C> DataFrame<T>.sortByDesc(selector: SortColumnsSelector<T, C>): DataFrame<T> {
    val set = selector.toColumns()
    return doSortBy { set.desc }
}

fun <T> DataFrame<T>.sortByDesc(vararg columns: KProperty<Comparable<*>?>) = sortByDesc { columns.toColumns() }
fun <T> DataFrame<T>.sortByDesc(vararg columns: String) = sortByDesc { columns.toColumns() }
fun <T> DataFrame<T>.sortByDesc(vararg columns: ColumnReference<Comparable<*>?>) = sortByDesc { columns.toColumns() }
fun <T> DataFrame<T>.sortByDesc(columns: Iterable<ColumnReference<Comparable<*>?>>) = sortByDesc { columns.toColumnSet() }

fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: String) = sortBy { cols.toColumns() }
fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: ColumnReference<Comparable<*>?>) = sortBy { cols.toColumns() }
fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: KProperty<Comparable<*>?>) = sortBy { cols.toColumns() }
fun <T, G, C> GroupedDataFrame<T, G>.sortBy(selector: SortColumnsSelector<G, C>) = doSortBy(selector)

private fun <T, G, C> GroupedDataFrame<T, G>.createColumnFromGroupExpression(receiver: SelectReceiver<T>, default: C? = null, selector: DataFrameSelector<G, C>): DataColumn<C?> {
    return receiver.exprGuess { row ->
        val group: DataFrame<G>? = row[groups]
        if(group == null) default
        else selector(group, group)
    }
}

fun <T, G, C> GroupedDataFrame<T, G>.sortByGroup(nullsLast: Boolean = false, default: C? = null, selector: DataFrameSelector<G, C>): GroupedDataFrame<T, G> = plain().sortBy {
    val column = createColumnFromGroupExpression(this, default, selector)
    if(nullsLast) column.nullsLast
    else column
}.toGrouped(groups)

fun <T, G, C> GroupedDataFrame<T, G>.sortByGroupDesc(nullsLast: Boolean = false, default: C? = null, selector: DataFrameSelector<G, C>): GroupedDataFrame<T, G> = plain().sortBy {
    val column = createColumnFromGroupExpression(this, default, selector)
    if(nullsLast) column.desc.nullsLast
    else column.desc
}.toGrouped(groups)

fun <T, G> GroupedDataFrame<T, G>.sortByCount() = sortByGroup(default = 0) { nrow() }
fun <T, G> GroupedDataFrame<T, G>.sortByCountDesc() = sortByGroupDesc(default = 0) { nrow() }

internal fun <T, C> DataFrame<T>.doSortBy(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy = UnresolvedColumnsPolicy.Fail,
    selector: SortColumnsSelector<T, C>
): DataFrame<T> {

    val columns = getSortColumns(selector, unresolvedColumnsPolicy)

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

    val valueComparator = Comparator<Any?> { left, right ->
        (left as Comparable<Any?>).compareTo(right)
    }

    val comparatorWithNulls = if (nullsLast) nullsLast(valueComparator) else nullsFirst(valueComparator)
    return Comparator { left, right -> comparatorWithNulls.compare(get(left), get(right)) }
}

@JvmName("toColumnSetForSort")
internal fun <T, C> SortColumnsSelector<T, C>.toColumns(): Columns<C> = toColumns {

    class SortReceiverImpl<T>(df: DataFrame<T>, allowMissingColumns: Boolean) : DataFrameReceiver<T>(df, allowMissingColumns), SortReceiver<T>

    SortReceiverImpl(
        it.df.typed(),
        it.allowMissingColumns
    )
}

internal fun <T, C> DataFrame<T>.getSortColumns(selector: SortColumnsSelector<T, C>, unresolvedColumnsPolicy: UnresolvedColumnsPolicy): List<SortColumnDescriptor<*>> {
    return selector.toColumns().resolve(this, unresolvedColumnsPolicy)
            .map {
                when (val col = it.data) {
                    is SortColumnDescriptor<*> -> col
                    else -> SortColumnDescriptor(col)
                }
            }
}

enum class SortDirection { Asc, Desc }

fun SortDirection.reversed() = when (this) {
    SortDirection.Asc -> SortDirection.Desc
    SortDirection.Desc -> SortDirection.Asc
}

class SortColumnDescriptor<C>(val column: DataColumn<C>, val direction: SortDirection = SortDirection.Asc, val nullsLast: Boolean = false) : DataColumn<C> by column

internal fun <T, G> GroupedDataFrame<T, G>.doSortBy(selector: SortColumnsSelector<G, *>): GroupedDataFrame<T, G> {

    return plain()
            .update { groups }
            .with { it?.doSortBy(UnresolvedColumnsPolicy.Skip, selector) }
            .doSortBy(UnresolvedColumnsPolicy.Skip, selector as SortColumnsSelector<T, *>)
            .toGrouped { it.frameColumn(groups.name()).typed() }
}

internal enum class SortFlag { Reversed, NullsLast }

internal fun <C> Columns<C>.addFlag(flag: SortFlag) = ColumnsWithSortFlag(this, flag)

internal fun <C> ColumnWithPath<C>.addFlag(flag: SortFlag): ColumnWithPath<C> {
    val col = data
    return when (col) {
        is SortColumnDescriptor -> {
            when (flag) {
                SortFlag.Reversed -> SortColumnDescriptor(col.column, col.direction.reversed(), col.nullsLast)
                SortFlag.NullsLast -> SortColumnDescriptor(col.column, col.direction, true)
            }
        }
        else -> {
            when (flag) {
                SortFlag.Reversed -> SortColumnDescriptor(col, SortDirection.Desc)
                SortFlag.NullsLast -> SortColumnDescriptor(col, SortDirection.Asc, true)
            }
        }
    }.addPath(path, df)
}

internal class ColumnsWithSortFlag<C>(val column: Columns<C>, val flag: SortFlag) : Columns<C> {
    override fun resolve(context: ColumnResolutionContext) = column.resolve(context).map { it.addFlag(flag) }
}