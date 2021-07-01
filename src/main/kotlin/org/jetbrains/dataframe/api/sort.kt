package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.*
import org.jetbrains.dataframe.impl.DataFrameReceiver
import org.jetbrains.dataframe.impl.columns.*
import kotlin.reflect.KProperty

public interface SortReceiver<out T> : SelectReceiver<T> {

    public val <C> Columns<C>.desc: Columns<C> get() = addFlag(SortFlag.Reversed)
    public val String.desc: Columns<Comparable<*>?> get() = cast<Comparable<*>>().desc
    public val <C> KProperty<C>.desc: Columns<C> get() = toColumnDef().desc

    public val <C> Columns<C?>.nullsLast: Columns<C?> get() = addFlag(SortFlag.NullsLast)
    public val String.nullsLast: Columns<Comparable<*>?> get() = cast<Comparable<*>>().nullsLast
    public val <C> KProperty<C?>.nullsLast: Columns<C?> get() = toColumnDef().nullsLast
}

public typealias SortColumnsSelector<T, C> = Selector<SortReceiver<T>, Columns<C>>

public fun <T, C> DataFrame<T>.sortBy(selector: SortColumnsSelector<T, C>): DataFrame<T> = doSortBy(UnresolvedColumnsPolicy.Fail, selector)
public fun <T> DataFrame<T>.sortBy(cols: Iterable<ColumnReference<Comparable<*>?>>): DataFrame<T> = sortBy { cols.toColumnSet() }
public fun <T> DataFrame<T>.sortBy(vararg cols: ColumnReference<Comparable<*>?>): DataFrame<T> = sortBy { cols.toColumns() }
public fun <T> DataFrame<T>.sortBy(vararg cols: String): DataFrame<T> = sortBy { cols.toColumns() }
public fun <T> DataFrame<T>.sortBy(vararg cols: KProperty<Comparable<*>?>): DataFrame<T> = sortBy { cols.toColumns() }

public fun <T> DataFrame<T>.sortWith(comparator: Comparator<DataRow<T>>): DataFrame<T> {
    val permutation = rows().sortedWith(comparator).asSequence().map { it.index }.asIterable()
    return this[permutation]
}

public fun <T> DataFrame<T>.sortWith(comparator: (DataRow<T>, DataRow<T>) -> Int): DataFrame<T> = sortWith(Comparator(comparator))

public fun <T, C> DataFrame<T>.sortByDesc(selector: SortColumnsSelector<T, C>): DataFrame<T> {
    val set = selector.toColumns()
    return doSortBy { set.desc }
}

public fun <T> DataFrame<T>.sortByDesc(vararg columns: KProperty<Comparable<*>?>): DataFrame<T> = sortByDesc { columns.toColumns() }
public fun <T> DataFrame<T>.sortByDesc(vararg columns: String): DataFrame<T> = sortByDesc { columns.toColumns() }
public fun <T> DataFrame<T>.sortByDesc(vararg columns: ColumnReference<Comparable<*>?>): DataFrame<T> = sortByDesc { columns.toColumns() }
public fun <T> DataFrame<T>.sortByDesc(columns: Iterable<ColumnReference<Comparable<*>?>>): DataFrame<T> = sortByDesc { columns.toColumnSet() }

public fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: String) = sortBy { cols.toColumns() }
public fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: ColumnReference<Comparable<*>?>) = sortBy { cols.toColumns() }
public fun <T, G> GroupedDataFrame<T, G>.sortBy(vararg cols: KProperty<Comparable<*>?>) = sortBy { cols.toColumns() }
public fun <T, G, C> GroupedDataFrame<T, G>.sortBy(selector: SortColumnsSelector<G, C>) = doSortBy(selector)

private fun <T, G, C> GroupedDataFrame<T, G>.createColumnFromGroupExpression(receiver: SelectReceiver<T>, default: C? = null, selector: DataFrameSelector<G, C>): DataColumn<C?> {
    return receiver.exprGuess { row ->
        val group: DataFrame<G>? = row[groups]
        if(group == null) default
        else selector(group, group)
    }
}

public fun <T, G, C> GroupedDataFrame<T, G>.sortByGroup(nullsLast: Boolean = false, default: C? = null, selector: DataFrameSelector<G, C>): GroupedDataFrame<T, G> = plain().sortBy {
    val column = createColumnFromGroupExpression(this, default, selector)
    if(nullsLast) column.nullsLast
    else column
}.toGrouped(groups)

public fun <T, G, C> GroupedDataFrame<T, G>.sortByGroupDesc(nullsLast: Boolean = false, default: C? = null, selector: DataFrameSelector<G, C>): GroupedDataFrame<T, G> = plain().sortBy {
    val column = createColumnFromGroupExpression(this, default, selector)
    if(nullsLast) column.desc.nullsLast
    else column.desc
}.toGrouped(groups)

public fun <T, G> GroupedDataFrame<T, G>.sortByCount() = sortByGroup(default = 0) { nrow() }
public fun <T, G> GroupedDataFrame<T, G>.sortByCountDesc() = sortByGroupDesc(default = 0) { nrow() }

internal fun <T, C> DataFrame<T>.doSortBy(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy = UnresolvedColumnsPolicy.Fail,
    selector: SortColumnsSelector<T, C>
): DataFrame<T> {

internal fun <T, C> DataFrame<T>.doSortBy(
    selector: SortColumnsSelector<T, C>,
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy = UnresolvedColumnsPolicy.Fail
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

internal fun <T, C> DataFrame<T>.getSortColumns(
    selector: SortColumnsSelector<T, C>,
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy
): List<SortColumnDescriptor<*>> {
    return selector.toColumns().resolve(this, unresolvedColumnsPolicy)
        .map {
            when (val col = it.data) {
                is SortColumnDescriptor<*> -> col
                else -> SortColumnDescriptor(col)
            }
        }
}

public enum class SortDirection { Asc, Desc }

public fun SortDirection.reversed(): SortDirection = when (this) {
    SortDirection.Asc -> SortDirection.Desc
    SortDirection.Desc -> SortDirection.Asc
}

public class SortColumnDescriptor<C>(
    public val column: DataColumn<C>,
    public val direction: SortDirection = SortDirection.Asc,
    public val nullsLast: Boolean = false
) : DataColumn<C> by column

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
