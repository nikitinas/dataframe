package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.*
import org.jetbrains.dataframe.impl.*
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.columns.toColumns

internal open class SelectReceiverImpl<T>(source: DataFrame<T>, allowMissingColumns: Boolean) :
    DataFrameReceiver<T>(source, allowMissingColumns), SelectReceiver<T>

public data class DataFrameSize(val ncol: Int, val nrow: Int) {
    override fun toString(): String = "$nrow x $ncol"
}

public typealias Predicate<T> = (T) -> Boolean

public typealias ColumnPath = List<String>

internal fun ColumnPath.replaceLast(name: String) = if (size < 2) listOf(name) else dropLast(1) + name

public typealias DataFrameSelector<T, R> = DataFrame<T>.(DataFrame<T>) -> R

public typealias ColumnsSelector<T, C> = SelectReceiver<T>.(SelectReceiver<T>) -> Columns<C>

public typealias ColumnSelector<T, C> = SelectReceiver<T>.(SelectReceiver<T>) -> ColumnReference<C>

public fun <T, C> DataFrame<T>.createSelector(selector: ColumnsSelector<T, C>): SelectReceiver<T>.(SelectReceiver<T>) -> Columns<C> = selector

internal fun <T> List<ColumnWithPath<T>>.top(): List<ColumnWithPath<T>> {
    val root = TreeNode.createRoot<ColumnWithPath<T>?>(null)
    forEach { root.put(it.path, it) }
    return root.topDfs { it.data != null }.map { it.data!! }
}

internal fun List<ColumnWithPath<*>>.allColumnsExcept(columns: Iterable<ColumnWithPath<*>>): List<ColumnWithPath<*>> {
    if (isEmpty()) return emptyList()
    val df = this[0].df
    require(all { it.df === df })
    val fullTree = collectTree()
    columns.forEach {
        var node = fullTree.getOrPut(it.path).asNullable()
        node?.dfs()?.forEach { it.data = null }
        while (node != null) {
            node.data = null
            node = node.parent
        }
    }
    val dfs = fullTree.topDfs { it.data != null }
    return dfs.map { it.data!!.addPath(it.pathFromRoot(), df) }
}

internal fun <T, C> DataFrame<T>.getColumns(
    skipMissingColumns: Boolean,
    selector: ColumnsSelector<T, C>
): List<DataColumn<C>> = getColumnsWithPaths(
    if (skipMissingColumns) UnresolvedColumnsPolicy.Skip else UnresolvedColumnsPolicy.Fail,
    selector
).map { it.data }

internal fun <T, C> DataFrame<T>.getColumnsWithPaths(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy,
    selector: ColumnsSelector<T, C>
): List<ColumnWithPath<C>> = selector.toColumns().resolve(this, unresolvedColumnsPolicy)

public fun <T, C> DataFrame<T>.getColumnsWithPaths(selector: ColumnsSelector<T, C>): List<ColumnWithPath<C>> =
    getColumnsWithPaths(UnresolvedColumnsPolicy.Fail, selector)

public fun <T, C> DataFrame<T>.getColumnPath(selector: ColumnSelector<T, C>): ColumnPath = getColumnPaths(selector).single()

public fun <T, C> DataFrame<T>.getColumnPaths(selector: ColumnsSelector<T, C>): List<ColumnPath> =
    selector.toColumns().resolve(this, UnresolvedColumnsPolicy.Fail).map { it.path }

public fun <T, C> DataFrame<T>.column(selector: ColumnSelector<T, C>): DataColumn<C> = get(selector)

public fun <T, C> DataFrame<T>.getColumnWithPath(selector: ColumnSelector<T, C>): ColumnWithPath<C> = getColumnsWithPaths(selector).single()

internal fun <T> DataFrame<T>.getColumns(columnNames: List<String>): List<AnyCol> = columnNames.map { this[it] }

internal fun <T> DataFrame<T>.new(columns: Iterable<AnyCol>) = dataFrameOf(columns).typed<T>()

public interface DataFrame<out T> : DataFrameAggregations<T> {

    public companion object {
        public fun empty(nrow: Int = 0): AnyFrame = EmptyDataFrame<Any?>(nrow)
    }

    public fun indices(): IntRange = 0 until nrow

    override fun ncol(): Int = columns().size

    override fun rows(): Iterable<DataRow<T>> = forwardIterable()

    public fun <C> values(byRow: Boolean = false, columns: ColumnsSelector<T, C>): Sequence<C>
    public fun values(byRow: Boolean = false): Sequence<Any?> = values(byRow) { all() }

    public fun <C> valuesNotNull(byRow: Boolean = false, columns: ColumnsSelector<T, C?>): Sequence<C> = values(byRow, columns).filterNotNull()
    public fun valuesNotNull(byRow: Boolean = false): Sequence<Any> = valuesNotNull(byRow) { all() }

    public fun columnNames(): List<String> = columns().map { it.name() }

    override fun columns(): List<AnyCol>
    public fun <C> columns(selector: ColumnsSelector<T, C>): List<DataColumn<C>> = get(selector)

    override fun column(columnIndex: Int): DataColumn<*> = columns()[columnIndex]

    public operator fun set(columnName: String, value: AnyCol)

    override operator fun get(index: Int): DataRow<T> = DataRowImpl(index, this)

    override operator fun get(columnName: String): DataColumn<*> =
        tryGetColumn(columnName) ?: throw Exception("Column not found: '$columnName'")

    override operator fun <R> get(column: ColumnReference<R>): DataColumn<R> = tryGetColumn(column)
        ?: error("Column not found: ${column.path().joinToString("/")}")

    override operator fun <R> get(column: ColumnReference<DataRow<R>>): ColumnGroup<R> =
        get<DataRow<R>>(column) as ColumnGroup<R>

    override operator fun <R> get(column: ColumnReference<DataFrame<R>>): FrameColumn<R> =
        get<DataFrame<R>>(column) as FrameColumn<R>

    override operator fun <C> get(selector: ColumnsSelector<T, C>): List<DataColumn<C>> = getColumns(false, selector)

    public operator fun get(indices: Iterable<Int>): DataFrame<T> = getRows(indices)
    public operator fun get(mask: BooleanArray): DataFrame<T> = getRows(mask)
    public operator fun get(range: IntRange): DataFrame<T> = getRows(range)
    public operator fun get(firstIndex: Int, vararg otherIndices: Int): DataFrame<T> = get(headPlusIterable(firstIndex, otherIndices.asIterable()))

    public operator fun plus(col: AnyCol): DataFrame<T> = dataFrameOf(columns() + col).typed<T>()
    public operator fun plus(col: Iterable<AnyCol>): DataFrame<T> = new(columns() + col)
    public operator fun plus(stub: AddRowNumberStub): DataFrame<T> = addRowNumber(stub.columnName)

    public fun getRows(indices: Iterable<Int>): DataFrame<T> = columns().map { col -> col.slice(indices) }.asDataFrame<T>()
    public fun getRows(mask: BooleanArray): DataFrame<T> = getRows(mask.toIndices())
    public fun getRows(range: IntRange): DataFrame<T> = if (range == indices()) this else columns().map { col -> col.slice(range) }.asDataFrame<T>()

    public fun getColumnIndex(name: String): Int
    public fun getColumnIndex(col: AnyCol): Int = getColumnIndex(col.name())

    public fun <R> tryGetColumn(column: ColumnReference<R>): DataColumn<R>? = column.resolveSingle(this, UnresolvedColumnsPolicy.Skip)?.data

    override fun tryGetColumn(columnName: String): AnyCol? =
        getColumnIndex(columnName).let { if (it != -1) column(it) else null }

    public fun tryGetColumn(path: ColumnPath): AnyCol? =
        if (path.size == 1) tryGetColumn(path[0])
        else path.dropLast(1).fold(this as AnyFrame?) { df, name -> df?.tryGetColumn(name) as? AnyFrame? }
            ?.tryGetColumn(path.last())

    public fun tryGetColumnGroup(name: String): ColumnGroup<*>? = tryGetColumn(name) as? ColumnGroup<*>

    public operator fun get(first: Column, vararg other: Column): DataFrame<T> = select(listOf(first) + other)
    public operator fun get(first: String, vararg other: String): DataFrame<T> = select(listOf(first) + other)

    public fun all(predicate: RowFilter<T>): Boolean = rows().all { predicate(it, it) }
    public fun any(predicate: RowFilter<T>): Boolean = rows().any { predicate(it, it) }

    public fun first(): DataRow<T> = get(0)
    public fun firstOrNull(): DataRow<T>? = if (nrow > 0) first() else null
    public fun first(predicate: RowFilter<T>): DataRow<T> = rows().first { predicate(it, it) }
    public fun firstOrNull(predicate: RowFilter<T>): DataRow<T>? = rows().firstOrNull { predicate(it, it) }
    public fun last(): DataRow<T> = get(nrow - 1)
    public fun lastOrNull(): DataRow<T>? = if (nrow > 0) last() else null
    public fun last(predicate: RowFilter<T>): DataRow<T> = backwardIterable().first { predicate(it, it) }
    public fun lastOrNull(predicate: RowFilter<T>): DataRow<T>? = backwardIterable().firstOrNull { predicate(it, it) }
    public fun take(numRows: Int): DataFrame<T> = getRows(0 until numRows)
    public fun drop(numRows: Int): DataFrame<T> = getRows(numRows until nrow())
    public fun takeLast(numRows: Int): DataFrame<T> = getRows(nrow() - numRows until nrow())
    public fun skipLast(numRows: Int): DataFrame<T> = getRows(0 until nrow() - numRows)
    public fun head(numRows: Int = 5): DataFrame<T> = take(numRows)
    public fun tail(numRows: Int = 5): DataFrame<T> = takeLast(numRows)
    public fun shuffled(): DataFrame<T> = getRows((0 until nrow()).shuffled())
    public fun <K, V> associate(transform: RowSelector<T, Pair<K, V>>): Map<K, V> = rows().associate { transform(it, it) }
    public fun <V> associateBy(transform: RowSelector<T, V>): Map<V, DataRow<T>> = rows().associateBy { transform(it, it) }

    public fun single(): DataRow<T> = rows().single()
    public fun single(predicate: RowSelector<T, Boolean>): DataRow<T> = rows().single { predicate(it, it) }

    public fun <R> mapIndexed(action: (Int, DataRow<T>) -> R): List<R> = rows().mapIndexed(action)

    public fun <R> mapIndexedNotNull(action: (Int, DataRow<T>) -> R?): List<R> = rows().mapIndexedNotNull(action)

    public operator fun iterator(): Iterator<DataRow<T>> = rows().iterator()
}

public inline fun <T, R> DataFrame<T>.map(selector: RowSelector<T, R>): List<R> = rows().map { selector(it, it) }

public fun AnyFrame.size(): DataFrameSize = DataFrameSize(ncol(), nrow())

public fun AnyFrame.getFrame(path: ColumnPath): AnyFrame = if (path.isNotEmpty()) this[path].asFrame() else this

public fun <T> AnyFrame.typed(): DataFrame<T> = this as DataFrame<T>

public fun <T> AnyRow.typed(): DataRow<T> = this as DataRow<T>

public fun <T> DataFrameBase<*>.typed(): DataFrameBase<T> = this as DataFrameBase<T>

public fun <T> DataRow<T>.toDataFrame(): DataFrame<T> = owner[index..index]

public fun <T> Iterable<DataRow<T>>.toDataFrame(): DataFrame<T> {
    var uniqueDf: DataFrame<T>? = null
    for (row in this) {
        if (uniqueDf == null) uniqueDf = row.df()
        else {
            if (uniqueDf !== row.df()) {
                uniqueDf = null
                break
            }
        }
    }
    return if (uniqueDf != null) {
        val permutation = map { it.index }
        uniqueDf[permutation]
    } else map { it.toDataFrame() }.union()
}

public fun <T> DataFrame<T>.forwardIterable(): Iterable<DataRow<T>> = object : Iterable<DataRow<T>> {
    override fun iterator() =

        object : Iterator<DataRow<T>> {
            var nextRow = 0

            override fun hasNext(): Boolean = nextRow < nrow

            override fun next(): DataRow<T> {
                require(nextRow < nrow)
                return get(nextRow++)
            }
        }
}

public fun <T> DataFrame<T>.backwardIterable(): Iterable<DataRow<T>> = object : Iterable<DataRow<T>> {
    override fun iterator() =

        object : Iterator<DataRow<T>> {
            var nextRow = nrow - 1

            override fun hasNext(): Boolean = nextRow >= 0

            override fun next(): DataRow<T> {
                require(nextRow >= 0)
                return get(nextRow--)
            }
        }
}

public fun <T, C> DataFrame<T>.forEachIn(selector: ColumnsSelector<T, C>, action: (DataRow<T>, DataColumn<C>) -> Unit): Unit =
    getColumnsWithPaths(selector).let { cols ->
        rows().forEach { row ->
            cols.forEach { col ->
                action(row, col.data)
            }
        }
    }

public typealias AnyFrame = DataFrame<*>

internal val AnyFrame.ncol get() = ncol()
internal val AnyFrame.nrow get() = nrow()
internal val AnyFrame.indices get() = indices()
