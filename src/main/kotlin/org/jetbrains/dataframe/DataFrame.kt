package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.DataFrameReceiver
import org.jetbrains.dataframe.impl.DataRowImpl
import org.jetbrains.dataframe.impl.EmptyDataFrame
import org.jetbrains.dataframe.impl.getOrPut
import org.jetbrains.dataframe.impl.topDfs
import org.jetbrains.dataframe.columns.ColumnSet
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.impl.TreeNode
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.put
import org.jetbrains.dataframe.impl.toIndices
import kotlin.reflect.KProperty

internal open class SelectReceiverImpl<T>(df: DataFrameBase<T>, allowMissingColumns: Boolean) :
    DataFrameReceiver<T>(df, allowMissingColumns), SelectReceiver<T>

data class DataFrameSize(val ncol: Int, val nrow: Int) {
    override fun toString() = "$nrow x $ncol"
}

typealias Predicate<T> = (T) -> Boolean

typealias ColumnPath = List<String>

typealias DataFrameSelector<T, R> = DataFrame<T>.(DataFrame<T>) -> R

typealias ColumnsSelector<T, C> = SelectReceiver<T>.(SelectReceiver<T>) -> ColumnSet<C>

typealias ColumnSelector<T, C> = SelectReceiver<T>.(SelectReceiver<T>) -> ColumnReference<C>

fun <T, C> DataFrame<T>.createSelector(selector: ColumnsSelector<T, C>) = selector

internal fun <T> List<ColumnWithPath<T>>.top(): List<ColumnWithPath<T>> {
    val root = TreeNode.createRoot<ColumnWithPath<T>?>(null)
    forEach { root.put(it.path, it) }
    return root.topDfs { it.data != null }.map { it.data!! }
}

internal fun List<ColumnWithPath<*>>.allColumnsExcept(columns: Iterable<ColumnWithPath<*>>): List<ColumnWithPath<*>> {
    if(isEmpty()) return emptyList()
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
): List<ColumnWithPath<C>> = selector.toColumns().resolve(ColumnResolutionContext(this, unresolvedColumnsPolicy))

fun <T, C> DataFrame<T>.getColumnsWithPaths(selector: ColumnsSelector<T, C>): List<ColumnWithPath<C>> =
    getColumnsWithPaths(UnresolvedColumnsPolicy.Fail, selector)

fun <T, C> DataFrame<T>.getColumnPath(selector: ColumnSelector<T, C>): ColumnPath = getColumnPaths(selector).single()

fun <T, C> DataFrame<T>.getColumnPaths(selector: ColumnsSelector<T, C>): List<ColumnPath> =
    selector.toColumns().resolve(ColumnResolutionContext(this, UnresolvedColumnsPolicy.Fail)).map { it.path }

internal fun <T, C> DataFrame<T>.getGroupColumns(selector: ColumnsSelector<T, DataRow<C>>) =
    getColumnsWithPaths(selector).map { it.data.asGroup() }

fun <T, C> DataFrame<T>.column(selector: ColumnSelector<T, C>) = get(selector).single()

fun <T, C> DataFrame<T>.getColumnWithPath(selector: ColumnSelector<T, C>) = getColumnsWithPaths(selector).single()

@JvmName("getColumnForSpread")
internal fun <T, C> DataFrame<T>.getColumn(selector: SpreadColumnSelector<T, C>) =
    DataFrameForSpreadImpl(this).let { selector(it, it) }.let {
        this[it]
    }

internal fun <T> DataFrame<T>.getColumns(columnNames: Array<out String>) = columnNames.map { this[it] }

internal fun <T, C> DataFrame<T>.getColumns(columnNames: Array<out KProperty<C>>) =
    columnNames.map { this[it.name] as ColumnReference<C> }

internal fun <T> DataFrame<T>.getColumns(columnNames: List<String>): List<AnyCol> = columnNames.map { this[it] }

internal fun <T> DataFrame<T>.new(columns: Iterable<AnyCol>) = dataFrameOf(columns).typed<T>()

interface DataFrame<out T> : DataFrameBase<T> {

    companion object {
        fun empty(nrow: Int = 0): AnyFrame = EmptyDataFrame<Any?>(nrow)
    }

    fun nrow(): Int

    fun indices(): IntRange = 0 until nrow

    override fun ncol(): Int = columns().size

    fun rows(): Iterable<DataRow<T>> = forwardIterable()
    fun columnNames() = columns().map { it.name() }

    override fun columns(): List<AnyCol>
    override fun column(columnIndex: Int) = columns()[columnIndex]

    operator fun set(columnName: String, value: AnyCol)

    override operator fun get(index: Int): DataRow<T> = DataRowImpl(index, this)
    override operator fun get(columnName: String) =
        tryGetColumn(columnName) ?: throw Exception("Column not found: '$columnName'")

    override operator fun <R> get(column: ColumnReference<R>): DataColumn<R> = tryGetColumn(column) ?: error("Column not found: ${column.path().joinToString("/")}")
    override operator fun <R> get(column: ColumnReference<DataRow<R>>): ColumnGroup<R> =
        get<DataRow<R>>(column) as ColumnGroup<R>

    override operator fun <R> get(column: ColumnReference<DataFrame<R>>): FrameColumn<R> =
        get<DataFrame<R>>(column) as FrameColumn<R>

    operator fun <C> get(selector: ColumnsSelector<T, C>): List<DataColumn<C>> = getColumns(false, selector)

    operator fun get(indices: Iterable<Int>) = getRows(indices)
    operator fun get(mask: BooleanArray) = getRows(mask)
    operator fun get(range: IntRange) = getRows(range)
    operator fun get(firstIndex: Int, vararg otherIndices: Int) = get(headPlusIterable(firstIndex, otherIndices.asIterable()))

    operator fun plus(col: AnyCol) = dataFrameOf(columns() + col).typed<T>()
    operator fun plus(col: Iterable<AnyCol>) = new(columns() + col)
    operator fun plus(stub: AddRowNumberStub) = addRowNumber(stub.columnName)

    fun getRows(indices: Iterable<Int>) = columns().map { col -> col.slice(indices) }.asDataFrame<T>()
    fun getRows(mask: BooleanArray) = getRows(mask.toIndices())
    fun getRows(range: IntRange) = columns().map { col -> col.slice(range) }.asDataFrame<T>()

    fun getColumnIndex(name: String): Int
    fun getColumnIndex(col: AnyCol) = getColumnIndex(col.name())

    fun <R> tryGetColumn(column: ColumnReference<R>): DataColumn<R>? =
        tryGetColumn(column.path()) as? DataColumn<R>

    override fun tryGetColumn(columnName: String): AnyCol? =
        getColumnIndex(columnName).let { if (it != -1) column(it) else null }

    fun tryGetColumn(path: ColumnPath): AnyCol? =
        if (path.size == 1) tryGetColumn(path[0])
        else path.dropLast(1).fold(this as AnyFrame?) { df, name -> df?.tryGetColumn(name) as? AnyFrame? }
            ?.tryGetColumn(path.last())

    fun tryGetColumnGroup(name: String) = tryGetColumn(name) as? ColumnGroup<*>
    fun getColumnGroup(name: String) = tryGetColumnGroup(name)!!

    operator fun get(col1: Column, col2: Column, vararg other: Column) = select(listOf(col1, col2) + other)
    operator fun get(col1: String, col2: String, vararg other: String) = select(getColumns(listOf(col1, col2) + other))

    fun all(predicate: RowFilter<T>): Boolean = rows().all { predicate(it, it) }
    fun any(predicate: RowFilter<T>): Boolean = rows().any { predicate(it, it) }

    fun first() = get(0)
    fun firstOrNull() = if(nrow > 0) first() else null
    fun first(predicate: RowFilter<T>) = rows().first { predicate(it, it) }
    fun firstOrNull(predicate: RowFilter<T>) = rows().firstOrNull { predicate(it, it) }
    fun last() = get(nrow-1)
    fun lastOrNull() = if(nrow > 0) last() else null
    fun last(predicate: RowFilter<T>) = backwardIterable().first { predicate(it, it) }
    fun lastOrNull(predicate: RowFilter<T>) = backwardIterable().firstOrNull { predicate(it, it) }
    fun take(numRows: Int) = getRows(0 until numRows)
    fun drop(numRows: Int) = getRows(numRows until nrow())
    fun takeLast(numRows: Int) = getRows(nrow() - numRows until nrow())
    fun skipLast(numRows: Int) = getRows(0 until nrow() - numRows)
    fun head(numRows: Int = 5) = take(numRows)
    fun tail(numRows: Int = 5) = takeLast(numRows)
    fun shuffled() = getRows((0 until nrow()).shuffled())
    fun <K, V> associate(transform: RowSelector<T, Pair<K, V>>) = rows().associate { transform(it, it) }
    fun <V> associateBy(transform: RowSelector<T, V>) = rows().associateBy { transform(it, it) }
    fun <R> distinctBy(selector: RowSelector<T, R>) =
        rows().distinctBy { selector(it, it) }.map { it.index }.let { getRows(it) }

    fun single() = rows().single()
    fun single(predicate: RowSelector<T, Boolean>) = rows().single { predicate(it, it) }

    fun <R> map(selector: RowSelector<T, R>) = rows().map { selector(it, it) }

    fun <R> mapIndexed(action: (Int, DataRow<T>) -> R) = rows().mapIndexed(action)

    fun <R> mapIndexedNotNull(action: (Int, DataRow<T>) -> R?) = rows().mapIndexedNotNull(action)

    fun distinct() = distinctBy { it.values }
}

fun AnyFrame.size() = DataFrameSize(ncol(), nrow())

fun AnyFrame.getFrame(path: ColumnPath): AnyFrame = if(path.isNotEmpty()) this[path].asFrame() else this

fun <T> AnyFrame.typed(): DataFrame<T> = this as DataFrame<T>

fun <T> DataFrameBase<*>.typed(): DataFrameBase<T> = this as DataFrameBase<T>

fun <T> DataRow<T>.toDataFrame(): DataFrame<T> = owner[index..index]

fun <T> DataFrame<T>.forwardIterable() = object : Iterable<DataRow<T>> {
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

fun <T> DataFrame<T>.backwardIterable() = object : Iterable<DataRow<T>> {
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

fun <T, C> DataFrame<T>.forEachIn(selector: ColumnsSelector<T, C>, action: (DataRow<T>, DataColumn<C>) -> Unit) =
    getColumnsWithPaths(selector).let { cols ->
        rows().forEach { row ->
            cols.forEach { col ->
                action(row, col.data)
            }
        }
    }

typealias AnyFrame = DataFrame<*>

internal val AnyFrame.ncol get() = ncol()
internal val AnyFrame.nrow get() = nrow()
internal val AnyFrame.indices get() = indices()