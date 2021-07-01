package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.*
import org.jetbrains.dataframe.impl.aggregation.AggregatableInternal
import org.jetbrains.dataframe.impl.aggregation.GroupByReceiverImpl
import org.jetbrains.dataframe.impl.aggregation.receivers.AggregateBodyInternal
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.io.renderToString

internal open class DataFrameImpl<T>(var columns: List<AnyCol>) : DataFrame<T>, AggregatableInternal<T> {

    private val nrow: Int = columns.firstOrNull()?.size ?: 0

    override fun nrow() = nrow

    private val columnsMap: MutableMap<String, Int>

    init {

        val invalidSizeColumns = columns.filter { it.size != nrow() }
        require(invalidSizeColumns.isEmpty()) { "Unequal column sizes:\n${columns.joinToString("\n") { it.name + " (" + it.size + ")" }}" }

        columnsMap = mutableMapOf()
        columns.forEachIndexed { i, col ->
            val name = col.name
            if (columnsMap.containsKey(name)) {
                if (name != "") {
                    val names = columns.groupBy { it.name }.filter { it.key != "" && it.value.size > 1 }.map { it.key }
                    throw IllegalArgumentException("Duplicate column names: $names. All columns: ${columnNames()}")
                }
            } else columnsMap[name] = i
        }
    }

    override fun getColumnIndex(columnName: String) = columnsMap[columnName] ?: -1

    override fun equals(other: Any?): Boolean {
        val df = other as? AnyFrame ?: return false
        return columns == df.columns()
    }

    override fun hashCode() = columns.hashCode()

    override fun toString() = renderToString()

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<DataRow<T>>? {
        return DataColumn.create("", this).addPath(emptyList(), this)
    }

    override fun set(columnName: String, value: AnyCol) {
        require(value.size == nrow()) { "Invalid column size for column '$columnName'. Expected: ${nrow()}, actual: ${value.size}" }

        val renamed = value.rename(columnName)
        val index = getColumnIndex(columnName)
        val newCols = if (index == -1) columns + renamed else columns.mapIndexed { i, col -> if (i == index) renamed else col }
        columnsMap[columnName] = if (index == -1) ncol() else index
        columns = newCols
    }

    override fun columns() = columns

    override fun <R> aggregateInternal(body: AggregateBodyInternal<T, R>): DataFrame<T> {
        val receiver = GroupByReceiverImpl(this)
        body(receiver, receiver)
        val result = receiver.compute()?.df() ?: DataFrame.empty(1)
        return result.typed()
    }

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all() }

    override fun <C> values(byRow: Boolean, columns: ColumnsSelector<T, C>): Sequence<C> {
        val cols = get(columns)
        return if (byRow) sequence {
            indices().forEach { row ->
                cols.forEach {
                    yield(it[row])
                }
            }
        }
        else sequence {
            cols.forEach { col ->
                col.values().forEach {
                    yield(it)
                }
            }
        }
    }
}

@PublishedApi
internal fun <T, R> DataFrameBase<T>.mapRows(selector: RowSelector<T, R>): Sequence<R> = rows().asSequence().map { selector(it, it) }
