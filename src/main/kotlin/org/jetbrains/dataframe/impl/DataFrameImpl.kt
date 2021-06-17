package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.aggregation.AggregateReceiver
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.shortPath
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.impl.aggregation.toColumnWithPath
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.io.renderToString
import java.lang.IllegalArgumentException
import kotlin.reflect.KType

internal open class DataFrameImpl<T>(var columns: List<AnyCol>) : DataFrame<T> {

    private val nrow: Int = columns.firstOrNull()?.size ?: 0

    override fun nrow() = nrow

    private val columnsMap : MutableMap<String, Int>

    init {

        val invalidSizeColumns = columns.filter { it.size != nrow() }
        require(invalidSizeColumns.isEmpty()) { "Unequal column sizes:\n${columns.joinToString("\n") { it.name + " (" + it.size + ")" }}" }

        columnsMap = mutableMapOf()
        columns.forEachIndexed { i, col ->
            val name = col.name
            if(columnsMap.containsKey(name)){
                if(name != "") {
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
        val newCols = if(index == -1) columns + renamed else columns.mapIndexed { i, col -> if(i == index) renamed else col }
        columnsMap[columnName] = if(index == -1) ncol() else index
        columns = newCols
    }

    override fun columns() = columns

    override fun <R> aggregateBase(body: AggregateBody<T, R>): DataFrame<T> {

        class DataFrameAggregateReceiver: AggregateReceiver<T>, DataFrame<T> by this {

            val values = mutableListOf<NamedValue>()

            override fun yield(value: NamedValue) = value.also { values.add(it) }

            override fun <R> yield(path: ColumnPath, value: R, type: KType?, default: R?) = yield(path, value, type, default, false)

            override fun pathForSingleColumn(column: AnyCol) = column.shortPath()
        }

        val receiver = DataFrameAggregateReceiver()
        val result = body(receiver, receiver)
        val values = if(result != Unit && receiver.values.isEmpty()) listOf(NamedValue.create(pathOf("value"), result, null, null, true))
            else receiver.values
        return values.map { it.toColumnWithPath() }.toDataFrame<T>()
    }

    override fun remainingColumnsSelector(): ColumnsSelector<*, *> = { all() }
}

@PublishedApi
internal fun <T, R> DataFrameBase<T>.mapRows(selector: RowSelector<T, R>):Sequence<R> = rows().asSequence().map { selector(it,it) }