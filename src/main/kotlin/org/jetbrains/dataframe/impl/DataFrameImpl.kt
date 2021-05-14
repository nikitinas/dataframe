package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.io.renderToString
import java.lang.IllegalArgumentException
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.jvm.jvmErasure

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

        if(value.size != nrow())
            throw IllegalArgumentException("Invalid column size for column '$columnName'. Expected: ${nrow()}, actual: ${value.size}")

        val renamed = value.rename(columnName)
        val index = getColumnIndex(columnName)
        val newCols = if(index == -1) columns + renamed else columns.mapIndexed { i, col -> if(i == index) renamed else col }
        columnsMap[columnName] = if(index == -1) ncol() else index
        columns = newCols
    }

    override fun columns() = columns
}