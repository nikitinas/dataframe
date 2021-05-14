package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.impl.columns.missing.MissingMapColumn
import org.jetbrains.dataframe.impl.columns.missing.MissingFrameColumn
import org.jetbrains.dataframe.impl.columns.missing.MissingValueColumn
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.impl.columns.MapColumnWithParent
import org.jetbrains.dataframe.impl.columns.asGroup
import java.lang.Exception

internal fun <T> prepareForReceiver(df: DataFrameBase<T>) = DataFrameImpl<T>(df.columns().map { if(it.isGroup()) MapColumnWithParent(null, it.asGroup()) else it })

internal abstract class DataFrameReceiverBase<T>(protected val source: DataFrameBase<T>): DataFrameBase<T> by source

internal abstract class DataFrameReceiver<T>(source: DataFrameBase<T>, private val allowMissingColumns: Boolean): DataFrameReceiverBase<T>(prepareForReceiver(source)) {

    override fun column(columnIndex: Int): AnyCol {
        if(allowMissingColumns && columnIndex < 0 || columnIndex >= ncol()) return MissingValueColumn<Any?>()
        return super.column(columnIndex)
    }

    override operator fun get(columnName: String) = getColumnChecked(columnName) ?: MissingValueColumn<Any?>()

    fun <R> getColumnChecked(columnName: String): DataColumn<R>? {
        val col = source.tryGetColumn(columnName)
        if(col == null) {
            if(allowMissingColumns) return null
            throw Exception("Column not found: '$columnName'")
        }
        return col as DataColumn<R>
    }

    override operator fun <R> get(column: ColumnReference<R>): DataColumn<R> = getColumnChecked(column.name()) ?: MissingValueColumn()
    override operator fun <R> get(column: ColumnReference<DataRow<R>>): MapColumn<R> = (getColumnChecked(column.name()) ?: MissingMapColumn<R>()) as MapColumn<R>
    override operator fun <R> get(column: ColumnReference<DataFrame<R>>): FrameColumn<R> = (getColumnChecked(column.name()) ?: MissingFrameColumn<R>()) as FrameColumn<R>
}