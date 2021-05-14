package org.jetbrains.dataframe.impl.columns.missing

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.DataRow
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.impl.columns.DataColumnGroup

internal class MissingColumnGroup<T> : MissingDataColumn<DataRow<T>>(), DataColumnGroup<T> {

    override fun <R> get(column: ColumnReference<R>) = MissingValueColumn<R>()

    override fun <R> get(column: ColumnReference<DataRow<R>>) = MissingColumnGroup<R>()

    override fun <R> get(column: ColumnReference<DataFrame<R>>) = MissingFrameColumn<R>()

    override val df: DataFrame<T>
        get() = throw UnsupportedOperationException()

    override fun tryGetColumn(columnName: String): AnyCol? {
        return null
    }

    override fun column(columnIndex: Int): AnyCol {
        return MissingValueColumn<Any?>()
    }

    override fun ncol(): Int = 0

    override fun get(index: Int) = throw UnsupportedOperationException()

    override fun get(columnName: String) = throw UnsupportedOperationException()

    override fun nrow(): Int = 0

    override fun columns(): List<AnyCol> = emptyList()

    override fun getColumnIndex(name: String) = -1

    override fun kind() = super.kind()

    override fun set(columnName: String, value: AnyCol) = throw UnsupportedOperationException()

    override fun get(firstIndex: Int, vararg otherIndices: Int) = throw UnsupportedOperationException()

    override fun distinct() = throw UnsupportedOperationException()
}