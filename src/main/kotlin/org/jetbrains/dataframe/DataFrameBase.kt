package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.SingleColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import kotlin.reflect.KProperty

interface DataFrameBase<out T>: SingleColumn<DataRow<T>> {

    operator fun get(columnName: String): AnyCol
    fun tryGetColumn(columnName: String): AnyCol?

    fun getGroup(columnName: String) = get(columnName).asGroup()
    fun getGroup(columnPath: ColumnPath): MapColumn<*> = get(columnPath).asGroup()

    fun frameColumn(columnName: String): FrameColumn<*> = get(columnName).asTable()
    fun frameColumn(columnPath: ColumnPath): FrameColumn<*> = get(columnPath).asTable()

    operator fun <R> get(column: ColumnReference<R>): DataColumn<R>
    operator fun <R> get(column: ColumnReference<DataRow<R>>): MapColumn<R>
    operator fun <R> get(column: ColumnReference<DataFrame<R>>): FrameColumn<R>

    fun hasColumn(columnName: String) = tryGetColumn(columnName) != null

    operator fun get(columnPath: ColumnPath): AnyCol {

        var res: AnyCol? = null
        columnPath.forEach {
            if(res == null) res = this[it]
            else res = res!!.asGroup()[it]
        }
        return res!!
    }

    operator fun get(index: Int): DataRow<T>
    fun tryGetColumn(columnIndex: Int) = if(columnIndex in 0 until ncol()) column(columnIndex) else null
    fun column(columnIndex: Int): AnyCol
    fun columns(): List<AnyCol>
    fun ncol(): Int
}

operator fun <T, R> DataFrameBase<T>.get(column: KProperty<DataFrame<R>>) = get(column.toColumnDef())

@JvmName("getT")
operator fun <T, R> DataFrameBase<T>.get(column: KProperty<DataRow<R>>) = get(column.toColumnDef())

operator fun <T, R> DataFrameBase<T>.get(column: KProperty<R>) = get(column.toColumnDef())