package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.SingleColumn
import org.jetbrains.dataframe.columns.FrameColumn
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import kotlin.reflect.KProperty

interface DataFrameBase<out T>: SingleColumn<DataRow<T>> {

    operator fun get(columnName: String): AnyCol
    fun tryGetColumn(columnName: String): AnyCol?

    fun getColumnGroup(columnName: String) = get(columnName).asGroup()
    fun getColumnGroup(columnPath: ColumnPath): ColumnGroup<*> = get(columnPath).asGroup()

    fun frameColumn(columnName: String): FrameColumn<*> = get(columnName).asTable()
    fun frameColumn(columnPath: ColumnPath): FrameColumn<*> = get(columnPath).asTable()

    operator fun <R> get(column: ColumnReference<R>): DataColumn<R>
    operator fun <R> get(column: ColumnReference<DataRow<R>>): ColumnGroup<R>
    operator fun <R> get(column: ColumnReference<DataFrame<R>>): FrameColumn<R>

    operator fun <R> get(column: KProperty<R>) = get(column.name) as DataColumn<R>
    operator fun <R> get(column: KProperty<DataRow<R>>) = get(column.name) as ColumnGroup<R>
    operator fun <R> get(column: KProperty<DataFrame<R>>) = get(column.name) as FrameColumn<R>

    operator fun <C> get(columns: ColumnsSelector<T, C>): List<DataColumn<C>>
    operator fun <C> get(columns: ColumnSelector<T, C>): DataColumn<C> = get(columns as ColumnsSelector<T, C>).single()

    fun <R> getColumn(name: String) = get(name) as DataColumn<R>
    fun <R> getColumn(index: Int) = column(index) as DataColumn<R>

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
    fun getOrNull(index: Int): DataRow<T>? = if(index < 0 || index >= nrow()) null else get(index)
    fun tryGetColumn(columnIndex: Int) = if(columnIndex in 0 until ncol()) column(columnIndex) else null
    fun column(columnIndex: Int): AnyCol
    fun columns(): List<AnyCol>
    fun ncol(): Int
    fun nrow(): Int
    fun rows(): Iterable<DataRow<T>>
}