package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.typed

class CrossIndexDescriptor<T>(
    var values: DataColumn<T>? = null,
    var distinctValues: List<T>? = null,
)

typealias AnyCrossDescriptor = CrossIndexDescriptor<Any?>

class CrossTabBuilder<T>(
    private val df: DataFrame<T>,
): DataFrame<T> by df {
    private val rowsDescriptor: AnyCrossDescriptor = CrossIndexDescriptor()
    private val columnsDescriptor: AnyCrossDescriptor = CrossIndexDescriptor()

    fun <C> crossRows(col: DataColumn<C>): CrossIndexDescriptor<C> = rowsDescriptor.crossApply { it.values = col }
    fun <C> crossRows(colName: String): CrossIndexDescriptor<C> = crossRows(df[colName].typed())

    fun <C> crossColumns(col: DataColumn<C>): CrossIndexDescriptor<C> = columnsDescriptor.crossApply { it.values = col }
    fun <C> crossColumns(colName: String): CrossIndexDescriptor<C> = crossColumns(df[colName].typed())

    infix fun <C> CrossIndexDescriptor<C>.byVals(values: List<C>) {
        distinctValues = values
    }

    fun build() = CrossTabAggregator(
        df,
        rowsDescriptor.values!!,
        columnsDescriptor.values!!,
        rowsDescriptor.distinctValues,
        columnsDescriptor.distinctValues
    )

    companion object {
        private fun <C> AnyCrossDescriptor.crossApply(block: AnyCrossDescriptor.(AnyCrossDescriptor) -> Unit): CrossIndexDescriptor<C> {
            block(this, this)
            return this as CrossIndexDescriptor<C>
        }
    }
}

class CrossTabAggregator<T>(
    val df: DataFrame<T>,
    val rowsValues: DataColumn<Any?>,
    val colsValues: DataColumn<Any?>,
    rowsSchema_: List<Any?>? = null,
    colsSchema_: List<Any?>? = null,
){
    val rowsSchema = rowsSchema_ ?: rowsValues.distinct().toList()
    val colsSchema = colsSchema_ ?: colsValues.distinct().toList()
}

inline fun <T, reified V> CrossTabAggregator<T>.aggregate(defaultValue: V? = null, block: DataFrame<T>.(DataFrame<T>) -> V): AnyFrame {
    val map = mutableSetOf<Pair<Any?, Any?>>()

    (0 until rowsValues.size).forEach { index ->
        map.add(rowsValues[index] to colsValues[index])
    }

    val resMap = mutableMapOf<Pair<Any?, Any?>, V>()
    map.forEach { (k1, k2) ->
        val filtered = df.filter { rowsValues.eq(k1) && colsValues.eq(k2) }
        val res = block(filtered, filtered)
        resMap[k1 to k2] = res
    }

    val data = colsSchema.mapTo(mutableListOf(column("column", rowsSchema))) { colName ->
        val dataCol = rowsSchema.map { rowName ->
            resMap[rowName to colName]  ?: defaultValue
        }
        column(colName.toString(), dataCol)
    }

    return dataFrameOf(data)
}

fun <T> CrossTabAggregator<T>.count() = aggregate(0) { it.nrow() }

fun <T> DataFrame<T>.crossTab(block: CrossTabBuilder<T>.(CrossTabBuilder<T>) -> Unit): CrossTabAggregator<T> {
    val builder = CrossTabBuilder(this)
    block(builder, builder)
    return builder.build()
}
