package org.jetbrains.dataframe

import com.beust.klaxon.internal.firstNotNullResult
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.io.valueColumnName
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

fun Iterable<AnyFrame?>.union() = merge(filterNotNull())

fun DataColumn<AnyFrame>.union() = values.union()

internal fun merge(dataFrames: List<AnyFrame>): AnyFrame {
    if (dataFrames.size == 1) return dataFrames[0]

    // collect column names preserving original order
    val columnNames = dataFrames
            .fold(emptyList<String>()) { acc, df -> acc + (df.columnNames() - acc) }

    val columns = columnNames.map { name ->
        val list = mutableListOf<Any?>()
        var nullable = false
        val types = mutableSetOf<KType>()

        // TODO: check not only the first column
        val firstColumn = dataFrames.firstNotNullResult { it.tryGetColumn(name) }!!
        if (firstColumn.isGroup()) {
            val groupedDataFrames = dataFrames.map {
                val column = it.tryGetColumn(name)
                if (column != null)
                    column.asFrame()
                else
                    emptyDataFrame(it.nrow())
            }
            val merged = merge(groupedDataFrames)
            DataColumn.create(name, merged)
        } else {

            val defaultValue = firstColumn.defaultValue()

            dataFrames.forEach {
                if (it.nrow() == 0) return@forEach
                val column = it.tryGetColumn(name)
                if (column != null) {
                    nullable = nullable || column.hasNulls
                    if (!column.hasNulls || column.values.any { it != null })
                        types.add(column.type)
                    list.addAll(column.values)
                } else {
                    if (it.nrow() > 0 && defaultValue == null) nullable = true
                    for (row in (0 until it.nrow())) {
                        list.add(defaultValue)
                    }
                }
            }

            if(types.any { it.classifier == DataFrame::class }){

                // convert all values to dataframe and return table column
                if(!types.all {it.classifier == DataFrame::class}){
                    list.forEachIndexed { index, value ->
                        list[index] = convertToDataFrame(value)
                    }
                }else if(nullable){
                    list.forEachIndexed { index, value ->
                        if(value == null)
                            list[index] = DataFrame.empty()
                    }
                    nullable = false
                }
                DataColumn.create(name, list as List<AnyFrame>)
            }else {
                val baseType = baseType(types).withNullability(nullable)

                if (baseType.classifier == List::class && !types.all { it.classifier == List::class })
                    list.forEachIndexed { index, value ->
                        if (value != null && value !is List<*>)
                            list[index] = listOf(value)
                    }

                DataColumn.create(name, list, baseType, defaultValue)
            }
        }
    }
    return dataFrameOf(columns)
}

internal fun convertToDataFrame(value: Any?): AnyFrame {
    return when (value) {
        null -> DataFrame.empty()
        is AnyFrame -> value
        is AnyRow -> value.toDataFrame()
        is List<*> -> value.mapNotNull { convertToDataFrame(it) }.union()
        else -> dataFrameOf(valueColumnName)(value)
    }
}

operator fun <T> DataFrame<T>.plus(other: DataFrame<T>) = merge(listOf(this, other)).typed<T>()
fun <T> DataFrame<T>.union(vararg other: DataFrame<T>) = merge(listOf(this) + other.toList()).typed<T>()