package org.jetbrains.dataframe

import com.beust.klaxon.internal.firstNotNullResult
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.guessColumnType
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.io.valueColumnName
import org.jetbrains.dataframe.columns.values
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

fun <T> Iterable<DataFrame<T>?>.union() = merge(filterNotNull()).typed<T>()

fun <T> DataColumn<DataFrame<T>>.union() = values.union().typed<T>()

internal fun merge(dataFrames: List<AnyFrame>): AnyFrame {
    if (dataFrames.size == 1) return dataFrames[0]

    // collect column names preserving original order
    val columnNames = dataFrames
            .fold(emptyList<String>()) { acc, df -> acc + (df.columnNames() - acc) }

    val columns = columnNames.map { name ->

        val columns = dataFrames.map { it.tryGetColumn(name) }

        if (columns.all { it == null || it.isGroup()}) {
            val frames = columns.mapIndexed { index, col ->
                col?.asFrame() ?: emptyDataFrame(dataFrames[index].nrow())
            }
            val merged = merge(frames)
            DataColumn.create(name, merged)
        } else {

            var nulls = false
            val types = mutableSetOf<KType>()
            var manyNulls = false
            val defaultValue = columns.firstNotNullResult { it?.defaultValue() }
            var hasMany = false
            val list = columns.flatMapIndexed { index, col ->
                if(col != null) {
                    val type = col.type()
                    if (type.classifier == Many::class) {
                        val typeArgument = type.arguments[0].type
                        if (typeArgument != null) {
                            types.add(typeArgument)
                            if (!manyNulls) manyNulls = typeArgument.isMarkedNullable
                            hasMany = true
                        }
                    } else {
                        types.add(type)
                        if (!nulls) nulls = col.hasNulls
                    }
                    col.toList()
                }
                else {
                    val nrow = dataFrames[index].nrow()
                    if(!nulls && nrow > 0 && defaultValue == null) nulls = true
                    List(nrow) { defaultValue }
                }
            }

            val guessType = types.size > 1
            val baseType = baseType(types)
            val targetType = if(guessType || !hasMany) baseType.withNullability(nulls)
                             else Many::class.createTypeWithArgument(baseType.withNullability(manyNulls))
            guessColumnType(name, list, targetType, guessType, defaultValue)
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