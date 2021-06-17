package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.values
import kotlin.reflect.full.withNullability

internal fun DataColumn<String?>.tryParseAny(): DataColumn<*> {

    if(allNulls()) return this

    var parserId = 0
    val parsedValues = mutableListOf<Any?>()

    do {
        val parser = Parsers[parserId]
        parsedValues.clear()
        for (str in values) {
            if (str == null) parsedValues.add(null)
            else {
                val res = parser.parse(str)
                if (res == null) {
                    parserId++
                    break
                }
                parsedValues.add(res)
            }
        }
    } while (parserId < Parsers.size && parsedValues.size != size)
    if (parserId == Parsers.size) return this
    return DataColumn.create(name(), parsedValues, Parsers[parserId].type.withNullability(hasNulls))
}

fun <T> DataFrame<T>.parse() = parse { colsDfsOf() }

fun <T> DataFrame<T>.parse(columns: ColumnsSelector<T, String?>) = convert(columns).to { it.tryParseAny() }