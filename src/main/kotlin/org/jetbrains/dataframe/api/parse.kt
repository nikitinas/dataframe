package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.hasNulls
import org.jetbrains.dataframe.columns.size
import org.jetbrains.dataframe.columns.typeClass
import org.jetbrains.dataframe.columns.values
import kotlin.reflect.full.withNullability

val DataFrame.Companion.parser: DataFrameParserOptions get() = Parsers

interface DataFrameParserOptions {

    fun addDateTimeFormat(format: String)

}

fun DataColumn<String?>.tryParse(): DataColumn<*> {

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

fun <T> DataFrame<T>.parse(): DataFrame<T> = parse { this@parse.dfs() }

fun <T> DataFrame<T>.parse(columns: ColumnsSelector<T, Any?>): DataFrame<T> = convert(columns).to {
    when {
        it.isFrameColumn() -> it.castTo<AnyFrame?>().parse()
        it.typeClass == String::class -> it.castTo<String?>().tryParse()
        else -> it
    }
}

fun DataColumn<String?>.parse() = tryParse().also { if(it.typeClass == String::class) error("Can't guess column type")}

@JvmName("tryParseAnyFrame?")
fun DataColumn<AnyFrame?>.parse(): DataColumn<AnyFrame?> = map { it?.parse() }