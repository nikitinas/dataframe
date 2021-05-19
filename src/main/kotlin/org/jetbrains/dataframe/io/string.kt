package org.jetbrains.dataframe.io

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.impl.*
import org.jetbrains.dataframe.columns.values

fun <T, G> GroupedDataFrame<T, G>.print() = println(this)

internal fun AnyFrame.renderToString(limit: Int = 20, truncate: Int = 40): String {
    val sb = StringBuilder()
    sb.appendLine("Data Frame [${size()}]")
    sb.appendLine()

    val outputRows = limit.coerceAtMost(nrow())
    val output = columns().map { it.values.take(limit).map { renderValue(it).truncate(truncate) } }
    val header = columns().map { "${it.name()}:${renderType(it)}"}
    val columnLengths = output.mapIndexed { col, values -> (values + header[col]).map { it.length }.maxOrNull()!! + 1 }

    sb.append("|")
    for (col in header.indices) {
        sb.append(header[col].padEnd(columnLengths[col]) + "|")
    }
    sb.appendLine()
    sb.append("|")
    for (colLength in columnLengths) {
        for (i in 1..colLength) sb.append('-')
        sb.append("|")
    }
    sb.appendLine()

    for(row in 0 until outputRows){
        sb.append("|")
        for(col in output.indices){
            sb.append(output[col][row].padEnd(columnLengths[col]) + "|")
        }
        sb.appendLine()
    }
    if(nrow() > limit)
        sb.appendLine("...")
    return sb.toString()
}

internal fun AnyRow.renderToString(): String{
    if(isEmpty()) return ""
    return owner.columns().map {it.name() to it[index]}.filter{it.second != null}
        .map { "${it.first}:${renderValue(it.second)}" }.joinToString(prefix = "{ ", postfix = " }")
}


internal fun renderValue(value: Any?) =
    when(value) {
        is AnyFrame -> when{
            value.isEmpty() -> ""
            value.nrow() == 1 -> value[0].toString()
            else -> "${value.nrow()} rows"
        }
        is Double -> value.format(6)
        else -> escapeHTML(value.toString())
    }

fun Double.format(digits: Int) = "%.${digits}f".format(this)