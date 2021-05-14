package org.jetbrains.dataframe.io

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.ColumnNameGenerator
import org.jetbrains.dataframe.impl.asList
import org.jetbrains.dataframe.impl.createDataCollector
import org.jetbrains.dataframe.columns.type
import org.jetbrains.dataframe.columns.values
import java.io.File
import java.lang.StringBuilder
import java.net.URL
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.withNullability

fun DataFrame.Companion.readJSON(file: File) = readJSON(file.toURI().toURL())

fun DataFrame.Companion.readJSON(path: String): AnyFrame {
    val url = when {
        isURL(path) -> URL(path).toURI()
        else -> File(path).toURI()
    }
    return readJSON(url.toURL())
}

@Suppress("UNCHECKED_CAST")
fun DataFrame.Companion.readJSON(url: URL): AnyFrame =
        readJSON(Parser.default().parse(url.openStream()))

fun DataFrame.Companion.readJsonStr(text: String) = readJSON(Parser.default().parse(StringBuilder(text)))

private fun readJSON(parsed: Any?) = when (parsed) {
    is JsonArray<*> -> fromList(parsed.value)
    else -> fromList(listOf(parsed))
}

private val arrayColumnName = "array"

internal val valueColumnName = "value"

internal fun fromList(records: List<*>): AnyFrame {

    fun AnyFrame.isSingleUnnamedColumn() = ncol == 1 && column(0).name.let { it  == valueColumnName || it == arrayColumnName }

    var hasPrimitive = false
    var hasArray = false
    // list element type can be JsonObject, JsonArray or primitive
    val nameGenerator = ColumnNameGenerator()
    records.forEach {
                when (it) {
                    is JsonObject -> it.entries.forEach {
                        nameGenerator.addIfAbsent(it.key)
                    }
                    is JsonArray<*> -> hasArray = true
                    null -> {}
                    else -> hasPrimitive = true
                }
            }

    val valueColumn = if(hasPrimitive){
        nameGenerator.addUnique(valueColumnName)
    }else valueColumnName

    val arrayColumn = if(hasArray){
        nameGenerator.addUnique(arrayColumnName)
    }else arrayColumnName

    val columns: List<AnyCol> = nameGenerator.names.map { colName ->
        when {
            colName == valueColumn -> {
                val collector = createDataCollector(records.size)
                records.forEach {
                    when (it) {
                        is JsonObject -> collector.add(null)
                        is JsonArray<*> -> collector.add(null)
                        else -> collector.add(it)
                    }
                }
                collector.toColumn(colName)
            }
            colName == arrayColumn -> {
                val values = mutableListOf<Any?>()
                val startIndices = ArrayList<Int>()
                records.forEach {
                    startIndices.add(values.size)
                    if (it is JsonArray<*>) values.addAll(it.value)
                }
                val parsed = fromList(values)
                when {
                    parsed.isSingleUnnamedColumn() -> {
                        val col = parsed.column(0)
                        val elementType = col.type
                        val values = col.values.asList().splitByIndices(startIndices.asSequence(), false).toList()
                        val hasNulls = values.any { it == null }
                        DataColumn.create(colName, values, List::class.createType(listOf(KTypeProjection.invariant(elementType))).withNullability(hasNulls))
                    }
                    else -> DataColumn.create(colName, parsed, startIndices, false)
                }
            }
            else -> {
                val values = ArrayList<Any?>(records.size)

                records.forEach {
                    when (it) {
                        is JsonObject -> values.add(it[colName])
                        else -> values.add(null)
                    }
                }

                val parsed = fromList(values)
                when {
                    parsed.ncol == 0 -> DataColumn.create(colName, arrayOfNulls<Any?>(values.size).toList(), getType<Any?>())
                    parsed.isSingleUnnamedColumn() -> parsed.column(0).rename(colName)
                    else -> DataColumn.create(colName, parsed)
                }
            }
        }
    }
    if(columns.isEmpty()) return DataFrame.empty(records.size)
    return columns.toDataFrame()
}

