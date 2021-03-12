package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.ColumnDataCollector
import org.jetbrains.dataframe.impl.columns.addPath
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.asTable
import org.jetbrains.dataframe.impl.columns.isTable
import org.jetbrains.dataframe.impl.columns.toColumnWithPath
import org.jetbrains.dataframe.impl.createDataCollector
import org.jetbrains.dataframe.impl.nameGenerator
import kotlin.reflect.KProperty

fun <T, C> DataFrame<T>.split(selector: ColumnsSelector<T, C>): SplitClause<T, C> =
    SplitClause(this, selector, false) { it }

fun <T> DataFrame<T>.split(column: String) = split { column.toColumnDef() }
fun <T, C> DataFrame<T>.split(column: ColumnReference<C>) = split { column }
fun <T, C> DataFrame<T>.split(column: KProperty<C>) = split { column.toColumnDef() }

class SplitClause<T, C>(
    val df: DataFrame<T>,
    val columns: ColumnsSelector<T, C>,
    val inward: Boolean,
    val transform: (C) -> Any?
)

fun <T, C> SplitClause<T, C>.by(
    vararg delimiters: String,
    trim: Boolean = true,
    ignoreCase: Boolean = false,
    limit: Int = 0
) = by {
    it?.toString()?.split(*delimiters, ignoreCase = ignoreCase, limit = limit)?.let {
        if (trim) it.map { it.trim() }
        else it
    } ?: emptyList()
}

fun <T, C, K> SplitClause<T, C>.by(splitter: (C) -> List<K>) = SplitClause(df, columns, inward) {
    splitter(it)
}

fun <T, C> SplitClause<T, C>.inward() = SplitClause(df, columns, true, transform)

fun <T, C> SplitClause<T, C>.into(firstName: ColumnReference<*>, vararg otherNames: ColumnReference<*>) =
    into(listOf(firstName.name()) + otherNames.map { it.name() })

fun <T, C> SplitClause<T, C>.intoMany(namesProvider: (ColumnWithPath<C>, numberOfNewColumns: Int) -> List<String>) =
    doSplitCols(this, namesProvider)

fun <T, C> SplitClause<T, C>.into(vararg names: String, extraNamesGenerator: (ColumnWithPath<C>.(extraColumnIndex: Int) -> String)? = null) = into(names.toList(), extraNamesGenerator)

fun <T, C> SplitClause<T, C>.into(names: List<String>, extraNamesGenerator: (ColumnWithPath<C>.(extraColumnIndex: Int) -> String)? = null) = intoMany { col, numberOfNewCols ->
    if(extraNamesGenerator != null && names.size < numberOfNewCols)
        names + (1 .. (numberOfNewCols-names.size)).map { extraNamesGenerator(col, it) }
    else names
}

internal fun valueToList(value: Any?) = when (value) {
    null -> emptyList()
    is List<*> -> value
    else -> value.toString().split(",").map {it.trim()}
}

fun <T, C> doSplitCols(
    clause: SplitClause<T, C>,
    columnNamesGenerator: ColumnWithPath<C>.(Int) -> List<String>
): DataFrame<T> {

    val nameGenerator = clause.df.nameGenerator()
    val nrow = clause.df.nrow()

    val removeResult = clause.df.doRemove(clause.columns)

    val toInsert = removeResult.removedColumns.flatMap { node ->

        val column = node.toColumnWithPath<C>(clause.df)
        val columnCollectors = mutableListOf<ColumnDataCollector>()
        for (row in 0 until nrow) {
            val value = clause.transform(column.data[row])
            val list = valueToList(value)
            for (j in list.indices) {
                if (columnCollectors.size <= j) {
                    val collector = createDataCollector(nrow)
                    repeat(row) { collector.add(null) }
                    columnCollectors.add(collector)
                }
                columnCollectors[j].add(list[j])
            }
            for (j in list.size until columnCollectors.size)
                columnCollectors[j].add(null)
        }

        var names = columnNamesGenerator(column, columnCollectors.size)
        if(names.size < columnCollectors.size)
            names = names + (1..(columnCollectors.size - names.size)).map {"splitted$it" }

        columnCollectors.mapIndexed { i, col ->

            val name = nameGenerator.addUnique(names[i])
            val sourcePath = node.pathFromRoot()
            val path = if (clause.inward) sourcePath + name else sourcePath.dropLast(1) + name
            val data = col.toColumn(name)
            ColumnToInsert(path, node, data)
        }
    }

    return removeResult.df.doInsert(toInsert)
}


fun <T, C> SplitClause<T, C>.intoRows() = doSplitRows(
    df,
    df.getColumnsWithPaths(columns).map { it.data.map { valueToList(transform(it)) }.addPath(it.path, df) })

