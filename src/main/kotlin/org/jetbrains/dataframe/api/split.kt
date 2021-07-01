package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.ColumnDataCollector
import org.jetbrains.dataframe.impl.columns.toColumnWithPath
import org.jetbrains.dataframe.impl.createDataCollector
import org.jetbrains.dataframe.impl.nameGenerator
import kotlin.reflect.KProperty

public fun <T, C> DataFrame<T>.split(selector: ColumnsSelector<T, C>): SplitClause<T, C> =
    SplitClause(this, selector, false) { it }

public fun <T> DataFrame<T>.split(column: String): SplitClause<T, Any?> = split { column.toColumnDef() }
public fun <T, C> DataFrame<T>.split(column: ColumnReference<C>): SplitClause<T, C> = split { column }
public fun <T, C> DataFrame<T>.split(column: KProperty<C>): SplitClause<T, C> = split { column.toColumnDef() }

public class SplitClause<T, C>(
    public val df: DataFrame<T>,
    public val columns: ColumnsSelector<T, C>,
    public val inward: Boolean,
    public val transform: (C) -> Any?
)

public fun <T, C> SplitClause<T, C>.by(
    vararg delimiters: String,
    trim: Boolean = true,
    ignoreCase: Boolean = false,
    limit: Int = 0
): SplitClause<T, C> = by {
    it?.toString()?.split(*delimiters, ignoreCase = ignoreCase, limit = limit)?.let {
        if (trim) it.map { it.trim() }
        else it
    } ?: emptyList()
}

public fun <T, C, K> SplitClause<T, C>.by(splitter: (C) -> List<K>): SplitClause<T, C> = SplitClause(df, columns, inward) {
    splitter(it).toMany()
}

public fun <T, C> SplitClause<T, C>.inward(): SplitClause<T, C> = SplitClause(df, columns, true, transform)

public fun <T, C> SplitClause<T, C>.into(firstName: ColumnReference<*>, vararg otherNames: ColumnReference<*>): DataFrame<T> =
    into(listOf(firstName.name()) + otherNames.map { it.name() })

public fun <T, C> SplitClause<T, C>.intoMany(
    namesProvider: (ColumnWithPath<C>, numberOfNewColumns: Int) -> List<String>
): DataFrame<T> =
    doSplitCols(this, namesProvider)

public fun <T, C> SplitClause<T, C>.into(
    vararg names: String,
    extraNamesGenerator: (ColumnWithPath<C>.(extraColumnIndex: Int) -> String)? = null
): DataFrame<T> = into(names.toList(), extraNamesGenerator)

public fun <T, C> SplitClause<T, C>.into(
    names: List<String>,
    extraNamesGenerator: (ColumnWithPath<C>.(extraColumnIndex: Int) -> String)? = null
): DataFrame<T> = intoMany { col, numberOfNewCols ->
    if (extraNamesGenerator != null && names.size < numberOfNewCols) {
        names + (1..(numberOfNewCols - names.size)).map { extraNamesGenerator(col, it) }
    } else names
}

internal fun valueToList(value: Any?, splitStrings: Boolean = true): List<Any?> = when (value) {
    null -> emptyList()
    is AnyMany -> value
    is AnyFrame -> value.rows().toList()
    else -> if (splitStrings) value.toString().split(",").map { it.trim() } else listOf(value)
}

public fun <T, C> doSplitCols(
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
        if (names.size < columnCollectors.size) {
            names = names + (1..(columnCollectors.size - names.size)).map { "splitted$it" }
        }

        columnCollectors.mapIndexed { i, col ->

            val name = nameGenerator.addUnique(names[i])
            val sourcePath = node.pathFromRoot()
            val path = if (clause.inward) sourcePath + name else sourcePath.dropLast(1) + name
            val data = col.toColumn(name)
            ColumnToInsert(path, data, node)
        }
    }

    return removeResult.df.insert(toInsert)
}

public fun <T, C> SplitClause<T, C>.intoRows(dropEmpty: Boolean = true): DataFrame<T> = df.explode(dropEmpty, columns)
