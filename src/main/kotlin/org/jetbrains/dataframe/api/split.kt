package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.impl.ColumnDataCollector
import org.jetbrains.dataframe.impl.columns.toColumnWithPath
import org.jetbrains.dataframe.impl.createDataCollector
import org.jetbrains.dataframe.impl.nameGenerator
import kotlin.reflect.KProperty
import kotlin.reflect.KType

fun <T, C> DataFrame<T>.split(selector: ColumnsSelector<T, C?>): Split<T, C> =
    SplitClause(this, selector)

fun <T> DataFrame<T>.split(column: String) = split { column.toColumnDef() }
fun <T, C> DataFrame<T>.split(column: ColumnReference<C?>) = split { column }
fun <T, C> DataFrame<T>.split(column: KProperty<C?>) = split { column.toColumnDef() }

interface Split<out T, out C> {

    fun by(vararg delimiters: String, trim: Boolean = true, ignoreCase: Boolean = false, limit: Int = 0) = with {
        it.toString().split(*delimiters, ignoreCase = ignoreCase, limit = limit).let {
            if (trim) it.map { it.trim() }
            else it
        }
    }
}

typealias ColumnNamesGenerator<C> = ColumnWithPath<C>.(extraColumnIndex: Int) -> String

interface SplitWithTransform<out T, out C, in R> {

    fun intoRows(dropEmpty: Boolean = true): DataFrame<T>

    fun inplace(): DataFrame<T>

    fun inward(vararg names: String, extraNamesGenerator: ColumnNamesGenerator<C>? = null) = inward(names.toList(), extraNamesGenerator)

    fun inward(names: Iterable<String>, extraNamesGenerator: ColumnNamesGenerator<C>? = null): DataFrame<T>
}

class SplitClause<T, C>(
    val df: DataFrame<T>,
    val columns: ColumnsSelector<T, C?>
): Split<T, C>

inline fun <T, C, reified R> Split<T, C>.with(noinline splitter: (C) -> Iterable<R>) = with(getType<R>(), splitter)

@PublishedApi
internal fun <T, C, R> Split<T, C>.with(type: KType, splitter: (C) -> Iterable<R>): SplitWithTransform<T, C, R> {
    require(this is SplitClause<T, C>)
    return SplitClauseWithTransform(df, columns, false, type) {
        if(it == null) emptyMany() else splitter(it).toMany()
    }
}

data class SplitClauseWithTransform<T, C, R>(
    val df: DataFrame<T>,
    val columns: ColumnsSelector<T, C?>,
    val inward: Boolean,
    val targetType: KType,
    val transform: (C) -> Iterable<R>
): SplitWithTransform<T, C, R>{

    override fun intoRows(dropEmpty: Boolean) = df.explode(dropEmpty, columns)

    override fun inplace() = df.convert(columns).with(Many::class.createTypeWithArgument(targetType)) { if(it == null) emptyMany() else transform(it).toMany() }

    override fun inward(names: Iterable<String>, extraNamesGenerator: ColumnNamesGenerator<C>?) = copy(inward = true).into(names.toList(), extraNamesGenerator)
}

class FrameSplit<T, C>(
    val df: DataFrame<T>,
    val columns: ColumnSelector<T, DataFrame<C>?>
)


fun <T, C, R> SplitWithTransform<T, C, R>.into(firstName: ColumnReference<*>, vararg otherNames: ColumnReference<*>) =
    into(listOf(firstName.name()) + otherNames.map { it.name() })

fun <T, C, R> SplitWithTransform<T, C, R>.intoMany(namesProvider: (ColumnWithPath<C>, numberOfNewColumns: Int) -> List<String>) =
    doSplitCols(this as SplitClauseWithTransform<T, C, R>, namesProvider)

fun <T, C, R> SplitWithTransform<T, C, R>.into(
    vararg names: String,
    extraNamesGenerator: (ColumnWithPath<C>.(extraColumnIndex: Int) -> String)? = null
) = into(names.toList(), extraNamesGenerator)

fun <T, C, R> SplitWithTransform<T, C, R>.into(
    names: List<String>,
    extraNamesGenerator: (ColumnWithPath<C>.(extraColumnIndex: Int) -> String)? = null
) = intoMany { col, numberOfNewCols ->
    if (extraNamesGenerator != null && names.size < numberOfNewCols)
        names + (1..(numberOfNewCols - names.size)).map { extraNamesGenerator(col, it) }
    else names
}

internal fun valueToList(value: Any?, splitStrings: Boolean = true): List<Any?> = when (value) {
    null -> emptyList()
    is AnyMany -> value
    is AnyFrame -> value.rows().toList()
    else -> if(splitStrings) value.toString().split(",").map { it.trim() } else listOf(value)
}

fun <T, C, R> doSplitCols(
    clause: SplitClauseWithTransform<T, C, R>,
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
        if (names.size < columnCollectors.size)
            names = names + (1..(columnCollectors.size - names.size)).map { "splitted$it" }

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

@JvmName("intoRowsTC")
inline fun <T, C: Iterable<R>, reified R> Split<T, C>.intoRows(dropEmpty: Boolean = true) = with { it }.intoRows(dropEmpty)

@JvmName("intoRowsFrame")
fun <T> Split<T, AnyFrame>.intoRows(dropEmpty: Boolean = true) = with { it.rows() }.intoRows(dropEmpty)

@JvmName("inplaceTC")
inline fun <T, C: Iterable<R>, reified R> Split<T, C>.inplace() = with { it }.inplace()

inline fun <T, C: Iterable<R>, reified R> Split<T, C>.inward(vararg names: String, noinline extraNamesGenerator: ColumnNamesGenerator<C>? = null) =
    with { it }.inward(names.toList(), extraNamesGenerator)

inline fun <T, C: Iterable<R>, reified R> Split<T, C>.into(vararg names: String, noinline extraNamesGenerator: ColumnNamesGenerator<C>? = null) =
    with { it }.into(names.toList(), extraNamesGenerator)

@JvmName("intoTC")
fun <T> Split<T, String>.into(vararg names: String, extraNamesGenerator: (ColumnWithPath<String>.(extraColumnIndex: Int) -> String)? = null) =
    with { it.splitDefault() }.into(names.toList(), extraNamesGenerator)

internal fun String.splitDefault() = split(",").map { it.trim() }