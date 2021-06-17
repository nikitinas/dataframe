package org.jetbrains.dataframe

import org.jetbrains.dataframe.impl.ColumnNameGenerator
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns

internal val defaultSeparator: CharSequence = "_"

fun <T> DataFrame<T>.flatten(separator: CharSequence = defaultSeparator) = flatten(separator) { all() }

fun <T, C> DataFrame<T>.flatten(
    separator: CharSequence = defaultSeparator,
    selector: ColumnsSelector<T, C>
): DataFrame<T> {

    val rootColumns = getColumnsWithPaths { selector.toColumns().filter { it.isGroup() }.top() }
    val rootPrefixes = rootColumns.map { it.path }.toSet()
    val nameGenerator = ColumnNameGenerator()

    fun getRootPrefix(path: ColumnPath) =
        (1 until path.size).asSequence().map { path.subList(0, it) }.first { rootPrefixes.contains(it) }

    val result = move { rootColumns.toColumnSet().dfs { !it.isGroup() } }
        .into {
            val prefix = getRootPrefix(it.path).dropLast(1)
            val desiredName = it.path.drop(prefix.size).joinToString(separator)
            val name = nameGenerator.addUnique(desiredName)
            prefix + name
        }
    return result
}