package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.AnyColumn
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.impl.TreeNode
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.columns.withDf
import kotlin.reflect.KProperty

public infix operator fun <T> DataFrame<T>.minus(column: String): DataFrame<T> = remove(column)
public infix operator fun <T> DataFrame<T>.minus(column: Column): DataFrame<T> = remove(column)
public infix operator fun <T> DataFrame<T>.minus(cols: Iterable<Column>): DataFrame<T> = remove(cols)
public infix operator fun <T> DataFrame<T>.minus(cols: ColumnsSelector<T, *>): DataFrame<T> = remove(cols)

public fun <T> DataFrame<T>.remove(selector: ColumnsSelector<T, *>): DataFrame<T> = doRemove(selector).df
public fun <T> DataFrame<T>.remove(vararg cols: KProperty<*>): DataFrame<T> = remove { cols.toColumns() }
public fun <T> DataFrame<T>.remove(vararg cols: String): DataFrame<T> = remove { cols.toColumns() }
public fun <T> DataFrame<T>.remove(vararg cols: Column): DataFrame<T> = remove { cols.toColumns() }
public fun <T> DataFrame<T>.remove(cols: Iterable<Column>): DataFrame<T> = remove { cols.toColumnSet() }

internal data class RemoveResult<T>(val df: DataFrame<T>, val removedColumns: List<TreeNode<ColumnPosition>>) {

    val removedNothing: Boolean = removedColumns.isEmpty()

    val removeRoot: TreeNode<ColumnPosition>? = removedColumns.firstOrNull()?.getRoot()
}

internal fun <T> DataFrame<T>.doRemove(selector: ColumnsSelector<T, *>): RemoveResult<T> {
    val colPaths = getColumnPaths(selector)
    val originalOrder = colPaths.mapIndexed { index, path -> path to index }.toMap()

    val root = TreeNode.createRoot(ColumnPosition(-1, false, null))

    if (colPaths.isEmpty()) return RemoveResult(this, emptyList())

    fun dfs(cols: Iterable<AnyColumn>, paths: List<ColumnPath>, node: TreeNode<ColumnPosition>): AnyFrame? {
        if (paths.isEmpty()) return null

        val depth = node.depth
        val children = paths.groupBy { it[depth] }
        val newCols = mutableListOf<AnyColumn>()

        cols.forEachIndexed { index, column ->
            val childPaths = children[column.name()]
            if (childPaths != null) {
                val node = node.addChild(column.name, ColumnPosition(index, true, null))
                if (childPaths.all { it.size > depth + 1 }) {
                    val groupCol = (column as ColumnGroup<*>)
                    val newDf = dfs(groupCol.df.columns(), childPaths, node)
                    if (newDf != null) {
                        val newCol = groupCol.withDf(newDf)
                        newCols.add(newCol)
                        node.data.wasRemoved = false
                    }
                } else {
                    node.data.column = column as AnyCol
                }
            } else newCols.add(column)
        }
        if (newCols.isEmpty()) return null
        return newCols.asDataFrame<Unit>()
    }

    val newDf = dfs(columns(), colPaths, root) ?: emptyDataFrame(nrow())

    val removedColumns = root.allRemovedColumns().map { it.pathFromRoot() to it }.sortedBy { originalOrder[it.first] }.map { it.second }

    return RemoveResult(newDf.typed(), removedColumns)
}
