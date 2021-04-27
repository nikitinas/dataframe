package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.impl.TreeNode
import org.jetbrains.dataframe.impl.columns.withDf
import org.jetbrains.dataframe.impl.getAncestor

fun <T> DataFrame<T>.insert(path: ColumnPath, column: AnyCol) = insertColumns(this, listOf(ColumnToInsert(path, column)))

internal data class ColumnToInsert(
    val insertionPath: ColumnPath,
    val column: AnyCol,
    val referenceNode: TreeNode<ColumnPosition>? = null
)

internal fun <T> DataFrame<T>.insert(columns: List<ColumnToInsert>) = insertColumns(this, columns)

internal fun <T> insertColumns(df: DataFrame<T>?, columns: List<ColumnToInsert>) =
    insertColumns(df, columns, columns.firstOrNull()?.referenceNode?.getRoot(), 0)

internal fun insertColumns(columns: List<ColumnToInsert>) =
    insertColumns<Unit>(null, columns, columns.firstOrNull()?.referenceNode?.getRoot(), 0)

internal fun <T> insertColumns(
    df: DataFrame<T>?,
    columns: List<ColumnToInsert>,
    treeNode: TreeNode<ColumnPosition>?,
    depth: Int
): DataFrame<T> {

    if (columns.isEmpty()) return df ?: DataFrame.empty().typed()

    val childDepth = depth + 1

    val columnsMap = columns.groupBy { it.insertionPath[depth] }.toMutableMap() // map: columnName -> columnsToAdd

    val newColumns = mutableListOf<AnyCol>()

    // insert new columns under existing
    df?.columns()?.forEach {
        val subTree = columnsMap[it.name()]
        if (subTree != null) {
            // assert that new columns go directly under current column so they have longer paths
            val invalidPath = subTree.firstOrNull { it.insertionPath.size == childDepth }
            assert(invalidPath == null) { "Can't insert column `" + invalidPath!!.insertionPath.joinToString(".") + "`. Column with this path already exists" }
            val group = it as? MapColumn<*>
            assert(group != null) { "Can not insert columns under a column '${it.name()}', because it is not a column group" }
            val newDf = insertColumns(group!!.df, subTree, treeNode?.get(it.name()), childDepth)
            val newCol = group.withDf(newDf)
            newColumns.add(newCol)
            columnsMap.remove(it.name())
        } else newColumns.add(it)
    }

    // collect new columns to insert
    val columnsToAdd = columns.mapNotNull {
        val name = it.insertionPath[depth]
        val subTree = columnsMap[name]
        if (subTree != null) {
            columnsMap.remove(name)

            // look for columns in subtree that were originally located at the current insertion path
            // find the minimal original index among them
            // new column will be inserted at that position
            val minIndex = subTree.minOf {
                if (it.referenceNode == null) Int.MAX_VALUE
                else {
                    var col = it.referenceNode
                    if (col.depth > depth) col = col.getAncestor(depth + 1)
                    if (col.parent === treeNode) {
                        if (col.data.wasRemoved) col.data.originalIndex else col.data.originalIndex + 1
                    } else Int.MAX_VALUE
                }
            }

            minIndex to (name to subTree)
        } else null
    }.sortedBy { it.first } // sort by insertion index

    val removedSiblings = treeNode?.children
    var k = 0 // index in 'removedSiblings' list
    var insertionIndexOffset = 0

    columnsToAdd.forEach { (insertionIndex, pair) ->
        val (name, columns) = pair

        // adjust insertion index by number of columns that were removed before current index
        if (removedSiblings != null) {
            while (k < removedSiblings.size && removedSiblings[k].data.originalIndex < insertionIndex) {
                if (removedSiblings[k].data.wasRemoved) insertionIndexOffset--
                k++
            }
        }

        val nodeToInsert =
            columns.firstOrNull { it.insertionPath.size == childDepth } // try to find existing node to insert
        val newCol = if (nodeToInsert != null) {
            val column = nodeToInsert.column
            if (columns.size > 1) {
                assert(columns.count { it.insertionPath.size == childDepth } == 1) { "Can not insert more than one column into the path ${nodeToInsert.insertionPath}" }
                val group = column as MapColumn<*>
                val newDf = insertColumns(
                    group.df,
                    columns.filter { it.insertionPath.size > childDepth },
                    treeNode?.get(name),
                    childDepth
                )
                group.withDf(newDf)
            } else column.rename(name)
        } else {
            val newDf = insertColumns<Unit>(null, columns, treeNode?.get(name), childDepth)
            DataColumn.create(name, newDf) // new node needs to be created
        }
        if (insertionIndex == Int.MAX_VALUE)
            newColumns.add(newCol)
        else {
            newColumns.add(insertionIndex + insertionIndexOffset, newCol)
            insertionIndexOffset++
        }
    }

    return newColumns.asDataFrame()
}
