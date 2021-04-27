package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.SingleColumn
import org.jetbrains.dataframe.impl.*
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumnWithPath
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty

fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, selector: ColumnsSelector<T, *>) = move(selector).to(newColumnIndex)
fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, cols: Iterable<Column>) = moveTo(newColumnIndex) { cols.toColumnSet() }
fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg cols: String) = moveTo(newColumnIndex) { cols.toColumns() }
fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg cols: KProperty<*>) = moveTo(newColumnIndex) { cols.toColumns() }
fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg cols: Column) = moveTo(newColumnIndex) { cols.toColumns() }

fun <T> DataFrame<T>.moveToLeft(selector: ColumnsSelector<T, *>) = move(selector).toLeft()
fun <T> DataFrame<T>.moveToLeft(cols: Iterable<Column>) = moveToLeft { cols.toColumnSet() }
fun <T> DataFrame<T>.moveToLeft(vararg cols: String) = moveToLeft { cols.toColumns() }
fun <T> DataFrame<T>.moveToLeft(vararg cols: Column) = moveToLeft { cols.toColumns() }
fun <T> DataFrame<T>.moveToLeft(vararg cols: KProperty<*>) = moveToLeft { cols.toColumns() }

fun <T> DataFrame<T>.moveToRight(cols: ColumnsSelector<T, *>) = move(cols).toRight()
fun <T> DataFrame<T>.moveToRight(cols: Iterable<Column>) = moveToRight { cols.toColumnSet() }
fun <T> DataFrame<T>.moveToRight(vararg cols: String) = moveToRight { cols.toColumns() }
fun <T> DataFrame<T>.moveToRight(vararg cols: Column) = moveToRight { cols.toColumns() }
fun <T> DataFrame<T>.moveToRight(vararg cols: KProperty<*>) = moveToRight { cols.toColumns() }

fun <T, C> DataFrame<T>.move(cols: Iterable<ColumnReference<C>>) = move { cols.toColumnSet() }
fun <T, C> DataFrame<T>.move(vararg cols: ColumnReference<C>) = move { cols.toColumns() }
fun <T> DataFrame<T>.move(vararg cols: String) = move { cols.toColumns() }
fun <T, C> DataFrame<T>.move(vararg cols: KProperty<C>) = move { cols.toColumns() }
fun <T, C> DataFrame<T>.move(selector: ColumnsSelector<T, C>): MoveColsClause<T, C> {

    val (df, removed) = doRemove(selector)
    return MoveColsClause(df, this, removed)
}

interface DataFrameForMove<T> : DataFrameBase<T> {

    fun path(vararg columns: String): List<String> = listOf(*columns)

    fun SingleColumn<*>.addPath(vararg columns: String): ColumnPath = this.resolveSingle (ColumnResolutionContext(this@DataFrameForMove, UnresolvedColumnsPolicy.Create))!!.path + listOf(*columns)

    operator fun SingleColumn<*>.plus(column: String) = addPath(column)
}

internal class MoveReceiver<T>(df: DataFrame<T>) : DataFrameReceiver<T>(df, false), DataFrameForMove<T>

fun <T, C> MoveColsClause<T, C>.under(parentPath: DataFrameForMove<T>.(ColumnWithPath<C>) -> List<String>): DataFrame<T> {
    val receiver = MoveReceiver(df)
    val columnsToInsert = removed.map {
        val col = it.column
        ColumnToInsert(parentPath(receiver, col) + it.name, col.data, it)
    }
    return df.insert(columnsToInsert)
}

fun <T, C> MoveColsClause<T, C>.toTop(groupNameExpression: DataFrameForMove<T>.(ColumnWithPath<C>) -> String = { it.name() }) =
    into { listOf(groupNameExpression(it)) }

fun <T, C> MoveColsClause<T, C>.intoIndexed(newPathExpression: DataFrameForMove<T>.(ColumnWithPath<C>, Int) -> ColumnPath): DataFrame<T> {
    var counter = 0
    return into { col ->
        newPathExpression(this, col, counter++)
    }
}

fun <T, C> MoveColsClause<T, C>.into(newPathExpression: DataFrameForMove<T>.(ColumnWithPath<C>) -> ColumnPath): DataFrame<T> {

    val receiver = MoveReceiver(df)
    val columnsToInsert = removed.map {
        val col = it.column
        ColumnToInsert(newPathExpression(receiver, col), col.data, it)
    }
    return df.insert(columnsToInsert)
}

fun <T, C> MoveColsClause<T, C>.into(vararg path: String) = into(path.toList())
fun <T, C> MoveColsClause<T, C>.into(path: ColumnPath) = into { path }

fun <T, C> MoveColsClause<T, C>.under(vararg path: String) = under(path.toList())
fun <T, C> MoveColsClause<T, C>.under(path: ColumnPath) = under { path }
fun <T, C> MoveColsClause<T, C>.under(groupRef: MapColumnReference) = under(groupRef.path())

fun <T, C> MoveColsClause<T, C>.to(columnIndex: Int): DataFrame<T> {
    val newColumnList = df.columns().subList(0, columnIndex) + removed.map { it.data.column as DataColumn<C> } + df.columns().subList(columnIndex, df.ncol())
    return newColumnList.asDataFrame()
}

fun <T, C> MoveColsClause<T, C>.after(columnPath: ColumnPath) = after { columnPath.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.after(column: Column) = after { column }
fun <T, C> MoveColsClause<T, C>.after(column: KProperty<*>) = after { column.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.after(column: String) = after { column.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.after(column: ColumnSelector<T, *>): DataFrame<T> {
    val refCol = originalDf.getColumnWithPath(column)
    val removeRoot = removed.first().getRoot()

    val refNode = removeRoot.getOrPut(refCol.path) {
        val parent = if(it.size > 1) originalDf[it.dropLast(1)].asFrame() else originalDf
        val index = parent.getColumnIndex(it.last())
        val col = parent.column(index)
        ColumnPosition(index, false, col)
    }

    val parentPath = refCol.path.dropLast(1)

    val toInsert = removed.map {
        val path = parentPath + it.name
        ColumnToInsert(path, it.column.data, refNode)
    }
    return df.insert(toInsert)
}

fun <T, C> MoveColsClause<T, C>.toLeft() = to(0)
fun <T, C> MoveColsClause<T, C>.toRight() = to(df.ncol())
class MoveColsClause<T, C> internal constructor(internal val df: DataFrame<T>, internal val originalDf: DataFrame<T>, internal val removed: List<TreeNode<ColumnPosition>>) {

    internal val TreeNode<ColumnPosition>.column get() = toColumnWithPath<C>(originalDf)
}