package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.SingleColumn
import org.jetbrains.dataframe.impl.DataFrameReceiver
import org.jetbrains.dataframe.impl.TreeNode
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumnWithPath
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.getOrPut
import kotlin.reflect.KProperty

public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, selector: ColumnsSelector<T, *>): DataFrame<T> = move(selector).to(newColumnIndex)
public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, cols: Iterable<Column>): DataFrame<T> = moveTo(newColumnIndex) { cols.toColumnSet() }
public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg cols: String): DataFrame<T> = moveTo(newColumnIndex) { cols.toColumns() }
public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg cols: KProperty<*>): DataFrame<T> = moveTo(newColumnIndex) { cols.toColumns() }
public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg cols: Column): DataFrame<T> = moveTo(newColumnIndex) { cols.toColumns() }

public fun <T> DataFrame<T>.moveToLeft(selector: ColumnsSelector<T, *>): DataFrame<T> = move(selector).toLeft()
public fun <T> DataFrame<T>.moveToLeft(cols: Iterable<Column>): DataFrame<T> = moveToLeft { cols.toColumnSet() }
public fun <T> DataFrame<T>.moveToLeft(vararg cols: String): DataFrame<T> = moveToLeft { cols.toColumns() }
public fun <T> DataFrame<T>.moveToLeft(vararg cols: Column): DataFrame<T> = moveToLeft { cols.toColumns() }
public fun <T> DataFrame<T>.moveToLeft(vararg cols: KProperty<*>): DataFrame<T> = moveToLeft { cols.toColumns() }

public fun <T> DataFrame<T>.moveToRight(cols: ColumnsSelector<T, *>): DataFrame<T> = move(cols).toRight()
public fun <T> DataFrame<T>.moveToRight(cols: Iterable<Column>): DataFrame<T> = moveToRight { cols.toColumnSet() }
public fun <T> DataFrame<T>.moveToRight(vararg cols: String): DataFrame<T> = moveToRight { cols.toColumns() }
public fun <T> DataFrame<T>.moveToRight(vararg cols: Column): DataFrame<T> = moveToRight { cols.toColumns() }
public fun <T> DataFrame<T>.moveToRight(vararg cols: KProperty<*>): DataFrame<T> = moveToRight { cols.toColumns() }

public fun <T, C> DataFrame<T>.move(cols: Iterable<ColumnReference<C>>): MoveColsClause<T, C> = move { cols.toColumnSet() }
public fun <T, C> DataFrame<T>.move(vararg cols: ColumnReference<C>): MoveColsClause<T, C> = move { cols.toColumns() }
public fun <T> DataFrame<T>.move(vararg cols: String): MoveColsClause<T, Any?> = move { cols.toColumns() }
public fun <T> DataFrame<T>.move(vararg cols: ColumnPath): MoveColsClause<T, Any?> = move { cols.toColumns() }
public fun <T, C> DataFrame<T>.move(vararg cols: KProperty<C>): MoveColsClause<T, C> = move { cols.toColumns() }
public fun <T, C> DataFrame<T>.move(selector: ColumnsSelector<T, C>): MoveColsClause<T, C> {
    val (df, removed) = doRemove(selector)
    return MoveColsClause(df, this, removed)
}

public interface DataFrameForMove<T> : DataFrameBase<T> {

    public fun path(vararg columns: String): List<String> = listOf(*columns)

    public fun SingleColumn<*>.addPath(columnPath: ColumnPath): ColumnPath

    public operator fun SingleColumn<*>.plus(column: String): List<String> = addPath(listOf(column))
}

internal class MoveReceiver<T>(df: DataFrame<T>) : DataFrameReceiver<T>(df, false), DataFrameForMove<T> {

    override fun SingleColumn<*>.addPath(columnPath: ColumnPath): ColumnPath {
        return this.resolveSingle(this@MoveReceiver, UnresolvedColumnsPolicy.Create)!!.path + columnPath
    }
}

public fun <T, C> MoveColsClause<T, C>.under(parentPath: DataFrameForMove<T>.(ColumnWithPath<C>) -> List<String>): DataFrame<T> {
    val receiver = MoveReceiver(df)
    val columnsToInsert = removed.map {
        val col = it.column
        ColumnToInsert(parentPath(receiver, col) + it.name, col.data, it)
    }
    return df.insert(columnsToInsert)
}

public fun <T, C> MoveColsClause<T, C>.toTop(
    groupNameExpression: DataFrameForMove<T>.(ColumnWithPath<C>) -> String = { it.name() }
): DataFrame<T> =
    into { listOf(groupNameExpression(it)) }

public fun <T, C> MoveColsClause<T, C>.intoIndexed(
    newPathExpression: DataFrameForMove<T>.(ColumnWithPath<C>, Int) -> ColumnPath
): DataFrame<T> {
    var counter = 0
    return into { col ->
        newPathExpression(this, col, counter++)
    }
}

public fun <T, C> MoveColsClause<T, C>.into(newPathExpression: DataFrameForMove<T>.(ColumnWithPath<C>) -> ColumnPath): DataFrame<T> {
    val receiver = MoveReceiver(df)
    val columnsToInsert = removed.map {
        val col = it.column
        ColumnToInsert(newPathExpression(receiver, col), col.data, it)
    }
    return df.insert(columnsToInsert)
}

public fun <T, C> MoveColsClause<T, C>.into(vararg path: String): DataFrame<T> = into(path.toList())
public fun <T, C> MoveColsClause<T, C>.into(path: ColumnPath): DataFrame<T> = into { path }

public fun <T, C> MoveColsClause<T, C>.under(vararg path: String): DataFrame<T> = under(path.toList())
public fun <T, C> MoveColsClause<T, C>.under(path: ColumnPath): DataFrame<T> = under { path }
public fun <T, C> MoveColsClause<T, C>.under(groupRef: MapColumnReference): DataFrame<T> = under(groupRef.path())

public fun <T, C> MoveColsClause<T, C>.to(columnIndex: Int): DataFrame<T> {
    val newColumnList = df.columns().subList(0, columnIndex) + removed.map { it.data.column as DataColumn<C> } + df.columns().subList(columnIndex, df.ncol())
    return newColumnList.asDataFrame()
}

public fun <T, C> MoveColsClause<T, C>.after(columnPath: ColumnPath): DataFrame<T> = after { columnPath.toColumnDef() }
public fun <T, C> MoveColsClause<T, C>.after(column: Column): DataFrame<T> = after { column }
public fun <T, C> MoveColsClause<T, C>.after(column: KProperty<*>): DataFrame<T> = after { column.toColumnDef() }
public fun <T, C> MoveColsClause<T, C>.after(column: String): DataFrame<T> = after { column.toColumnDef() }
public fun <T, C> MoveColsClause<T, C>.after(column: ColumnSelector<T, *>): DataFrame<T> = afterOrBefore(column, true)

/*
fun <T, C> MoveColsClause<T, C>.before(columnPath: ColumnPath) = before { columnPath.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.before(column: Column) = before { column }
fun <T, C> MoveColsClause<T, C>.before(column: KProperty<*>) = before { column.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.before(column: String) = before { column.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.before(column: ColumnSelector<T, *>) = afterOrBefore(column, false)
*/

// TODO: support 'before' mode
public fun <T, C> MoveColsClause<T, C>.afterOrBefore(column: ColumnSelector<T, *>, isAfter: Boolean): DataFrame<T> {
    val refPath = originalDf.getColumnWithPath(column).path
    val removeRoot = removed.first().getRoot()

    val refNode = removeRoot.getOrPut(refPath) {
        val parent = originalDf.getFrame(it.dropLast(1))
        val index = parent.getColumnIndex(it.last())
        val col = df.column(index)
        ColumnPosition(index, false, col)
    }

    val parentPath = refPath.dropLast(1)

    val toInsert = removed.map {
        val path = parentPath + it.name
        ColumnToInsert(path, it.column.data, refNode)
    }
    return df.insert(toInsert)
}

public fun <T, C> MoveColsClause<T, C>.toLeft(): DataFrame<T> = to(0)
public fun <T, C> MoveColsClause<T, C>.toRight(): DataFrame<T> = to(df.ncol())
public class MoveColsClause<T, C> internal constructor(
    internal val df: DataFrame<T>,
    internal val originalDf: DataFrame<T>,
    internal val removed: List<TreeNode<ColumnPosition>>
) {

    internal val TreeNode<ColumnPosition>.column get() = toColumnWithPath<C>(originalDf)
}
