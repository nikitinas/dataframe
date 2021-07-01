package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.*
import org.jetbrains.dataframe.impl.columns.*
import org.jetbrains.dataframe.impl.columns.toColumns
import kotlin.reflect.KProperty
import kotlin.reflect.KType

public interface SelectReceiver<out T> : DataFrameBase<T> {

    public fun DataFrameBase<*>.first(numCols: Int): Columns<Any?> = cols().take(numCols)

    public fun DataFrameBase<*>.last(numCols: Int): Columns<Any?> = cols().takeLast(numCols)

    public fun DataFrameBase<*>.group(name: String): ColumnGroup<*> = this.get(name) as ColumnGroup<*>

    public fun <C> Columns<*>.cols(firstCol: ColumnReference<C>, vararg otherCols: ColumnReference<C>): Columns<C> = (listOf(firstCol) + otherCols).let { refs ->
        transform { it.flatMap { col -> refs.mapNotNull { col.getChild(it) } } }
    }

    public fun Columns<*>.cols(firstCol: String, vararg otherCols: String): Columns<Any?> = (listOf(firstCol) + otherCols).let { names ->
        transform { it.flatMap { col -> names.mapNotNull { col.getChild(it) } } }
    }

    public fun Columns<*>.cols(vararg indices: Int): Columns<Any?> = transform { it.flatMap { it.children().let { children -> indices.map { children[it] } } } }
    public fun Columns<*>.cols(range: IntRange): Columns<Any?> = transform { it.flatMap { it.children().subList(range.start, range.endInclusive + 1) } }
    public fun Columns<*>.cols(predicate: (AnyCol) -> Boolean = { true }): Columns<Any?> = colsInternal(predicate)

    public fun <C> Columns<C>.dfs(predicate: (ColumnWithPath<*>) -> Boolean): Columns<Any?> = dfsInternal(predicate)

    public fun DataFrameBase<*>.all(): Columns<*> = ColumnsList(children())

    public fun none(): Columns<*> = ColumnsList<Any?>(emptyList())

    public fun Columns<*>.dfs(): Columns<Any?> = dfs { !it.isGroup() }

    // excluding current
    public fun DataFrameBase<*>.allAfter(colPath: ColumnPath): Columns<Any?> = children().let { var take = false; it.filter { if (take) true else { take = colPath == it.path; false } } }
    public fun DataFrameBase<*>.allAfter(colName: String): Columns<Any?> = allAfter(listOf(colName))
    public fun DataFrameBase<*>.allAfter(column: Column): Columns<Any?> = allAfter(column.path())

    // including current
    public fun DataFrameBase<*>.allSince(colPath: ColumnPath): Columns<Any?> = children().let { var take = false; it.filter { if (take) true else { take = colPath == it.path; take } } }
    public fun DataFrameBase<*>.allSince(colName: String): Columns<Any?> = allSince(listOf(colName))
    public fun DataFrameBase<*>.allSince(column: Column): Columns<Any?> = allSince(column.path())

    // excluding current
    public fun DataFrameBase<*>.allBefore(colPath: ColumnPath): Columns<Any?> = children().let { var take = true; it.filter { if (!take) false else { take = colPath != it.path; take } } }
    public fun DataFrameBase<*>.allBefore(colName: String): Columns<Any?> = allBefore(listOf(colName))
    public fun DataFrameBase<*>.allBefore(column: Column): Columns<Any?> = allBefore(column.path())

    // including current
    public fun DataFrameBase<*>.allUntil(colPath: ColumnPath): Columns<Any?> = children().let { var take = true; it.filter { if (!take) false else { take = colPath != it.path; true } } }
    public fun DataFrameBase<*>.allUntil(colName: String): Columns<Any?> = allUntil(listOf(colName))
    public fun DataFrameBase<*>.allUntil(column: Column): Columns<Any?> = allUntil(column.path())

    public fun DataFrameBase<*>.colGroups(filter: (ColumnGroup<*>) -> Boolean = { true }): Columns<AnyRow> = this.columns().filter { it.isGroup() && filter(it.asGroup()) }.map { it.asGroup() }.toColumnSet()

    public fun <C> Columns<C>.children(predicate: (AnyCol) -> Boolean = { true }): Columns<Any?> = transform { it.flatMap { it.children().filter { predicate(it.data) } } }

    public fun MapColumnReference.children(): Columns<Any?> = transform { it.single().children() }

    public operator fun <C> List<DataColumn<C>>.get(range: IntRange): Columns<C> = ColumnsList(subList(range.first, range.last + 1))

    public operator fun String.invoke(): ColumnAccessor<Any?> = toColumnDef()

    public fun <C> String.cast(): ColumnAccessor<C> = ColumnAccessorImpl(this)

    public fun <C> col(property: KProperty<C>): ColumnAccessor<C> = property.toColumnDef()

    public fun DataFrameBase<*>.col(index: Int): DataColumn<*> = column(index)
    public fun Columns<*>.col(index: Int): Columns<Any?> = transform { it.mapNotNull { it.getChild(index) } }

    public fun DataFrameBase<*>.col(colName: String): DataColumn<*> = get(colName)
    public fun Columns<*>.col(colName: String): Columns<Any?> = transform { it.mapNotNull { it.getChild(colName) } }

    public operator fun Columns<*>.get(colName: String): Columns<Any?> = col(colName)
    public operator fun <C> Columns<*>.get(column: ColumnReference<C>): Columns<C> = cols(column)

    public fun <C> Columns<C>.drop(n: Int): Columns<C> = transform { it.drop(n) }
    public fun <C> Columns<C>.take(n: Int): Columns<C> = transform { it.take(n) }
    public fun <C> Columns<C>.dropLast(n: Int): Columns<C> = transform { it.dropLast(n) }
    public fun <C> Columns<C>.takeLast(n: Int): Columns<C> = transform { it.takeLast(n) }
    public fun <C> Columns<C>.top(): Columns<C> = transform { it.top() }
    public fun <C> Columns<C>.takeWhile(predicate: Predicate<ColumnWithPath<C>>): Columns<C> = transform { it.takeWhile(predicate) }
    public fun <C> Columns<C>.takeLastWhile(predicate: Predicate<ColumnWithPath<C>>): Columns<C> = transform { it.takeLastWhile(predicate) }
    public fun <C> Columns<C>.filter(predicate: Predicate<ColumnWithPath<C>>): Columns<C> = transform { it.filter(predicate) }

    public fun Columns<*>.numberCols(filter: (NumberCol) -> Boolean = { true }): Columns<Number?> = colsOf(filter)
    public fun Columns<*>.stringCols(filter: (StringCol) -> Boolean = { true }): Columns<String?> = colsOf(filter)
    public fun Columns<*>.intCols(filter: (IntCol) -> Boolean = { true }): Columns<Int?> = colsOf(filter)
    public fun Columns<*>.doubleCols(filter: (DoubleCol) -> Boolean = { true }): Columns<Double?> = colsOf(filter)
    public fun Columns<*>.booleanCols(filter: (BooleanCol) -> Boolean = { true }): Columns<Boolean?> = colsOf(filter)

    public fun Columns<*>.nameContains(text: CharSequence): Columns<Any?> = cols { it.name.contains(text) }
    public fun Columns<*>.nameContains(regex: Regex): Columns<Any?> = cols { it.name.contains(regex) }
    public fun Columns<*>.startsWith(prefix: CharSequence): Columns<Any?> = cols { it.name.startsWith(prefix) }
    public fun Columns<*>.endsWith(suffix: CharSequence): Columns<Any?> = cols { it.name.endsWith(suffix) }

    public infix fun <C> Columns<C>.and(other: Columns<C>): Columns<C> = ColumnsList(this, other)

    public fun <C> Columns<C>.except(vararg other: Columns<*>): Columns<*> = except(other.toColumns())
    public fun <C> Columns<C>.except(vararg other: String): Columns<*> = except(other.toColumns())

    public fun <C> Columns<C?>.withoutNulls(): Columns<C> = transform { it.filter { !it.hasNulls } } as Columns<C>

    public infix fun <C> Columns<C>.except(other: Columns<*>): Columns<*> =
        createColumnSet { resolve(it).allColumnsExcept(other.resolve(it)) }

    public infix fun <C> Columns<C>.except(selector: ColumnsSelector<T, *>): Columns<C> = except(selector.toColumns()) as Columns<C>

    public operator fun <C> ColumnSelector<T, C>.invoke(): ColumnReference<C> = this(this@SelectReceiver, this@SelectReceiver)
    public operator fun <C> ColumnsSelector<T, C>.invoke(): Columns<C> = this(this@SelectReceiver, this@SelectReceiver)

    public operator fun <C> ColumnReference<C>.invoke(newName: String): ColumnReference<C> = renamedReference(newName)
    public infix fun <C> ColumnReference<C>.into(newName: String): ColumnReference<C> = named(newName)
    public infix fun <C> ColumnReference<C>.named(newName: String): ColumnReference<C> = renamedReference(newName)

    public infix fun String.and(other: String): Columns<Any?> = toColumnDef() and other.toColumnDef()
    public infix fun <C> String.and(other: Columns<C>): Columns<Any?> = toColumnDef() and other
    public infix fun <C> KProperty<C>.and(other: Columns<C>): Columns<C> = toColumnDef() and other
    public infix fun <C> Columns<C>.and(other: KProperty<C>): Columns<C> = this and other.toColumnDef()
    public infix fun <C> KProperty<C>.and(other: KProperty<C>): Columns<C> = toColumnDef() and other.toColumnDef()
    public infix fun <C> Columns<C>.and(other: String): Columns<Any?> = this and other.toColumnDef()

    public operator fun <C> String.invoke(newColumnExpression: RowSelector<T, C>): DataColumn<C> = newColumnWithActualType(this, newColumnExpression)

    public infix fun <C> String.by(newColumnExpression: RowSelector<T, C>): DataColumn<C> = newColumnWithActualType(this, newColumnExpression)

    public fun DataFrameBase<*>.string(columnName: String): DataColumn<String> = getColumn(columnName)
    public fun DataFrameBase<*>.int(columnName: String): DataColumn<Int> = getColumn(columnName)
    public fun DataFrameBase<*>.bool(columnName: String): DataColumn<Boolean> = getColumn(columnName)
    public fun DataFrameBase<*>.double(columnName: String): DataColumn<Double> = getColumn(columnName)
    public fun DataFrameBase<*>.long(columnName: String): DataColumn<Long> = getColumn(columnName)
    public fun DataFrameBase<*>.nint(columnName: String): DataColumn<Int?> = getColumn(columnName)
    public fun DataFrameBase<*>.nstring(columnName: String): DataColumn<String?> = getColumn(columnName)
    public fun DataFrameBase<*>.ndouble(columnName: String): DataColumn<Double?> = getColumn(columnName)
    public fun DataFrameBase<*>.nbool(columnName: String): DataColumn<Boolean?> = getColumn(columnName)
    public fun DataFrameBase<*>.nlong(columnName: String): DataColumn<Long?> = getColumn(columnName)
}

public inline fun <T, reified R> SelectReceiver<T>.expr(name: String = "", useActualType: Boolean = false, noinline expression: AddExpression<T, R>): DataColumn<R> = newColumn(name, useActualType, expression)

internal fun <T, R> SelectReceiver<T>.exprWithActualType(name: String = "", expression: AddExpression<T, R>): DataColumn<R> = newColumnWithActualType(name, expression)

internal fun <T,C> ColumnsSelector<T, C>.filter(predicate: (ColumnWithPath<C>) -> Boolean): ColumnsSelector<T, C> = { this@filter(it, it).filter(predicate) }
//internal fun Columns<*>.filter(predicate: (AnyCol) -> Boolean) = transform { it.filter { predicate(it.data) } }

internal fun Columns<*>.colsInternal(predicate: (AnyCol) -> Boolean) = transform { it.flatMap { it.children().filter { predicate(it.data) } } }
internal fun Columns<*>.dfsInternal(predicate: (ColumnWithPath<*>) -> Boolean) = transform { it.filter { it.isGroup() }.flatMap { it.children().dfs().filter(predicate) } }

public fun <C> Columns<*>.dfsOf(type: KType, predicate: (ColumnWithPath<C>) -> Boolean = { true }): Columns<*> = dfsInternal { it.data.hasElementsOfType(type) && predicate(it.typed()) }
public inline fun <reified C> Columns<*>.dfsOf(noinline filter: (ColumnWithPath<C>) -> Boolean = { true }): Columns<C> = dfsOf(
    getType<C>(),
    filter
) as Columns<C>

public fun Columns<*>.colsOf(type: KType): Columns<Any?> = colsOf(type) { true }

public fun <C> Columns<*>.colsOf(type: KType, filter: (DataColumn<C>) -> Boolean): Columns<C> = colsInternal { it.hasElementsOfType(type) && filter(it.typed()) } as Columns<C>
public inline fun <reified C> Columns<*>.colsOf(noinline filter: (DataColumn<C>) -> Boolean = { true }): Columns<C> = colsOf(getType<C>(), filter)
