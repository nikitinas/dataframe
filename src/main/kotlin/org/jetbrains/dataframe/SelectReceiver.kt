package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.AnyCol
import org.jetbrains.dataframe.columns.BooleanCol
import org.jetbrains.dataframe.columns.ColumnAccessor
import org.jetbrains.dataframe.impl.columns.ColumnAccessorImpl
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.Columns
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.DoubleCol
import org.jetbrains.dataframe.columns.IntCol
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.NumberCol
import org.jetbrains.dataframe.columns.StringCol
import org.jetbrains.dataframe.columns.name
import org.jetbrains.dataframe.columns.renamedReference
import org.jetbrains.dataframe.impl.columns.ColumnsList
import org.jetbrains.dataframe.impl.columns.asGroup
import org.jetbrains.dataframe.impl.columns.toColumnSet
import org.jetbrains.dataframe.impl.columns.toColumns
import org.jetbrains.dataframe.impl.columns.transform
import org.jetbrains.dataframe.impl.columns.typed
import kotlin.reflect.KProperty
import kotlin.reflect.KType

interface SelectReceiver<out T> : DataFrameBase<T> {

    fun DataFrameBase<*>.first(numCols: Int) = cols().take(numCols)

    fun DataFrameBase<*>.last(numCols: Int) = cols().takeLast(numCols)

    fun DataFrameBase<*>.group(name: String) = this.get(name) as ColumnGroup<*>

    fun <C> Columns<*>.cols(firstCol: ColumnReference<C>, vararg otherCols: ColumnReference<C>) = (listOf(firstCol) + otherCols).let { refs ->
        transform { it.flatMap { col -> refs.mapNotNull { col.getChild(it) } } }
    }

    fun Columns<*>.cols(firstCol: String, vararg otherCols: String) = (listOf(firstCol) + otherCols).let { names ->
        transform { it.flatMap { col -> names.mapNotNull { col.getChild(it) } } }
    }

    fun Columns<*>.cols(vararg indices: Int) = transform { it.flatMap { it.children().let { children -> indices.map { children[it]}} } }
    fun Columns<*>.cols(range: IntRange) = transform { it.flatMap { it.children().subList(range.start, range.endInclusive+1) } }
    fun Columns<*>.cols(predicate: (AnyCol) -> Boolean = {true}) = colsInternal(predicate)

    fun <C> Columns<C>.colsDfs(predicate: (ColumnWithPath<*>) -> Boolean = {true}) = dfsInternal(predicate)

    fun DataFrameBase<*>.all(): Columns<*> = ColumnsList(children())

    fun Columns<*>.allDfs() = colsDfs { !it.isGroup() }

    // excluding current
    fun DataFrameBase<*>.allAfter(colPath: ColumnPath) = children().let { var take = false; it.filter { if(take) true else { take = colPath == it.path; false} } }
    fun DataFrameBase<*>.allAfter(colName: String) = allAfter(listOf(colName))
    fun DataFrameBase<*>.allAfter(column: Column) = allAfter(column.path())

    // including current
    fun DataFrameBase<*>.allSince(colPath: ColumnPath) = children().let { var take = false; it.filter { if(take) true else { take = colPath == it.path; take} } }
    fun DataFrameBase<*>.allSince(colName: String) = allSince(listOf(colName))
    fun DataFrameBase<*>.allSince(column: Column) = allSince(column.path())

    // excluding current
    fun DataFrameBase<*>.allBefore(colPath: ColumnPath) = children().let { var take = true; it.filter { if(!take) false else { take = colPath != it.path; take} } }
    fun DataFrameBase<*>.allBefore(colName: String) = allBefore(listOf(colName))
    fun DataFrameBase<*>.allBefore(column: Column) = allBefore(column.path())

    // including current
    fun DataFrameBase<*>.allUntil(colPath: ColumnPath) = children().let { var take = true; it.filter { if(!take) false else { take = colPath != it.path; true} } }
    fun DataFrameBase<*>.allUntil(colName: String) = allUntil(listOf(colName))
    fun DataFrameBase<*>.allUntil(column: Column) = allUntil(column.path())


    fun DataFrameBase<*>.colGroups(filter: (ColumnGroup<*>) -> Boolean = { true }): Columns<AnyRow> = this.columns().filter { it.isGroup() && filter(it.asGroup()) }.map { it.asGroup() }.toColumnSet()

    fun <C> Columns<C>.children(predicate: (AnyCol) -> Boolean = {true} ) = transform { it.flatMap { it.children().filter { predicate(it.data) } } }

    fun MapColumnReference.children() = transform { it.single().children() }

    operator fun <C> List<DataColumn<C>>.get(range: IntRange):Columns<C> = ColumnsList(subList(range.first, range.last + 1))

    operator fun String.invoke() = toColumnDef()

    fun <C> String.cast(): ColumnAccessor<C> = ColumnAccessorImpl(this)

    fun <C> col(property: KProperty<C>) = property.toColumnDef()

    fun DataFrameBase<*>.col(index: Int) = column(index)
    fun Columns<*>.col(index: Int) = transform { it.mapNotNull { it.getChild(index) } }

    fun DataFrameBase<*>.col(colName: String) = get(colName)
    fun Columns<*>.col(colName: String) = transform { it.mapNotNull { it.getChild(colName) } }

    operator fun Columns<*>.get(colName: String) = col(colName)
    operator fun <C> Columns<*>.get(column: ColumnReference<C>) = cols(column)

    fun <C> Columns<C>.drop(n: Int) = transform { it.drop(n) }
    fun <C> Columns<C>.take(n: Int) = transform { it.take(n) }
    fun <C> Columns<C>.dropLast(n: Int) = transform { it.dropLast(n) }
    fun <C> Columns<C>.takeLast(n: Int) = transform { it.takeLast(n) }
    fun <C> Columns<C>.top() = transform { it.top() }
    fun <C> Columns<C>.takeWhile(predicate: Predicate<ColumnWithPath<C>>) = transform { it.takeWhile(predicate) }
    fun <C> Columns<C>.takeLastWhile(predicate: Predicate<ColumnWithPath<C>>) = transform { it.takeLastWhile(predicate) }
    fun <C> Columns<C>.filter(predicate: Predicate<ColumnWithPath<C>>) = transform { it.filter(predicate) }

    fun Columns<*>.numberCols(filter: (NumberCol) -> Boolean = { true }): Columns<Number?> = colsOf(filter)
    fun Columns<*>.stringCols(filter: (StringCol) -> Boolean = { true }): Columns<String?> = colsOf(filter)
    fun Columns<*>.intCols(filter: (IntCol) -> Boolean = { true }): Columns<Int?> = colsOf(filter)
    fun Columns<*>.doubleCols(filter: (DoubleCol) -> Boolean = { true }): Columns<Double?> = colsOf(filter)
    fun Columns<*>.booleanCols(filter: (BooleanCol) -> Boolean = { true }): Columns<Boolean?> = colsOf(filter)

    fun Columns<*>.nameContains(text: CharSequence) = cols { it.name.contains(text) }
    fun Columns<*>.nameContains(regex: Regex) = cols { it.name.contains(regex) }
    fun Columns<*>.startsWith(prefix: CharSequence) = cols { it.name.startsWith(prefix)}
    fun Columns<*>.endsWith(suffix: CharSequence) = cols { it.name.endsWith(suffix)}

    infix fun <C> Columns<C>.and(other: Columns<C>): Columns<C> = ColumnsList(this, other)

    fun <C> Columns<C>.except(vararg other: Columns<*>) = except(other.toList().toColumnSet())

    fun <C> Columns<C?>.withoutNulls(): Columns<C> = transform { it.filter { !it.hasNulls } } as Columns<C>

    infix fun <C> Columns<C>.except(other: Columns<*>): Columns<*> =
        createColumnSet { resolve(it).allColumnsExcept(other.resolve(it)) }

    infix fun <C> Columns<C>.except(selector: ColumnsSelector<T, *>): Columns<*> = except(selector.toColumns())

    operator fun <C> ColumnSelector<T, C>.invoke() = this(this@SelectReceiver, this@SelectReceiver)
    operator fun <C> ColumnsSelector<T, C>.invoke() = this(this@SelectReceiver, this@SelectReceiver)

    operator fun <C> ColumnReference<C>.invoke(newName: String) = renamedReference(newName)
    infix fun <C> ColumnReference<C>.into(newName: String) = named(newName)
    infix fun <C> ColumnReference<C>.named(newName: String) = renamedReference(newName)

    infix fun String.and(other: String) = toColumnDef() and other.toColumnDef()
    infix fun <C> String.and(other: Columns<C>) = toColumnDef() and other
    infix fun <C> KProperty<C>.and(other: Columns<C>) = toColumnDef() and other
    infix fun <C> Columns<C>.and(other: KProperty<C>) = this and other.toColumnDef()
    infix fun <C> KProperty<C>.and(other: KProperty<C>) = toColumnDef() and other.toColumnDef()
    infix fun <C> Columns<C>.and(other: String) = this and other.toColumnDef()

    operator fun <C> String.invoke(newColumnExpression: RowSelector<T, C>) = newGuessColumn(this, newColumnExpression)

    infix fun <C> String.by(newColumnExpression: RowSelector<T, C>) = newGuessColumn(this, newColumnExpression)

    fun DataFrameBase<*>.string(columnName: String) = getColumn<String>(columnName)
    fun DataFrameBase<*>.int(columnName: String) = getColumn<Int>(columnName)
    fun DataFrameBase<*>.bool(columnName: String) = getColumn<Boolean>(columnName)
    fun DataFrameBase<*>.double(columnName: String) = getColumn<Double>(columnName)
    fun DataFrameBase<*>.long(columnName: String) = getColumn<Long>(columnName)
}

internal fun <T,C> ColumnsSelector<T, C>.filter(predicate: (ColumnWithPath<C>) -> Boolean): ColumnsSelector<T, C> = { this@filter(it, it).filter(predicate) }
//internal fun Columns<*>.filter(predicate: (AnyCol) -> Boolean) = transform { it.filter { predicate(it.data) } }
internal fun Columns<*>.colsInternal(predicate: (AnyCol) -> Boolean) = transform { it.flatMap { it.children().filter { predicate(it.data) } } }
internal fun Columns<*>.dfsInternal(predicate: (ColumnWithPath<*>) -> Boolean) = transform { it.filter { it.isGroup() }.flatMap { it.children().colsDfs().filter(predicate) } }

fun <C> Columns<*>.colsDfsOf(type: KType, predicate: (ColumnWithPath<C>) -> Boolean = { true }) = dfsInternal { it.data.isSubtypeOf(type) && predicate(it.typed()) }
inline fun <reified C> Columns<*>.colsDfsOf(noinline filter: (ColumnWithPath<C>) -> Boolean = { true }) = colsDfsOf(
    getType<C>(), filter)

fun  Columns<*>.colsOf(type: KType): Columns<Any?> = colsOf(type) { true }

fun <C> Columns<*>.colsOf(type: KType, filter: (DataColumn<C>) -> Boolean): Columns<C> = colsInternal { it.isSubtypeOf(type) && filter(it.typed()) } as Columns<C>
inline fun <reified C> Columns<*>.colsOf(noinline filter: (DataColumn<C>) -> Boolean = { true }): Columns<C> = colsOf(getType<C>(), filter)