package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnDefinition
import org.jetbrains.dataframe.columns.ColumnDefinitionImpl
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnSet
import org.jetbrains.dataframe.columns.ColumnWithPath
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.columns.MapColumn
import org.jetbrains.dataframe.columns.isSubtypeOf
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

    fun DataFrameBase<*>.group(name: String) = this.get(name) as MapColumn<*>

    fun <C> ColumnSet<*>.cols(firstCol: ColumnReference<C>, vararg otherCols: ColumnReference<C>) = (listOf(firstCol) + otherCols).let { refs ->
        transform { it.flatMap { col -> refs.mapNotNull { col.getChild(it) } } }
    }

    fun ColumnSet<*>.cols(firstCol: String, vararg otherCols: String) = (listOf(firstCol) + otherCols).let { names ->
        transform { it.flatMap { col -> names.mapNotNull { col.getChild(it) } } }
    }

    fun ColumnSet<*>.cols(range: IntRange) = transform { it.flatMap { it.children().subList(range.start, range.endInclusive+1) } }
    fun ColumnSet<*>.cols(predicate: (AnyCol) -> Boolean = {true}) = colsInternal(predicate)

    fun <C> ColumnSet<C>.colsDfs(predicate: (ColumnWithPath<*>) -> Boolean = {true}) = dfsInternal(predicate)

    fun DataFrameBase<*>.all(): ColumnSet<*> = ColumnsList(children())

    fun DataFrameBase<*>.allDfs() = colsDfs { !it.isGroup() }

    fun DataFrameBase<*>.colGroups(filter: (MapColumn<*>) -> Boolean = { true }): ColumnSet<AnyRow> = this.columns().filter { it.isGroup() && filter(it.asGroup()) }.map { it.asGroup() }.toColumnSet()

    fun <C> ColumnSet<C>.children(predicate: (AnyCol) -> Boolean = {true} ) = transform { it.flatMap { it.children().filter { predicate(it.data) } } }

    fun MapColumnReference.children() = transform { it.single().children() }

    operator fun <C> List<DataColumn<C>>.get(range: IntRange):ColumnSet<C> = ColumnsList(subList(range.first, range.last + 1))

    operator fun String.invoke() = toColumnDef()

    fun <C> String.cast(): ColumnDefinition<C> = ColumnDefinitionImpl(this)

    fun <C> col(property: KProperty<C>) = property.toColumnDef()

    fun DataFrameBase<*>.col(index: Int) = column(index)
    fun ColumnSet<*>.col(index: Int) = transform { it.mapNotNull { it.getChild(index) } }

    fun DataFrameBase<*>.col(colName: String) = get(colName)
    fun ColumnSet<*>.col(colName: String) = transform { it.mapNotNull { it.getChild(colName) } }

    operator fun ColumnSet<*>.get(colName: String) = col(colName)
    operator fun <C> ColumnSet<*>.get(column: ColumnReference<C>) = cols(column)

    fun <C> ColumnSet<C>.drop(n: Int) = transform { it.drop(n) }
    fun <C> ColumnSet<C>.take(n: Int) = transform { it.take(n) }
    fun <C> ColumnSet<C>.dropLast(n: Int) = transform { it.dropLast(n) }
    fun <C> ColumnSet<C>.takeLast(n: Int) = transform { it.takeLast(n) }
    fun <C> ColumnSet<C>.takeWhile(predicate: Predicate<ColumnWithPath<C>>) = transform { it.takeWhile(predicate) }
    fun <C> ColumnSet<C>.takeLastWhile(predicate: Predicate<ColumnWithPath<C>>) = transform { it.takeLastWhile(predicate) }
    fun <C> ColumnSet<C>.filter(predicate: Predicate<ColumnWithPath<C>>) = transform { it.filter(predicate) }

    fun <C> DataColumn<C>.rename(newName: String) = (this as ColumnReference<C>).rename(newName)
    infix fun <C> DataColumn<C>.named(newName: String) = rename(newName)

    fun ColumnSet<*>.numberCols(filter: (NumberCol) -> Boolean = { true }) = colsOf(filter)
    fun ColumnSet<*>.stringCols(filter: (StringCol) -> Boolean = { true }) = colsOf(filter)
    fun ColumnSet<*>.intCols(filter: (IntCol) -> Boolean = { true }) = colsOf(filter)
    fun ColumnSet<*>.doubleCols(filter: (DoubleCol) -> Boolean = { true }) = colsOf(filter)
    fun ColumnSet<*>.booleanCols(filter: (BooleanCol) -> Boolean = { true }) = colsOf(filter)

    fun ColumnSet<*>.nameContains(text: CharSequence) = cols { it.name.contains(text) }
    fun ColumnSet<*>.nameContains(regex: Regex) = cols { it.name.contains(regex) }
    fun ColumnSet<*>.startsWith(prefix: CharSequence) = cols { it.name.startsWith(prefix)}
    fun ColumnSet<*>.endsWith(suffix: CharSequence) = cols { it.name.endsWith(suffix)}

    infix fun <C> ColumnSet<C>.and(other: ColumnSet<C>): ColumnSet<C> = ColumnsList(this, other)

    fun <C> ColumnSet<C>.except(vararg other: ColumnSet<*>) = except(other.toList().toColumnSet())

    fun <C> ColumnSet<C?>.withoutNulls(): ColumnSet<C> = transform { it.filter { !it.hasNulls } } as ColumnSet<C>

    infix fun <C> ColumnSet<C>.except(other: ColumnSet<*>): ColumnSet<*> =
        createColumnSet { resolve(it).allColumnsExcept(other.resolve(it)) }

    infix fun <C> ColumnSet<C>.except(selector: ColumnsSelector<T, *>): ColumnSet<*> = except(selector.toColumns())

    operator fun <C> ColumnSelector<T, C>.invoke() = this(this@SelectReceiver, this@SelectReceiver)

    operator fun <C> ColumnReference<C>.invoke(newName: String) = rename(newName)
    infix fun <C> DataColumn<C>.into(newName: String) = (this as ColumnReference<C>).rename(newName)

    infix fun String.and(other: String) = toColumnDef() and other.toColumnDef()
    infix fun <C> String.and(other: ColumnSet<C>) = toColumnDef() and other
    infix fun <C> KProperty<C>.and(other: ColumnSet<C>) = toColumnDef() and other
    infix fun <C> ColumnSet<C>.and(other: KProperty<C>) = this and other.toColumnDef()
    infix fun <C> KProperty<C>.and(other: KProperty<C>) = toColumnDef() and other.toColumnDef()
    infix fun <C> ColumnSet<C>.and(other: String) = this and other.toColumnDef()
}

internal fun ColumnSet<*>.colsInternal(predicate: (AnyCol) -> Boolean) = transform { it.flatMap { it.children().filter { predicate(it.data) } } }
internal fun ColumnSet<*>.dfsInternal(predicate: (ColumnWithPath<*>) -> Boolean) = transform { it.filter { it.isGroup() }.flatMap { it.children().colsDfs().filter(predicate) } }

fun <C> ColumnSet<*>.colsDfsOf(type: KType, predicate: (ColumnWithPath<C>) -> Boolean = { true }) = dfsInternal { it.data.isSubtypeOf(type) && predicate(it.typed()) }
inline fun <reified C> ColumnSet<*>.colsDfsOf(noinline filter: (ColumnWithPath<C>) -> Boolean = { true }) = colsDfsOf(
    getType<C>(), filter)

fun <C> ColumnSet<*>.colsOf(type: KType, filter: (DataColumn<C>) -> Boolean = { true }): ColumnSet<C> = colsInternal { it.isSubtypeOf(type) && filter(it.typed()) } as ColumnSet<C>
inline fun <reified C> ColumnSet<*>.colsOf(noinline filter: (DataColumn<C>) -> Boolean = { true }) = colsOf(getType<C>(), filter)