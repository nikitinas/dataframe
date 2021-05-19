package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.and
import org.jetbrains.dataframe.impl.columns.isTable
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure

data class GatherClause<T, C, K, R>(val df: DataFrame<T>, val selector: ColumnsSelector<T, C>, val filter: ((C) -> Boolean)? = null,
                               val nameTransform: ((String) -> K), val valueTransform: ((C) -> R))

fun <T, C> DataFrame<T>.gather(selector: ColumnsSelector<T, C>) = GatherClause(this, selector, null, { it }, { it })
fun <T, C, K, R> GatherClause<T, C, K, R>.where(filter: Predicate<C>) = GatherClause(df, selector, this.filter?.let { it and filter }
        ?: filter,
        nameTransform, valueTransform)

fun <T, C, K, R> GatherClause<T, C, *, R>.mapNames(transform: (String) -> K) = GatherClause(df, selector, filter, transform, valueTransform)
fun <T, C, K, R> GatherClause<T, C, K, *>.map(transform: (C) -> R) = GatherClause(df, selector, filter, nameTransform, transform)
fun <T, C : Any, K, R> GatherClause<T, C?, K, *>.mapNotNull(transform: (C) -> R) = GatherClause(df, selector, filter, nameTransform, { if (it != null) transform(it) else null })

inline fun <T, C, reified K, reified R> GatherClause<T, C, K, R>.into(keyColumn: ColumnReference<String>) = into(keyColumn.name())
inline fun <T, C, reified K, reified R> GatherClause<T, C, K, R>.into(keyColumn: String) = doGather(this, keyColumn, null, getType<K>(), getType<R>())
inline fun <T, C, reified K, reified R> GatherClause<T, C, K, R>.into(keyColumn: String, valueColumn: String) = doGather(this, keyColumn, valueColumn, getType<K>(), getType<R>())

fun <T, C, K, R> doGather(clause: GatherClause<T, C, K, R>, namesTo: String, valuesTo: String? = null, keyColumnType: KType, valueColumnType: KType): DataFrame<T> {

    val removed = clause.df.doRemove(clause.selector)

    val columnsToGather = removed.removedColumns.map { it.data.column as DataColumn<C> }

    val isGatherGroups = columnsToGather.any { it.isGroup() }
    if (isGatherGroups && columnsToGather.any { !it.isGroup() })
        throw UnsupportedOperationException("Cannot mix ColumnGroups with other types of columns in 'gather' operation")

    val gatheredColumnKeys = columnsToGather.map { clause.nameTransform(it.name()) }

    val namesColumn = column<List<K>>(namesTo)
    val valuesColumn = column<List<*>>(valuesTo ?: "newValues")

    var df = removed.df

    val filter = clause.filter

    if(filter == null) {
        df = df.add(namesColumn) {
            gatheredColumnKeys
        }.add(valuesColumn) { row ->
            columnsToGather.map { col ->
                clause.valueTransform(col[row])
            }
        }

        df = df.split { namesColumn and valuesColumn }.intoRows()
    }else {
        val nameAndValue = column<List<Pair<K, R>>>("nameAndValue")
        df = df.add(nameAndValue) { row ->
            columnsToGather.mapIndexedNotNull { colIndex, col ->
                val value = col[row]
                if(filter(value)) {
                    gatheredColumnKeys[colIndex] to clause.valueTransform(value)
                }else null
            }
        }

        df = df.split { nameAndValue }.intoRows()

        val nameAndValuePairs = nameAndValue.changeType<Pair<K, C>?>()

        df = df.split { nameAndValuePairs }.by { it?.let { listOf(it.first, it.second)} ?: listOf(null, null) }.into(namesColumn, valuesColumn)
    }

    df = df.convert(namesColumn.name()).to(keyColumnType)

    val valuesCol = df[valuesColumn.name()]

    if(valuesTo == null) {

        // values column needs to be removed
        if(valuesCol.isGroup()){
            df = df.ungroup(valuesColumn.name())
        }else df = df.remove(valuesColumn.name())

    }else {
        if(!valuesCol.isTable() && valueColumnType.jvmErasure != Any::class){
            df = df.convert(valuesColumn.name()).to(valueColumnType)
        }
    }

    return df
}