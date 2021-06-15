package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.DataColumn
import org.jetbrains.dataframe.impl.columns.isTable
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure

data class GatherClause<T, C, K, R>(val df: DataFrame<T>, val selector: ColumnsSelector<T, C>, val filter: ((C) -> Boolean)? = null,
                                    val dropNulls: Boolean = true,
                               val nameTransform: ((String) -> K), val valueTransform: ((C) -> R)? = null)

fun <T, C> DataFrame<T>.gather(dropNulls: Boolean = true, selector: ColumnsSelector<T, C?>) = GatherClause<T, C, String, C>(this, selector as ColumnsSelector<T, C>, null, dropNulls, { it }, null)

fun <T, C, K, R> GatherClause<T, C, K, R>.where(filter: Predicate<C>) = copy(filter = filter)

fun <T, C, K, R> GatherClause<T, C, *, R>.mapNames(transform: (String) -> K) = GatherClause(df, selector, filter, dropNulls, transform, valueTransform)
fun <T, C, K, R> GatherClause<T, C, K, *>.map(transform: (C) -> R) = GatherClause(df, selector, filter, dropNulls, nameTransform, transform)

inline fun <T, C, reified K, reified R> GatherClause<T, C, K, R>.into(keyColumn: ColumnReference<String>) = into(keyColumn.name())
inline fun <T, C, reified K, reified R> GatherClause<T, C, K, R>.into(keyColumn: String) = doGather(this, keyColumn, null, getType<K>(), getType<R>())
inline fun <T, C, reified K, reified R> GatherClause<T, C, K, R>.into(keyColumn: String, valueColumn: String) = doGather(this, keyColumn, valueColumn, getType<K>(), getType<R>())

fun <T, C, K, R> doGather(clause: GatherClause<T, C, K, R>, namesTo: String, valuesTo: String? = null, keyColumnType: KType, valueColumnType: KType): DataFrame<T> {

    val removed = clause.df.doRemove(clause.selector)

    val columnsToGather = removed.removedColumns.map { it.data.column as DataColumn<C> }

    val isGatherGroups = columnsToGather.any { it.isGroup() }
    if (isGatherGroups && columnsToGather.any { !it.isGroup() })
        throw UnsupportedOperationException("Cannot mix ColumnGroups with other types of columns in 'gather' operation")

    val keys = columnsToGather.map { clause.nameTransform(it.name()) }

    val namesColumn = column<Many<K>>(namesTo)
    val valuesColumn = column<AnyMany>(valuesTo ?: "newValues")

    var df = removed.df

    var filter = clause.filter
    if(clause.dropNulls && columnsToGather.any { it.hasNulls() }) {
        if(filter == null) filter = { it != null }
        else {
            val oldFilter = filter
            filter = { it != null && oldFilter(it)}
        }
    }

    val valueTransform = clause.valueTransform

    if(filter == null) {

        // optimization when no filter is applied
        val wrappedKeys = keys.toMany()
        df = df.add { // add columns for names and values
            namesColumn by { wrappedKeys }
            valuesColumn by { row ->
                columnsToGather.map { col ->
                    val value = col[row]
                    if(valueTransform != null)
                        when {
                            value is Many<*> -> (value as Many<C>).map(valueTransform)
                            else -> valueTransform(value)
                        }
                    else value
                }.toMany()
            }
        }.explode(namesColumn, valuesColumn) // expand collected names and values
         .explode(valuesColumn) // expand values in Many

    }else {

        val nameAndValue = column<Many<Pair<K, R>>>("nameAndValue")
        df = df.add(nameAndValue) { row ->
            columnsToGather.mapIndexedNotNull { colIndex, col ->
                val value = col[row]
                when {
                    value is Many<*> -> {
                        val filtered = (value as Many<C>).filter(filter).toMany()
                        keys[colIndex] to (valueTransform?.let { filtered.map(it).toMany() } ?: filtered)
                    }
                    filter(value) -> keys[colIndex] to (valueTransform?.invoke(value) ?: value)
                    else -> null
                }
            }.toMany()
        }

        df = df.explode { nameAndValue }

        val nameAndValuePairs = nameAndValue.changeType<Pair<K, C>>()

        df = df.split { nameAndValuePairs }
            .by { listOf(it.first, it.second) }
            .into(namesColumn, valuesColumn)
            .explode(valuesColumn)
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