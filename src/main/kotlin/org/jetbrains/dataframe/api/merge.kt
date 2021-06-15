package org.jetbrains.dataframe

class MergeClause<T, C, R>(val df: DataFrame<T>, val selector: ColumnsSelector<T, C>, val transform: (Iterable<C>) -> R)

fun <T, C> DataFrame<T>.merge(selector: ColumnsSelector<T, C>) = MergeClause(this, selector, { it })

inline fun <T, C, reified R> MergeClause<T, C, R>.into(columnName: String) = into(listOf(columnName))

inline fun <T, C, reified R> MergeClause<T, C, R>.into(columnPath: ColumnPath): DataFrame<T> {
    val grouped = df.move(selector).under(columnPath)
    val res = grouped.update { getColumnGroup(columnPath) }.with {
        transform(it.values().toMany() as Iterable<C>)
    }
    return res
}

fun <T, C, R> MergeClause<T, C, R>.asStrings() = by(", ")
fun <T, C, R> MergeClause<T, C, R>.by(separator: CharSequence = ", ", prefix: CharSequence = "", postfix: CharSequence = "", limit: Int = -1, truncated: CharSequence = "...") =
        MergeClause(df, selector) { it.joinToString(separator = separator, prefix = prefix, postfix = postfix, limit = limit, truncated = truncated) }

inline fun <T, C, R, reified V> MergeClause<T, C, R>.by(crossinline transform: (R) -> V) = MergeClause(df, selector) { transform(this@by.transform(it)) }