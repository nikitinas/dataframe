package org.jetbrains.dataframe

public class MergeClause<T, C, R>(
    public val df: DataFrame<T>,
    public val selector: ColumnsSelector<T, C>,
    public val transform: (Iterable<C>) -> R
)

public fun <T, C> DataFrame<T>.merge(selector: ColumnsSelector<T, C>): MergeClause<T, C, Iterable<C>> = MergeClause(this, selector, { it })

public inline fun <T, C, reified R> MergeClause<T, C, R>.into(columnName: String): DataFrame<T> = into(listOf(columnName))

public inline fun <T, C, reified R> MergeClause<T, C, R>.into(columnPath: ColumnPath): DataFrame<T> {
    val grouped = df.move(selector).under(columnPath)
    val res = grouped.update { getColumnGroup(columnPath) }.with {
        transform(it.values().toMany() as Iterable<C>)
    }
    return res
}

public fun <T, C, R> MergeClause<T, C, R>.asStrings(): MergeClause<T, C, String> = by(", ")
public fun <T, C, R> MergeClause<T, C, R>.by(
    separator: CharSequence = ", ",
    prefix: CharSequence = "",
    postfix: CharSequence = "",
    limit: Int = -1,
    truncated: CharSequence = "..."
): MergeClause<T, C, String> =
    MergeClause(df, selector) { it.joinToString(separator = separator, prefix = prefix, postfix = postfix, limit = limit, truncated = truncated) }

public inline fun <T, C, R, reified V> MergeClause<T, C, R>.by(crossinline transform: (R) -> V): MergeClause<T, C, V> = MergeClause(df, selector) { transform(this@by.transform(it)) }
