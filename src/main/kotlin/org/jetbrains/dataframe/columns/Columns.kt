package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnResolutionContext

/**
 * A set of columns. Used in column selectors API
 * @param C common type of columns
 */
public interface Columns<out C> {

    public fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>>
}
