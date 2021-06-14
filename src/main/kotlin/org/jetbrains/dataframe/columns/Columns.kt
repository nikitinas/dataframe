package org.jetbrains.dataframe.columns

import org.jetbrains.dataframe.ColumnResolutionContext
import org.jetbrains.dataframe.DataFrame
import org.jetbrains.dataframe.UnresolvedColumnsPolicy

/**
 * A set of columns. Used in column selectors API
 * @param C common type of columns
 */
interface Columns<out C> {

    fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>>
}