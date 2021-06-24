package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

public fun <T> DataFrame<T>.print(): Unit = println(this)
public fun <T> DataRow<T>.print(): Unit = println(this)
public fun <T> DataColumn<T>.print(): Unit = println(this)
