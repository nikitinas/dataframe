package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.DataColumn

fun <T> DataFrame<T>.print() = println(this)
fun <T> DataRow<T>.print() = println(this)
fun <T> DataColumn<T>.print() = println(this)