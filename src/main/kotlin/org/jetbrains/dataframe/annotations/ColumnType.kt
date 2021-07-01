package org.jetbrains.dataframe.annotations

import org.jetbrains.dataframe.columns.AnyCol
import kotlin.reflect.KClass

@Target(AnnotationTarget.PROPERTY)
public annotation class ColumnType(val type: KClass<out AnyCol>)
