package org.jetbrains.dataframe.annotations

@Target(AnnotationTarget.PROPERTY)
public annotation class ColumnName(val name: String)
