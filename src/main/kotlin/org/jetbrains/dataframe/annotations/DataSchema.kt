package org.jetbrains.dataframe.annotations

@Target(AnnotationTarget.CLASS)
public annotation class DataSchema(val isOpen: Boolean = true)
