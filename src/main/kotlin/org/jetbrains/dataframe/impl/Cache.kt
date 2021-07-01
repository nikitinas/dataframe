package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.Many
import org.jetbrains.dataframe.createTypeWithArgument
import kotlin.reflect.KType

private val listTypes = mutableMapOf<KType, KType>()

public fun getListType(valueType: KType): KType = listTypes.getOrPut(valueType) { Many::class.createTypeWithArgument(valueType) }
