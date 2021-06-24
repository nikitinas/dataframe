package org.jetbrains.dataframe

import io.kotest.matchers.shouldBe
import org.jetbrains.dataframe.columns.ColumnGroup
import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.SingleColumn
import org.jetbrains.dataframe.impl.columns.ColumnGroupWithParent
import org.jetbrains.dataframe.impl.createTypeUsing
import org.junit.Test

class TypeProjectionTests {
    class TypeInferenceTest1 {
        interface A<T>
        interface X<T> : A<List<T>>

        @Test
        fun test() {
            X::class.createTypeUsing<A<List<Int>>>() shouldBe getType<X<Int>>()
            A::class.createTypeUsing<X<Int>>() shouldBe getType<A<List<Int>>>()
        }
    }

    class TypeInferenceTest2 {
        interface A<out T>
        interface B<T> : A<A<T>>
        interface C<T> : A<B<T>>
        interface D<T>
        interface X<T : Number, V : Number> : C<T>, D<V>

        @Test
        fun test() {
            X::class.createTypeUsing<A<A<A<Int>>>>() shouldBe getType<X<Int, *>>()
            A::class.createTypeUsing<X<Double, Int>?>() shouldBe getType<A<B<Double>>?>()
        }
    }

    @Test
    fun `collection to list projection`() {
        List::class.createTypeUsing<Collection<Int>?>() shouldBe getType<List<Int>?>()
        Collection::class.createTypeUsing<List<Int>>() shouldBe getType<Collection<Int>>()
    }

    @Test
    fun `column group projections`() {
        ColumnGroup::class.createTypeUsing<ColumnReference<DataRowBase<Int>>>() shouldBe getType<ColumnGroup<Int>>()
        SingleColumn::class.createTypeUsing<ColumnGroupWithParent<Int>>() shouldBe getType<SingleColumn<DataRow<Int>>>()
    }
}
