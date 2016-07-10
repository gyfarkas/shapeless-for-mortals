// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
/**
 * TypeClass (api/impl/syntax) for marshalling objects into
 * `java.util.HashMap<String,Object>` (yay, big data!).
 */
package s4m.smbd

import shapeless._, labelled.{ field, FieldType }

/**
 * This exercise involves writing tests, only a skeleton is provided.
 *
 * - Exercise 1.1: derive =BigDataFormat= for sealed traits.
 * - Exercise 1.2: define identity constraints using singleton types.
 */
package object api {
  type StringyMap = java.util.HashMap[String, AnyRef]
  type BigResult[T] = Either[String, T] // aggregating errors doesn't add much
}

package api {
  trait BigDataFormat[T] {
    def label: String
    def toProperties(t: T): StringyMap
    def fromProperties(m: StringyMap): BigResult[T]
  }

  trait SPrimitive[V] {
    def toValue(v: V): AnyRef
    def fromValue(v: AnyRef): V
  }

  // EXERCISE 1.2
  trait BigDataFormatId[T, V] {
    def key: String
    def value(t: T): V
  }
}

package object impl {
  import api._

  implicit object stringPromitive extends SPrimitive[String] {
    def toValue(s: String) = s
    def fromValue(v: AnyRef) = v.toString
  }

  implicit object intPromitive extends SPrimitive[Int] {
    def toValue(s: Int) = s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) = v.asInstanceOf[Int]
  }

  implicit object DoublePromitive extends SPrimitive[Double] {
    def toValue(s: Double) = s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) = v.asInstanceOf[Double]
  }

  implicit object BoolPromitive extends SPrimitive[Boolean] {
    def toValue(s: Boolean) = s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) = v.asInstanceOf[Boolean]
  }

  // EXERCISE 1.1 goes here
  implicit object hNilBigDataFormat extends BigDataFormat[HNil] {
    def label = "hnil"
    def toProperties(t: HNil) = new java.util.HashMap[String, AnyRef]()
    def fromProperties(m: StringyMap) = Right(HNil)
  }

  implicit def hListBigDataFormat[Key <: Symbol, Value, Tail <: HList](implicit
    witness: Witness.Aux[Key],
    valueFormatter: Lazy[SPrimitive[Value]],
    restFormatter: Lazy[BigDataFormat[Tail]]): BigDataFormat[FieldType[Key, Value] :: Tail] =
    new BigDataFormat[FieldType[Key, Value] :: Tail] {
      def label = "hcons"
      def toProperties(t: FieldType[Key, Value] :: Tail): StringyMap = {
        val key = witness.value.name
        val rest = restFormatter.value.toProperties(t.tail)
        val head = valueFormatter.value.toValue(t.head)
        rest.put(key, head)
        rest
      }

      def fromProperties(m: StringyMap): BigResult[FieldType[Key, Value] :: Tail] = {
        val x = valueFormatter.value.fromValue(m.get(witness.value.name))
        val h = field[Key](x)
        val tail = restFormatter.value.fromProperties(m)
        tail match {
          case Left(e) => Left(e)
          case Right(t) => Right(h :: t)
        }
      }
    }

  implicit object cNilBigDataFormat extends BigDataFormat[CNil] {
    def label = "cNil"
    def toProperties(t: CNil): StringyMap = new java.util.HashMap[String, AnyRef]()
    def fromProperties(m: StringyMap) = Left("CNil")
  }

  implicit def coproductBigDataFormat[Key <: Symbol, Value, Rest <: Coproduct](implicit
    witness: Witness.Aux[Key],
    valueFormatter: Lazy[BigDataFormat[Value]],
    restFormatter: Lazy[BigDataFormat[Rest]]): BigDataFormat[FieldType[Key, Value] :+: Rest] = new BigDataFormat[FieldType[Key, Value] :+: Rest] {
    def label = "cNil"
    def toProperties(t: FieldType[Key, Value] :+: Rest) = {
      t match {
        case Inl(h) => {
          val r = valueFormatter.value.toProperties(h)
          r.put("type", witness.value.name)
          r
        }
        case Inr(r) => restFormatter.value.toProperties(r)
      }
    }

    def fromProperties(m: StringyMap) = {
      if (m.get("type") == witness.value.name) {
        valueFormatter.value.fromProperties(m) match {
          case Right(x) => Right(Inl(field[Key](x)))
          case Left(y) => Left(y)
        }
      } else {
        restFormatter.value.fromProperties(m) match {
          case Left(y) => Left(y)
          case Right(x) => Right(Inr(x))
        }
      }
    }
  }

  implicit def familyBigDataFormat[T, Repr](implicit
    gen: LabelledGeneric.Aux[T, Repr],
    reprFormatter: Lazy[BigDataFormat[Repr]],
    tpe: Typeable[T]): BigDataFormat[T] =
    new BigDataFormat[T] {
      def label = "T"
      def toProperties(t: T) = reprFormatter.value.toProperties(gen.to(t))
      def fromProperties(m: StringyMap) = try {
        reprFormatter.value.fromProperties(m) match {
          case Right(x) => Right(gen.from(x))
          case Left(y) => Left(y)
        }
      } catch {
        case _: Throwable => Left("some sensible error here")
      }
    }
}

package impl {
  import api._

  // EXERCISE 1.2 goes here
}

package object syntax {
  import api._

  implicit class RichBigResult[R](val e: BigResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) => throw new IllegalArgumentException(error.mkString(","))
      case Right(r) => r
    }
  }

  /** Syntactic helper for serialisables. */
  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def label(implicit s: BigDataFormat[T]): String = s.label
    def toProperties(implicit s: BigDataFormat[T]): StringyMap = s.toProperties(t)
    def idKey[P](implicit lens: Lens[T, P]): String = ???
    def idValue[P](implicit lens: Lens[T, P]): P = lens.get(t)
  }

  implicit class RichProperties(val props: StringyMap) extends AnyVal {
    def as[T](implicit s: BigDataFormat[T]): T = s.fromProperties(props).getOrThrowError
  }
}
