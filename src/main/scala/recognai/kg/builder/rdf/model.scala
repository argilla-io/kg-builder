package recognai.kg.builder.rdf

import recognai.kg.builder.rdf.operations.SparqlOperationsWithJena

/**
  * Created by @frascuchon on 10/11/2016.
  */
package object model extends {

  val URI_TYPE = SparqlOperationsWithJena.typeURI.toString()

  case class Triple(Subject: String, Predicate: String, Object: ObjectProperty)

  object ObjectProperty {

    def apply(uri: String): ObjectProperty = ObjectProperty(Some(uri), None)

    def apply(literal: LiteralProperty): ObjectProperty = ObjectProperty(None, Some(literal))

  }

  case class ObjectProperty(uri: Option[String] = None, literal: Option[LiteralProperty] = None) {

    def isLiteral(): Boolean = literal match {
      case Some(_) => true
      case _ => false
    }

    def isURI(): Boolean = uri match {
      case Some(_) => true
      case _ => false
    }

    def getType(): Option[String] =
      for {
        l <- literal
      } yield l.datatype
  }

  case class LiteralProperty(value: String, datatype: String, lang: Option[String] = None)

  object EmptyObject extends ObjectProperty {

    override def isLiteral(): Boolean = false

    override def isURI(): Boolean = false

  }

  case class Subject(Subject: String, properties: Map[String, Seq[ObjectProperty]]) {

    def literals(): Map[String, Seq[LiteralProperty]] =
      properties.map {
        case (property, values) => (property, values.flatMap(_.literal))
      }

    // TODO: several types per subject
    def getType: Option[String] = properties.get(URI_TYPE).flatMap {
      case a :: _ => a.uri
      case _ => None
    }

  }

}

