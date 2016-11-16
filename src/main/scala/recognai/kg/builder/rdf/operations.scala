package recognai.kg.builder.rdf

import java.net.URL

import com.typesafe.scalalogging.LazyLogging
import org.w3.banana.jena.JenaModule
import org.w3.banana.{JsonSolutionsWriterModule, NTriplesWriterModule, RDFModule, RDFOpsModule, SparqlHttpModule, SparqlOpsModule}
import recognai.kg.builder.rdf.model._

import scala.util.{Failure, Success, Try}

/**
  * Created by @frascuchon on 10/11/2016.
  */
package object operations {

  /**
    * Banana rdf modules configuration
    */
  trait RDFModulesCompanion
    extends RDFModule
      with RDFOpsModule
      with SparqlOpsModule
      with SparqlHttpModule
      with NTriplesWriterModule
      with JsonSolutionsWriterModule

  /**
    * Generic banana sparql query engine
    */
  trait SparqlOperations extends RDFModulesCompanion with LazyLogging {

    import ops._
    import recognai.kg.builder.BuilderConstants._
    import sparqlHttp.sparqlEngineSyntax._
    import sparqlOps._

    def queryCount(url: String, queryPart: String, queryPrefix: String): Option[Long] = {
      val queryString = s"$queryPrefix select (count(*) as ?count) {$queryPart}"

      val count = for {
        endpoint <- Try(new URL(url))
        query <- parseSelect(queryString)
        answer <- endpoint.executeSelect(query)
        countNode <- answer.iterator().toList.head("count")
      } yield countNode match {
        case Literal(value, _, _) if countNode.isLiteral => value.toLong
        case _ => 0
      }

      count recoverWith {
        case e: Exception => logger.warn(s"Error executing query ${queryString} for endpoint $url", e); count
      } toOption
    }

    def query(url: String, queryPrefix: String, queryPart: String, offset: Long, limit: Long): Iterator[Triple] = {

      def readTriples(): Try[Iterator[Map[String, Rdf#Node]]] = {
        val queryString = s"$queryPrefix SELECT * {$queryPart} OFFSET $offset LIMIT $limit"
        for {
          endpoint <- Try(new URL(url))
          query <- parseSelect(queryString)
        } yield endpoint.executeSelect(query) match {
          case Success(anwsers) =>
            anwsers.iterator() map (row => row.varnames().flatMap(name => row(name).toOption map (name -> _)).toMap)
          case Failure(err) =>
            logger.warn(s"Cannot perform sparql query $queryString to endpoint $url", err)
            Iterator.empty
        }
      }

      def resolveObject(node: Rdf#Node): Option[ObjectProperty] = {

        // TODO generalize!!!!
        def cleaningValue(value: String, dataTypeURI: String): Option[String] = {
          if ("+0000-00-00T00:00:00Z".equalsIgnoreCase(value)) None
          else Option(value)
        }

        foldNode(node)(
          { case URI(uri) => Some(ObjectProperty(uri)) },
          { case BNode(id) => None },
          { case Literal(value, URI(dataType), lang) =>
            for {
              cleanValue <- cleaningValue(value, dataType)
            } yield ObjectProperty(LiteralProperty(value, dataType, lang.map(_.toString)))
          }
        )
      }

      readTriples() match {
        case Success(results) => results flatMap (m =>
          for {
            s <- m.get(SUBJECT_VARIABLE_NAME)
            p <- m.get(PREDICATE_VARIABLE_NAME)
            o <- m.get(OBJECT_VARIABLE_NAME)
            objectProperty <- resolveObject(o)
          } yield recognai.kg.builder.rdf.model.Triple(s.toString, p.toString, objectProperty)
          )
        case Failure(error) =>
          // TODO log error
          Iterator.empty
      }

    }

    def typeURI: Rdf#URI = rdf.`type`
  }

  /**
    * Jena implementation for sparql query executor
    */
  object SparqlOperationsWithJena extends SparqlOperations with JenaModule

}
