package com.tuplejump.cobalt.query

import org.apache.cassandra.thrift.{IndexOperator, IndexExpression}
import java.nio.ByteBuffer

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 3/1/13
 * Time: 7:01 PM
 * To change this template use File | Settings | File Templates.
 */

trait Query {
  protected[query] var expressions = List[IndexExpression]()
}

class EmptyQuery() extends Query {
  implicit val query = this
  def where(colName: ByteBuffer) = {
    new FirstColumn(colName)
  }
}

class InitializedQuery(q: Query) extends Query {
  implicit val query = this
  this.expressions = q.expressions
  def and(colName: ByteBuffer) = {
    new Column(colName)
  }
}

class FinalQuery(q: InitializedQuery) {
  def getExpressions() = {
    q.expressions.reverse
  }
}

object FinalQuery {
  implicit def Query2BuiltQuery(q: InitializedQuery) = new FinalQuery(q)
}

object Query extends EmptyQuery {
  def apply = new EmptyQuery()
}

class FirstColumn(colName: ByteBuffer)(implicit query: Query) {
  def isEq(colValue: ByteBuffer) = {
    query.expressions ::= new IndexExpression(colName, IndexOperator.EQ, colValue)
    println(query.expressions)
    new InitializedQuery(query)
  }
}

class Column(colName: ByteBuffer)(implicit query: InitializedQuery) {
  def isEq(colValue: ByteBuffer) = {
    query.expressions ::= new IndexExpression(colName, IndexOperator.EQ, colValue)
    query
  }

  def isGt(colValue: ByteBuffer) = {
    query.expressions ::= new IndexExpression(colName, IndexOperator.GT, colValue)
    query
  }

  def isGte(colValue: ByteBuffer) = {
    query.expressions ::= new IndexExpression(colName, IndexOperator.GTE, colValue)
    query
  }

  def isLt(colValue: ByteBuffer) = {
    query.expressions ::= new IndexExpression(colName, IndexOperator.LT, colValue)
    query
  }

  def isLte(colValue: ByteBuffer) = {
    query.expressions ::= new IndexExpression(colName, IndexOperator.LTE, colValue)
    query
  }
}

