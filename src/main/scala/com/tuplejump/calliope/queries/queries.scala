package com.tuplejump.calliope.queries

import org.apache.cassandra.thrift.{IndexOperator, IndexExpression}
import java.nio.ByteBuffer

trait ThriftQuery {
  protected[queries] var expressions = List[IndexExpression]()
}

class Query() extends ThriftQuery {
  implicit val query = this

  def where(colName: ByteBuffer) = {
    new FirstColumn(colName)
  }
}

object Query {
  def apply() = new Query()

  //def apply() = new Query()
}

class InitializedQuery(q: ThriftQuery) extends ThriftQuery {
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

class FirstColumn(colName: ByteBuffer)(implicit query: ThriftQuery) {
  def isEq(colValue: ByteBuffer) = {
    query.expressions ::= new IndexExpression(colName, IndexOperator.EQ, colValue)
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
