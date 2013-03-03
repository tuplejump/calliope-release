package com.tuplejump.cobalt.query

import org.specs2.mutable._
import com.tuplejump.cobalt.query._
import com.tuplejump.cobalt.RichByteBuffer._
import org.apache.cassandra.thrift.IndexOperator

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 3/1/13
 * Time: 8:17 PM
 * To change this template use File | Settings | File Templates.
 */
class QuerySpec extends Specification {
  "Query" should {
    "instantiate as EmptyQuery" in {
      val q = Query
      q must beAnInstanceOf[EmptyQuery]
    }

    "return a FirstColumn on call to where" in {
      val col = Query.where("name")
      col must beAnInstanceOf[FirstColumn]

      col.isEq _ //Assert we have the equal function

      //This will not compile
      //col.isGt
      //col.isGte
      //col.isLt
      //col.isLte

      //to rpevent compilation error
      true
    }

    "create InitializedQuery on call to isEq in FirstColumn" in {
      val q = Query.where("name").isEq("John")
      q must beAnInstanceOf[InitializedQuery]

      //This will not compile
      //q.where

      q.and _ //verify that you have 'and'

      //to rpevent compilation error
      true
    }

    "give regular column from call to and on initialized query" in {
      val col = Query.where("name").isEq("John").and("age")
      col must beAnInstanceOf[Column]

      //Must compile
      col.isEq _
      col.isGt _
      col.isGte _
      col.isLt _
      col.isLte _

      //to rpevent compilation error
      true
    }

    "build correct IndexExpression with single condition" in {
      val q = Query.where("name").isEq("John")
      import FinalQuery._

      val exprs = q.getExpressions()
      exprs.length mustEqual (1)

      val expr = exprs(0)

      "name".equalsIgnoreCase(expr.bufferForColumn_name()) must beTrue
      "John".equalsIgnoreCase(expr.bufferForValue()) must beTrue
      expr.getOp mustEqual (IndexOperator.EQ)

      //to rpevent compilation error
      true
    }

    "build correct IndexExpression list with multiple conditions" in {
      val q = Query.where("name").isEq("John").and("age").isGt(10)

      import FinalQuery._
      val exprs = q.getExpressions()
      exprs.length mustEqual (2)

      val firstExpr = exprs(0)

      "name".equalsIgnoreCase(firstExpr.bufferForColumn_name()) must beTrue
      "John".equalsIgnoreCase(firstExpr.bufferForValue()) must beTrue
      firstExpr.getOp mustEqual (IndexOperator.EQ)

      val secExpr = exprs(1)

      "age".equalsIgnoreCase(secExpr.bufferForColumn_name()) must beTrue
      (10 - secExpr.bufferForValue()) mustEqual 0
      secExpr.getOp mustEqual (IndexOperator.GT)


    }
  }
}
