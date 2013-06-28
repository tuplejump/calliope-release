package com.tuplejump.calliope.queries

import com.tuplejump.calliope.utils.RichByteBuffer
import RichByteBuffer._
import org.apache.cassandra.thrift.IndexOperator
import org.scalatest.FunSpec
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import com.tuplejump.calliope.utils.RichByteBuffer

class QuerySpec extends FunSpec with ShouldMatchers with MustMatchers {

  describe("Query") {
    it("should instantiate as Query") {
      val q = Query()
      q.isInstanceOf[Query] must be(true)
    }

    it("should return a FirstColumn on call to where") {
      val col = Query().where("name")
      col.isInstanceOf[FirstColumn] must be(true)

      col.isEq _ //Assert we have the equal function

      //This will not compile
      //col.isGt
      //col.isGte
      //col.isLt
      //col.isLte

      //to rpevent compilation error
      true
    }

    it("should create InitializedQuery on call to isEq in FirstColumn") {
      val q = Query().where("name").isEq("John")
      q.isInstanceOf[InitializedQuery] must be(true)

      //This will not compile
      //query.where

      q.and _ //verify that you have 'and'

      //to prevent compilation error
      true
    }

    it("should give regular column from call to and on initialized query") {
      val col = Query().where("name").isEq("John").and("age")
      col.isInstanceOf[Column] must be(true)

      //Must compile
      col.isEq _
      col.isGt _
      col.isGte _
      col.isLt _
      col.isLte _

      //to prevent compilation error
      true
    }

    it("should build correct IndexExpression with single condition") {
      import FinalQuery._

      val q = Query().where("name").isEq("John")

      val exprs = q.getExpressions()
      println(exprs)
      exprs.length must be(1)

      val expr = exprs(0)

      "name".equalsIgnoreCase(expr.bufferForColumn_name()) must be(true)
      "John".equalsIgnoreCase(expr.bufferForValue()) must be(true)
      expr.getOp must be(IndexOperator.EQ)

      //to prevent compilation error
      true
    }

    it("should build correct IndexExpression list with multiple conditions") {
      val q = Query().where("name").isEq("John").and("age").isGt(10)

      import FinalQuery._
      val exprs = q.getExpressions()
      exprs.length must be(2)

      val firstExpr = exprs(0)

      "name".equalsIgnoreCase(firstExpr.bufferForColumn_name()) must be(true)
      "John".equalsIgnoreCase(firstExpr.bufferForValue()) must be(true)
      firstExpr.getOp must be(IndexOperator.EQ)

      val secExpr = exprs(1)

      "age".equalsIgnoreCase(secExpr.bufferForColumn_name()) must be(true)
      10 - secExpr.bufferForValue() must be(0)
      secExpr.getOp must be(IndexOperator.GT)


    }
  }
}