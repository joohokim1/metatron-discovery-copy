package app.metatron.discovery.prep.spark.rule.util

import java.sql.Types

import app.metatron.discovery.prep.parser.preparation.rule.expr._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

/**
  * Created by nowone on 2018. 11. 13..
  */
object ColumnTypeUtil {
  // ExprType 다시 참고
  val STRING      = "STRING"
  val LONG        = "LONG"
  val DOUBLE      = "DOUBLE"
  val BOOLEAN     = "BOOLEAN"
  val ARRAY       = "ARRAY"
  val MAP         = "MAP"
  val TIMESTAMP   = "TIMESTAMP"
  val UNKNOWN     = "UNKNOWN"


  def fromClass(obj: Any): String = obj match {
    case value: String                          => STRING
    case value: Long                            => LONG
    case value: Double                          => DOUBLE
    case value: Boolean                         => BOOLEAN
    case value: java.util.List[Object]          => ARRAY
    case value: java.util.Map[Object, Object]   => MAP
    case value: DateTime                        => TIMESTAMP
    case _                                      => UNKNOWN

  }

  def fromJdbcType( jdbcType:Int ): String = jdbcType match {

    case Types.VARCHAR                  => STRING
    case Types.CHAR                     => STRING
    case Types.NCHAR                    => STRING
    case Types.NVARCHAR                 => STRING
    case Types.LONGNVARCHAR             => STRING
    case Types.BOOLEAN                  => BOOLEAN
    case Types.NUMERIC                  => LONG
    case Types.INTEGER                  => LONG
    case Types.BIGINT                   => LONG
    case Types.FLOAT                    => DOUBLE
    case Types.DOUBLE                   => DOUBLE
    case Types.DECIMAL                  => DOUBLE
    case Types.ARRAY                    => ARRAY
    case Types.JAVA_OBJECT              => MAP
    case Types.DATE                     => TIMESTAMP
    case Types.TIME                     => TIMESTAMP
    case Types.TIME_WITH_TIMEZONE       => TIMESTAMP
    case Types.TIMESTAMP                => TIMESTAMP
    case Types.TIMESTAMP_WITH_TIMEZONE  => TIMESTAMP
    case  _                             => STRING
  }

  def fromDataType( dataType:DataType ): String = dataType match {

    case StringType                   => STRING
    case BooleanType                  => BOOLEAN
    case ByteType                     => LONG
    case ShortType                    => LONG
    case IntegerType                  => LONG
    case LongType                     => LONG
    case FloatType                    => DOUBLE
    case DoubleType                   => DOUBLE
    case TimestampType                => TIMESTAMP
    case  _                           => UNKNOWN
  }


  def decideType( exprOrg: Expression): String = {

    def decideTypeInternal(expr: Expression): String = {
      var returnType = UNKNOWN;
      if (expr.isInstanceOf[Identifier.IdentifierExpr]) {
        returnType
        // need df
      } else if (expr.isInstanceOf[Constant]) {
        returnType = expr match {
          case e: Constant.StringExpr   => STRING
          case e: Constant.LongExpr     => LONG
          case e: Constant.DoubleExpr   => DOUBLE
          case e: Constant.BooleanExpr  => BOOLEAN
          case e: Null.NullExpr => UNKNOWN
          case _ => throw new IllegalArgumentException("decideType(): unsupported constant type: expr=" + expr.toString)
        }
      } else if (expr.isInstanceOf[Expr.BinaryNumericOpExprBase]) {
        val left = decideTypeInternal(expr.asInstanceOf[Expr.BinaryNumericOpExprBase].getLeft());
        val right = decideTypeInternal(expr.asInstanceOf[Expr.BinaryNumericOpExprBase].getRight());
        if (left == right) {
          // for compatability to twinkle, which acts like this because of spark's behavior
          returnType = if(expr.isInstanceOf[Expr.BinDivExpr]) DOUBLE else left
        } else if ( (left == DOUBLE && right == LONG ) || ( left == LONG && right == DOUBLE) ) {
          // 한쪽이 double인 경우는 허용
          returnType = DOUBLE;
        }else {
          val msg = "decideType(): type mismatch: left=%s right=%s expr=%s".format(left, right, expr);
          throw new IllegalArgumentException(msg)
        }
      }
      // Function Operation
      else if (expr.isInstanceOf[Expr.FunctionExpr]) {
        val func = expr.asInstanceOf[Expr.FunctionExpr].getName();
        val args = expr.asInstanceOf[Expr.FunctionExpr].getArgs();

        returnType = func match {
          // conditional function
          case "if" => {
            if (args.size() == 1) {
              BOOLEAN;
            } else if (args.size() == 3) {
              val trueExpr = decideTypeInternal(args.get(1));
              val falseExpr = decideTypeInternal(args.get(2));
              if (trueExpr == falseExpr) {
                trueExpr;
              } else {
                if (trueExpr == UNKNOWN && falseExpr == UNKNOWN) {
                  val msg = "decideType(): both types are UNKNOWN trueVal=%s falseVal=%s".format(args.get(1).toString(), args.get(1).toString());
                  throw new IllegalArgumentException(msg)
                } else if (trueExpr == UNKNOWN) {
                  falseExpr;
                } else if (falseExpr == UNKNOWN) {
                  trueExpr;
                } else {
                  val msg = "decideType(): type different: trueVal=%s falseVal=%s".format(args.get(1).toString(), args.get(1).toString());
                  throw new IllegalArgumentException(msg)

                }
              }
            } else {
              throw new IllegalArgumentException("decideType(): invalid conditional function argument count: " + args.size());
            }

          }
            // 1-argument functions
          case "length" | "math.getExponent" |"math.round"                      => LONG
          case "isnull" | "isnan" | "ismissing"                                 => BOOLEAN
          case "ismismatched"                                                   => BOOLEAN
          case "upper" | "lower" | "trim" | "ltrim" |  "rtrim"                  => STRING
          case "math.abs"                                                       => decideTypeInternal(args.get(0));
          case "math.acos" | "math.asin" | "math.atan" | "math.cbrt" |
               "math.ceil" | "math.cos" | "math.cosh" |  "math.exp" |
               "math.floor" | "math.signum" | "math.sin" |"math.sinh" |
               "math.sqrt" | "math.tan" | "math.tanh"                           => DOUBLE
          case "math.max" | "math.min"                                          => LONG
          case "math.pow"                                                       => DOUBLE
          case "coalesce"                                                       => UNKNOWN
          case "substring"                                                      => STRING
          case "contains"                                                       => BOOLEAN
          case "startswith"                                                     => BOOLEAN
          case "endswith"                                                       => BOOLEAN
          case "timestamptostring"                                              => STRING
          case "concat"                                                         => STRING
          case "concat_ws"                                                      => STRING
          case "sum" |"avg" | "mean" | "max" | "min"                            => DOUBLE
          case "year" | "month" | "day" | "hour" |
               "minute" | "second" | "millisecond"                              => LONG
          case "weekday"                                                        => STRING
          case "now"                                                            => TIMESTAMP
          case "add_time"                                                       => TIMESTAMP
          case "time_diff"                                                      => LONG
          case "timestamp"                                                      => TIMESTAMP
          case _                                                                => {
            throw new IllegalArgumentException("decideType(): invalid function type");
          }
        } // end of switch (func)
      } // end of if (expr instanceof Expr.FunctionExpr)

      return returnType;
    }

    decideTypeInternal(exprOrg)
  }

}
