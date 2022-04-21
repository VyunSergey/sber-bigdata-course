package ru.sberbank.bigdata.study.course.sales.spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.BooleanType

object SparkUDFs extends Serializable {

  /*
   * UDF(User Defined Function) - функция определения соседей по стороне квадрата по заданным координатам
   * X - координаты (`latX`, `lonX`) левого нижнего угла заданного квадрата
   * Y - координаты (`latY`, `lonY`) левого нижнего угла соседних квадратов
   * #====#====#====#
   * |    |    |    |
   * |    |    |    |
   * Y====Y====Y====#
   * |    |****|    |
   * |    |****|    |
   * Y====X====Y====#
   * |    |    |    |
   * |    |    |    |
   * Y====Y====Y====#
   *
   * */
  val coordinateNeighbours: (java.math.BigDecimal, java.math.BigDecimal, java.math.BigDecimal, java.math.BigDecimal) => Boolean =
    (latX, lonX, latY, lonY) => {
      latX.add(latY.negate).abs.compareTo(new java.math.BigDecimal(0.01)) <= 0.0 &&
        lonX.add(lonY.negate).abs.compareTo(new java.math.BigDecimal(0.01)) <= 0.0 &&
        latX.add(latY.negate).abs.add(lonX.add(lonY.negate).abs).compareTo(new java.math.BigDecimal(0.0)) > 0.0
    }

  val neighboursUDF: UserDefinedFunction = udf(coordinateNeighbours, BooleanType)
}
