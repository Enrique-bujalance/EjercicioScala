package org.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Ejercicio1 {
  def main(args: Array[String]): Unit = {
    println("Ejercicio avanzado de Spark con Scala y Maven!")

    val spark = SparkSession.builder()
      .appName("EjercicioAvanzado")
      .master("local[*]")
      .getOrCreate()

    // Simulación de datos de ventas
    val ventas = Seq(
      (1, "Juan", "Electrónica", 500.0, "2024-01-01"),
      (2, "María", "Hogar", 200.0, "2024-01-05"),
      (3, "Pedro", "Electrónica", 150.0, "2024-02-01"),
      (4, "Lucía", "Moda", 100.0, "2024-02-15"),
      (5, "Juan", "Moda", 300.0, "2024-03-10"),
      (6, "María", "Electrónica", 700.0, "2024-03-20")
    )

    val ventasDF = spark.createDataFrame(ventas)
      .toDF("id_venta", "cliente", "categoria", "monto", "fecha")

    // Mostrar el DataFrame original
    println("Ventas originales:")
    ventasDF.show()

    // Cálculo del total de ventas por cliente
    val ventasPorCliente = ventasDF
      .groupBy("cliente")
      .agg(
        sum("monto").alias("total_ventas"),
        count("id_venta").alias("cantidad_compras")
      )
      .orderBy(desc("total_ventas"))

    println("Ventas por cliente:")
    ventasPorCliente.show()

    // Filtrar ventas de electrónica mayores a 300
    val ventasFiltradas = ventasDF
      .filter(col("categoria") === "Electrónica" && col("monto") > 300)

    println("Ventas de electrónica mayores a 300:")
    ventasFiltradas.show()

    // Añadir columna de año de la venta
    val ventasConAño = ventasDF.withColumn("año", year(col("fecha")))

    println("Ventas con año añadido:")
    ventasConAño.show()

    // Función personalizada para clasificar ventas
    def clasificarVenta(monto: Double): String = {
      if (monto > 500) "Alta"
      else if (monto > 200) "Media"
      else "Baja"
    }

    // Registro de función UDF
    val clasificarUDF = udf(clasificarVenta _)

    // Aplicar la función UDF al DataFrame
    val ventasClasificadas = ventasDF.withColumn("clasificación", clasificarUDF(col("monto")))

    println("Ventas clasificadas:")
    ventasClasificadas.show()

    // Detener Spark
    spark.stop()
  }
}