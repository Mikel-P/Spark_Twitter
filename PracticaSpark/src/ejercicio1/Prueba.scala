package ejercicio1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.sql.functions._
import java.util.Calendar



object Prueba {
  
  def isHashtag(s : String):Boolean  = {if (s == Nil) false else s.startsWith("#")}
  
  def extractHashtags(s : Array[String]):Array[String] ={
    s.filter(isHashtag)
    
  }
  
  
  
  def main(args: Array[String]) {
      //registramos la hora de comienzo de proceso
      val inicio = Calendar.getInstance().getTime()
      System.out.println("hora inicio:"+inicio);
      
      //inicializamos el contexto de Spark
      val sc = new SparkContext()
      //ponemos a nivel de error los logs para evitar una gran cantidad de mensajes de nivel info
      sc.setLogLevel("ERROR")
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      
      //cargamos los ficheros de datos a tratar
      val datos = sqlContext.read.json("/Twitter/2017/06/12/*")
      
      //seleccionamos el campo de creación del tweet para poder contar el número de tweets por hora
      val fechas = datos.select(substring(col("created_at"),0,13).as("fechas"))
      //agrupamos para obtener el número de tweets por hora y día
      val fechasAgrupadas = fechas.filter(!isnull($"fechas")).groupBy("fechas").count
      fechasAgrupadas.show

      //escribimos el Schema de datos del tweet
      val esquema = datos.printSchema
      
      
      //seleccionamos los campos que nos interesan para trabajar en de aquí en adelante.
      val datosUsuario = datos.select("text", "user.lang", "user.followers_count","user.name")
      val textosUsuario = datosUsuario.select($"text", explode(split($"text", " ")).as("words"), $"lang", $"followers_count", $"name")
      //filtramos para quedarnos con los Hashtag
      val hashtagsUsuario = textosUsuario.filter(col("words").startsWith("#"))
      
      //filtramos por idioma del usuario
      val hashtagUsuario_es = hashtagsUsuario.filter($"lang" === "es")
      val hashtagUsuario_en = hashtagsUsuario.filter($"lang" === "en")
      val hashtagUsuario_fr = hashtagsUsuario.filter($"lang" === "fr")
      val hashtagUsuario_it = hashtagsUsuario.filter($"lang" === "it")
      
      //obtenemos trending topic
      val top10 = hashtagsUsuario.groupBy("words").count.orderBy($"count".desc).show(10)
      
      //obtenemos trending topic por idioma
      val top10_es = hashtagUsuario_es.groupBy("words").count.orderBy($"count".desc).show(10)
      val top10_en = hashtagUsuario_en.groupBy("words").count.orderBy($"count".desc).show(10)
      val top10_fr = hashtagUsuario_fr.groupBy("words").count.orderBy($"count".desc).show(10)
      val top10_it = hashtagUsuario_it.groupBy("words").count.orderBy($"count".desc).show(10)
      
      //obtenemos el usuario con más seguidores que participó ese día
      val userMaxFollowers = datosUsuario.sort($"followers_count".desc).first
      System.out.println("usuario con mas seguidores: "+userMaxFollowers.toString);
      
      //registramos la hora de final de proceso.
      val fin = Calendar.getInstance().getTime()
      System.out.println("hora fin:"+fin); 
      
    
  }
   
}