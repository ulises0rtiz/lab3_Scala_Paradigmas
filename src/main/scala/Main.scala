import org.apache.spark.sql.SparkSession
import java.io.FileNotFoundException

object Main {
  def main(args: Array[String]): Unit = {
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return 
    }

    // cargo suscripciones desde archivo JSON, manejo de errores y advertencias
    val subscriptionsRaw = try {
      FileIO.readSubscriptions(cmdArgs.subscriptionFile)
    } catch {
      case _: FileNotFoundException =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - file not found")
        return
      case _: Exception =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - invalid JSON format")
        return
    }

    // filtro de suscripciones válidas (aquellas que tienen 'name' y 'url')
    val subscriptions = subscriptionsRaw.flatten
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // inicialización de Spark
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // inicialización de acumuladores para estadísticas
    val feedsSuccessAcc = sc.longAccumulator("FeedsSuccess")
    val feedsFailedAcc = sc.longAccumulator("FeedsFailed")
    val postsTotalAcc = sc.longAccumulator("PostsTotal")
    val postsDiscardedAcc = sc.longAccumulator("PostsDiscarded")

    // paralelización de la lista de suscripciones
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // descargar feeds y parsear posts, con manejo de errores y acumuladores
    val downloadResultsRDD = subscriptionsRDD.map { sub =>
      val feedOpt = try {
        FileIO.downloadFeed(sub.url)
      } catch {
        case _: Exception => None
      }

      if (feedOpt.isEmpty) {
        feedsFailedAcc.add(1)
        println(s"Warning: Failed to download from '${sub.name}' (${sub.url})")
      } else {
        feedsSuccessAcc.add(1)
      }

      val posts = feedOpt match {
        case Some(json) =>
          try {
            JsonParser.parsePosts(json, sub.name)
          } catch {
            case _: Exception =>
              println(s"Warning: Failed to parse posts from '${sub.name}' (${sub.url})")
              List.empty[Post]
          }
        case None => List.empty[Post]
      }
      
      posts.foreach(_ => postsTotalAcc.add(1))
      posts
    }

    val allPostsRDD = downloadResultsRDD.flatMap(posts => posts)

    // filtro de posts vacíos o irrelevantes, con acumulador de posts descartados
    val filteredPostsRDD = allPostsRDD.filter { p =>
      val isValid = p.title.nonEmpty && p.selftext.nonEmpty && p.selftext.trim.nonEmpty
      if (!isValid) postsDiscardedAcc.add(1)
      isValid
    }.cache()

    // mido tiempo de la primera etapa
    val t0 = System.currentTimeMillis()
    val validPostsCount = filteredPostsRDD.count()
    val t1 = System.currentTimeMillis()
    println(s"\n[Tiempo] Descarga y filtrado: ${(t1 - t0) / 1000.0} segundos\n")

    // calculo de estadísticas de longitud de posts
    val totalChars = if (validPostsCount > 0) filteredPostsRDD.map(p => p.title.length + p.selftext.length).sum().toLong else 0L
    val avgChars = if (validPostsCount > 0) (totalChars / validPostsCount).toInt else 0

    // preparo estadísticas de procesamiento
    val stats = Map(
      "feedsSuccess" -> feedsSuccessAcc.value.toInt,
      "feedsFailed" -> feedsFailedAcc.value.toInt,
      "postsSuccess" -> postsTotalAcc.value.toInt,
      "postsFailed" -> 0, 
      "postsFiltered" -> postsDiscardedAcc.value.toInt,
      "avgChars" -> avgChars
    )

    // imprimir estadísticas de procesamiento
    println(Formatters.formatProcessingStats(stats))
    println()

    // chequeo de que haya posts válidos antes de continuar con la detección de entidades
    if (validPostsCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    // cargo diccionario de entidades
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // detecto entidades
    val entitiesRDD = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // contador entidades
    val pairsRDD = entitiesRDD.map(e => ((e.entityType, e.text), 1))
    val countsRDD = pairsRDD.reduceByKey(_ + _)
    val sortedRDD = countsRDD.sortBy(pair => (-pair._2, pair._1._1, pair._1._2))

    // mido tiempo del cómputo Map-Reduce
    val t2 = System.currentTimeMillis()
    val finalResults = sortedRDD.collect().toList
    val t3 = System.currentTimeMillis()
    println(s"\n[Tiempo] Procesamiento Map-Reduce: ${(t3 - t2) / 1000.0} segundos\n")

    val typeStats = finalResults.groupBy(_._1._1).view.mapValues(_.map(_._2).sum).toMap + ("total" -> finalResults.map(_._2).sum)
    
    println(Formatters.formatTypeStats(typeStats))
    println()
    
    val entityCountsMap = finalResults.toMap
    println(Formatters.formatEntityStats(entityCountsMap, cmdArgs.topK))

    // ejercicio 5 
    filteredPostsRDD.unpersist()
    spark.stop()
  }
}