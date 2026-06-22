import org.apache.spark.sql.SparkSession
import java.io.FileNotFoundException

object Main {
  def main(args: Array[String]): Unit = {
    val cmdArgs = CommandLineArgs.parse(args).getOrElse(return)

    // cargo los subscriptions desde el archivo JSON manejando errores
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

    val subscriptions = subscriptionsRaw.flatten
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // inicia spark localmente
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // pararelizo y proceso los Feeds con manejo seguro (Worker)
    val subscriptionsRDD = sc.parallelize(subscriptions)

    val downloadResultsRDD = subscriptionsRDD.map { sub =>
      val feedOpt = try {
        FileIO.downloadFeed(sub.url)
      } catch {
        case _: Exception => None
      }

      if (feedOpt.isEmpty) {
        println(s"Warning: Failed to download from '${sub.name}' (${sub.url})")
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

      (feedOpt.isDefined, posts)
    }.cache() // fundamental para no descargar dos veces al contar

    // estadisticas
    val feedsSuccess = downloadResultsRDD.filter(_._1).count().toInt
    val feedsFailed = downloadResultsRDD.filter(!_._1).count().toInt

    val allPostsRDD = downloadResultsRDD.flatMap(_._2).cache()
    val postsSuccess = allPostsRDD.count().toInt
    val postsFailed = downloadResultsRDD.filter(_._2.isEmpty).count().toInt

    // extraigo el RDD de posts filtrados (este es el "resultado" que pide el Ej 2)
    val filteredPostsRDD = allPostsRDD
      .filter(p => p.title.nonEmpty && p.selftext.nonEmpty && p.selftext.trim.nonEmpty)
      .cache()
      
    val postsFiltered = postsSuccess - filteredPostsRDD.count().toInt

    val totalChars = filteredPostsRDD.map(p => p.title.length + p.selftext.length).sum().toLong
    val avgChars = if (filteredPostsRDD.count() > 0) (totalChars / filteredPostsRDD.count()).toInt else 0

    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed,
      "postsFiltered" -> postsFiltered,
      "avgChars" -> avgChars
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    if (filteredPostsRDD.count() == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }
 
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)
    
    val allEntities = filteredPostsRDD.collect().toList.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }
}