package app.metatron.discovery.prep.spark

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scalaj.http.{Http, HttpResponse}

/**
  * Created by nowone on 2018. 10. 11..
  */
object HttpUtil {


  //var headerMap = Map(
  //  "Content-Type" -> "application/json;charset=UTF-8",
  //  "Accept" -> "application/json, text/plain, */*",
  //  "Authorization" -> "oauth_token"
  // )

  /*
  var dataMap = Map("oq" -> "scalr")
  HttpUtil.get("http://www.google.co.kr/search", dataMap);
  // HttpUtil.post("http://www.google.co.kr/search", dataMap);
  */

  def encodePath(path: String): String = {
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
  }

  def get(url: String, paramMap: Map[String, String] = null, headerMap: Map[String, String] = null) : HttpResponse[String] = {
    this.call(url, "GET", paramMap, headerMap)
  }

  def post(url: String, dataMap: Map[String, String] = null, headerMap: Map[String, String] = null) : HttpResponse[String] = {
    this.call(url, "POST", dataMap, headerMap)
  }

  def patch(url: String, dataMap: Map[String, String] = null, headerMap: Map[String, String] = null) : HttpResponse[String] = {
    this.call(url, "PATCH", dataMap, headerMap)
  }

  def call(url: String, method: String = "GET", paramMap: Map[String, String] = null, headerMap: Map[String, String] = null) : HttpResponse[String] = {
    // get, patch, header
    var http = Http(url)

    if( paramMap != null) {
      http = http.params(paramMap)
    }

    if( headerMap != null ){
      http = http.headers(headerMap)
    }

    val response = http.method(method).asString

    println(response.body)

    response
  }

}
