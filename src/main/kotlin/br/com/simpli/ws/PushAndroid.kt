package br.com.simpli.ws

import com.amazonaws.util.ClassLoaderHelper
import com.google.auth.oauth2.GoogleCredentials
import com.google.gson.Gson
import com.google.gson.JsonParser
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.log4j.Logger
import java.io.IOException
import java.lang.StringBuilder
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.util.HashMap
import javax.net.ssl.HttpsURLConnection

/**
 * @author JoaoLippi
 * @since 2019/10/17
 */

class PushAndroid private constructor(private val apiKey: String?, private val servicesJson: String, private val firebase: Boolean, private val legacy: Boolean) {

    @Deprecated("GCM is not supported anymore and FCM legacy is deprecated", ReplaceWith("PushAndroid(apiKey) or PushAndroid(apiKey, servicesJson)"))
    constructor(apiKey: String, firebase: Boolean, legacy: Boolean) : this (apiKey, "/firebase-service.json", firebase, legacy)

    @JvmOverloads
    constructor(servicesJson: String = "/firebase-service.json") : this(null, servicesJson, true, false)

    private val projectId = getProjectId()

    private val endpointGCM = "https://android.googleapis.com/gcm/send"
    private val endpointFMCLegacy = "https://fcm.googleapis.comfcm/send"
    private val endpointFCM = "https://fcm.googleapis.com/v1/projects/$projectId/messages:send"

    private val scope = "https://www.googleapis.com/auth/firebase.messaging"
    private val mapper = Gson()


    fun send(text: String, vararg pushTokens: String): BatchResponse {
        return send(text, pushTokens.toList())
    }

    fun send(text: String, pushTokens: List<String>): BatchResponse {
        return send(text, pushTokens, true)
    }

    @Throws(Exception::class)
    fun send(text: String, pushTokens: List<String>, encode: Boolean): BatchResponse {

        val content = if (encode) URLEncoder.encode(text, "UTF-8") else text
        val url = if (firebase && legacy) endpointFMCLegacy else if (firebase) endpointFCM else endpointGCM

        return if (firebase && !legacy) {
            sendFirebase(content, url, pushTokens)
        } else {
            sendLegacyOrGCM(content, url, pushTokens)
        }
    }

    private fun sendFirebase(content: String, url: String, pushTokens: List<String>) : BatchResponse {

        // New Firebase API doesn't support multiple registration IDs in the same request anymore
        // unless it's a multipart/mixed POST request with a limit of 100 tokens
        val logPayload = mapper.toJson(payload(content))
        Logger.getLogger(PushAndroid::class.java).debug("Sending push: $logPayload")

        return runBlocking {
            pushTokens.map {
                GlobalScope.async {
                    Response(it, post(url,  mapper.toJson(payload(content, it))))
                }
            }.awaitAll()
        }.run {
            BatchResponse(this).apply {
                log()
            }
        }
    }

    private fun sendLegacyOrGCM(content: String, url: String, pushTokens: List<String>) : BatchResponse {
        val payload = mapper.toJson(legacyPayload(content, pushTokens))
        Logger.getLogger(PushAndroid::class.java).debug("Sending push (legacy): $payload")

        return BatchResponse(
            Response(pushTokens, post(url, payload)).apply {
                log()
            }
        )
    }

    private fun post(url: String, parameters: String): HttpsURLConnection {
        return (URL(url).openConnection() as HttpsURLConnection).apply {
            requestMethod = "POST"
            setRequestProperty("Content-Type", "application/json")

            if (legacy) {
                setRequestProperty("Authorization", "key=$apiKey")
            } else {
                setRequestProperty("Authorization", "Bearer ${getAuthToken()}")
            }

            doOutput = true
            outputStream.apply {
                write(parameters.toByteArray())
                flush()
                close()
            }
        }
    }

    private fun legacyPayload(content: String, pushTokens: List<String>): Map<String, Any> {
        return HashMap<String, Any>().apply {
            this["registration_ids"] = pushTokens
            this["data"] = HashMap<String, String>().apply {
                this["message"] = content
            }
        }
    }

    private fun payload(content: String, pushToken: String? = null): Map<String, Any> {
        return HashMap<String, Any>().apply {
            this["message"] = HashMap<String, Any>().apply {
                this["data"] = HashMap<String, String>().apply {
                    this["message"] = content
                }
                pushToken?.also {
                    this["token"] = it
                }
            }
        }
    }

    private fun getProjectId() : String? {
        return ClassLoaderHelper.getResourceAsStream(servicesJson)?.run {
            JsonParser()
                .parse(reader())
                ?.asJsonObject
                ?.get("project_id")
                ?.asString
        }
    }


    private fun getAuthToken(): String? {
        return ClassLoaderHelper.getResourceAsStream(servicesJson)?.run {
            GoogleCredentials
                .fromStream(this)
                .createScoped(scope)
                .run {
                    refreshIfExpired()
                    accessToken.tokenValue
                }
        }
    }

    class Response(tokens: List<String>, con: HttpsURLConnection) : Loggable {
        constructor(token: String, con: HttpsURLConnection) : this(listOf(token), con)

        val tokens = tokens.joinToString()
        val responseCode: Int = con.responseCode
        val responseMessage = StringBuilder().run {
            try {
                con.inputStream
            } catch (e: IOException) {
                con.errorStream
            }?.reader()?.readLines()?.forEach {
                this.append(it)
            }.let {
                this.toString()
            }
        }

        override fun log() {
            Logger.getLogger(PushAndroid::class.java).debug(this.responseMessage)
        }
    }

    class BatchResponse(val responses: List<Response>) : Loggable {
        constructor(vararg responses: Response) : this(responses.toList())

        val successes = responses.count { it.responseCode.success() }
        val failures = responses.count { !it.responseCode.success() }
        val failedTokens = responses.filter { !it.responseCode.success() }.map { it.tokens }

        override fun log() {
            Logger.getLogger(PushAndroid::class.java).debug(logMessage())
        }

        private fun logMessage(): String {
            return """
                Successful pushes: $successes
                Failed pushes: $failures
                Failed tokens: ${failedTokens.joinToString()}
            """.trimIndent()
        }

        private fun Int.success(): Boolean {
            return this < HttpURLConnection.HTTP_BAD_REQUEST
        }
    }

    private interface Loggable {
        fun log()
    }

}
