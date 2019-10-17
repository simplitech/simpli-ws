package br.com.simpli.ws

import com.amazonaws.util.ClassLoaderHelper.getResourceAsStream
import com.google.gson.Gson
import org.apache.log4j.Logger
import java.net.URL
import java.net.URLEncoder
import java.util.HashMap
import javax.net.ssl.HttpsURLConnection
import com.google.auth.oauth2.GoogleCredentials
import com.google.gson.JsonParser
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import java.io.*
import java.lang.StringBuilder


class PushAndroid private constructor(private val apiKey: String, private val servicesJson: String, private val firebase: Boolean, private val legacy: Boolean) {

    @Deprecated("GCM is not supported anymore and FCM legacy is deprecated", ReplaceWith("PushAndroid(apiKey) or PushAndroid(apiKey, servicesJson)"))
    constructor(apiKey: String, firebase: Boolean, legacy: Boolean) : this (apiKey, "/google-services.json", firebase, legacy)

    @JvmOverloads
    constructor(apiKey: String, servicesJson: String = "/google-services.json") : this(apiKey, servicesJson, true, false)

    private val projectId = getProjectId()

    private val endpointGCM = "https://android.googleapis.com/gcm/send"
    private val endpointFMCLegacy = "https://fcm.googleapis.comfcm/send"
    private val endpointFCM = "https://fcm.googleapis.com/v1/projects/$projectId/messages:send"

    private val scope = "https://www.googleapis.com/auth/firebase.messaging"
    private val mapper = Gson()


    fun send(text: String, vararg pushTokens: String) {
        send(text, pushTokens.toList())
    }

    fun send(text: String, pushTokens: List<String>) {
        send(text, pushTokens, true)
    }

    @Throws(Exception::class)
    fun send(text: String, pushTokens: List<String>, encode: Boolean) {

        val content = if (encode) URLEncoder.encode(text, "UTF-8") else text
        val url = if (firebase && legacy) endpointFMCLegacy else if (firebase) endpointFCM else endpointGCM

        val resp = if (firebase && !legacy) {
            sendFirebase(content, url, pushTokens)
        } else {
            sendLegacyOrGCM(content, url, pushTokens)
        }

        resp.log()
    }

    private fun sendFirebase(content: String, url: String, pushTokens: List<String>) : BatchResponse {

        // New Firebase API doesn't support multiple registration IDs in the same request anymore
        val payload = mapper.toJson(payload(content))
        Logger.getLogger(PushAndroid::class.java).debug("Sending push: $payload")

        return runBlocking {
            pushTokens.map { GlobalScope.async { Response(it, post(url, payload)) } }.awaitAll()
        }.run {
            BatchResponse(this)
        }

    }

    private fun sendLegacyOrGCM(content: String, url: String, pushTokens: List<String>) : Response {
        val payload = mapper.toJson(legacyPayload(content, pushTokens))
        Logger.getLogger(PushAndroid::class.java).debug("Sending push (legacy): $payload")

        return Response(pushTokens, post(url, payload))
    }

    private fun post(url: String, parameters: String): HttpsURLConnection? {
        return try {
            (URL(url).openConnection() as? HttpsURLConnection)?.apply {
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
                responseCode
            }
        } catch (e: IOException) {
            null
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

    private fun payload(content: String): Map<String, Any> {
        return HashMap<String, Any>().apply {
            this["message"] = HashMap<String, Any>().apply {
                this["data"] = HashMap<String, String>().apply {
                    this["message"] = content
                }
            }
        }
    }

    private fun getProjectId() : String? {
        return getResourceAsStream(servicesJson)?.run {
            JsonParser()
                .parse(reader())
                ?.asJsonObject
                ?.getAsJsonObject("project_info")
                ?.get("project_id")
                ?.asString
        }
    }


    private fun getAuthToken(): String {
        return GoogleCredentials
            .fromStream(getResourceAsStream(servicesJson))
            .createScoped(scope)
            .run {
                refreshIfExpired()
                accessToken.tokenValue
            }
    }

    private class Response(tokens: List<String>, con: HttpsURLConnection?) : Loggable {
        constructor(token: String, con: HttpsURLConnection?) : this(listOf(token), con)

        val tokens = tokens.joinToString()
        val responseCode = con?.responseCode
        val responseMessage = StringBuilder().run {
            con?.inputStream?.reader()?.readLines()?.forEach {
                this.append(it)
            }?.let {
                this.toString()
            }
        }

        override fun log() {
            Logger.getLogger(PushAndroid::class.java).debug(this.responseMessage)
        }
    }

    private class BatchResponse(responses: List<Response>) : Loggable {
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

        private fun Int?.success(): Boolean {
            return this?.run {this / 100 == 2} ?: false
        }
    }

    private interface Loggable {
        fun log()
    }

}