package br.com.simpli.ws

import com.google.gson.Gson
import org.apache.log4j.Logger
import java.io.BufferedReader
import java.io.DataOutputStream
import java.io.InputStreamReader
import java.net.URL
import java.net.URLEncoder
import java.util.HashMap
import javax.net.ssl.HttpsURLConnection

class PushAndroid {
    val instanceKeyAndroidConsole: String
    val useFirebase: Boolean

    constructor(instanceKeyAndroidConsole: String) : this(instanceKeyAndroidConsole, false)

    constructor(instanceKeyAndroidConsole: String, useFirebase: Boolean) {
        this.instanceKeyAndroidConsole = instanceKeyAndroidConsole
        this.useFirebase = useFirebase
    }

    private val urlGcm = "https://android.googleapis.com/gcm/send"
    private val urlFcm = "https://fcm.googleapis.com/fcm/send"
    private val gson = Gson()

    fun send(text: String, vararg registrationIds: String) {
        send(text, registrationIds.toList())
    }

    fun send(text: String, registrationIds: List<String>) {
        send(text, registrationIds, true)
    }

    @Throws(Exception::class)
    fun send(text: String, registrationIds: List<String>, encode: Boolean) {

        var text = text

        val url: String = if (useFirebase) urlFcm else urlGcm

        val obj = URL(url)
        val con = obj.openConnection() as HttpsURLConnection

        //add request header
        con.requestMethod = "POST"
        con.setRequestProperty("Content-Type", "application/json")
        con.setRequestProperty("Authorization", "key=$instanceKeyAndroidConsole")

        val message = HashMap<String, String>()

        if (encode) {
            text = URLEncoder.encode(text, "UTF-8")
        }

        message["message"] = text
        val parameters = HashMap<String, Any>()
        parameters["registration_ids"] = registrationIds
        parameters["data"] = message

        val urlParameters = gson.toJson(parameters)
        Logger.getLogger(PushAndroid::class.java).debug("Enviando Push: $urlParameters")

        // Send post request
        con.doOutput = true
        val wr = DataOutputStream(con.outputStream)
        wr.writeBytes(urlParameters)
        wr.flush()
        wr.close()

        val responseCode = con.responseCode
        Logger.getLogger(PushAndroid::class.java).debug("Push Response Code:$responseCode")

        val isn = BufferedReader(InputStreamReader(con.inputStream))
        var inputLine = isn.readLine()
        val response = StringBuffer()

        while (inputLine != null) {
            response.append(inputLine)
            inputLine = isn.readLine()
        }

        isn.close()

        Logger.getLogger(PushAndroid::class.java).debug(response)
    }
}