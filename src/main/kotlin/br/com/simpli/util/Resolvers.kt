package br.com.simpli.util

import com.amazonaws.auth.*
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.util.ClassLoaderHelper

internal fun resolveRegion(enum: Regions?, string: String?): Regions {
    // Tries to resolve given Region enum first, then string, then default provider
    return enum ?: string?.run {
        try {
            Regions.fromName(this.toLowerCase().replace('_', '-'))
        } catch (e: IllegalArgumentException) {
            null
        }
    } ?: Regions.fromName(DefaultAwsRegionProviderChain().region)
}

internal fun resolveCredentials(path: String?): AWSCredentialsProvider {
    // If path is given, tries to get from path first
    return path?.run {
        val pathAdjusted = if (!startsWith('/')) "/$this" else this

        try {
            val properties = ClassLoaderHelper.getResourceAsStream(pathAdjusted)
            AWSStaticCredentialsProvider(PropertiesCredentials(properties))
        } catch (e: Exception) {
            null
        }
    } ?: DefaultAWSCredentialsProviderChain()
}