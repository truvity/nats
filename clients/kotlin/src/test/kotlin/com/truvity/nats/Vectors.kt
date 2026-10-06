package com.truvity.nats

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.nio.file.Path

/** The shared JSON vectors, ../conformance/vectors relative to clients/kotlin (where Maven runs). */
internal fun readVectors(name: String): JsonNode =
    ObjectMapper().readTree(Files.readString(Path.of("..", "conformance", "vectors", name)))

internal fun checkAccountVectors() {
    val v = readVectors("account-for-namespace.json")
    val accounts = v["projectAccounts"].map { it.asText() }
    val cases = v["cases"]
    org.junit.jupiter.api.Assertions.assertTrue(cases.size() > 0)
    for (c in cases) {
        val ns = c["namespace"].asText()
        if (c["error"]?.asBoolean() == true) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException::class.java, { accountForNamespace(ns, accounts) }, "namespace \"$ns\"")
        } else {
            org.junit.jupiter.api.Assertions.assertEquals(c["account"].asText(), accountForNamespace(ns, accounts), "namespace \"$ns\"")
        }
    }
}

internal fun checkSubjectVectors() {
    val v = readVectors("subjects.json")
    org.junit.jupiter.api.Assertions.assertTrue(v["valid"].size() > 0)
    for (s in v["valid"]) {
        org.junit.jupiter.api.Assertions.assertTrue(isValidPublishSubject(s.asText()), "\"${s.asText()}\" is valid")
    }
    for (s in v["invalid"]) {
        org.junit.jupiter.api.Assertions.assertFalse(isValidPublishSubject(s.asText()), "\"${s.asText()}\" is invalid")
    }
    val tooLong = v["tooLong"].asInt()
    org.junit.jupiter.api.Assertions.assertTrue(isValidPublishSubject("a".repeat(tooLong - 1)))
    org.junit.jupiter.api.Assertions.assertFalse(isValidPublishSubject("a".repeat(tooLong)))
}
