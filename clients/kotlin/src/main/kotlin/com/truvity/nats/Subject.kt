package com.truvity.nats

/**
 * The broker's mapping rule: the account a client of a namespace lands in. A namespace in
 * [projectAccounts], `emp-<slug>`, `ci-<org>-<repo>` and exactly `ci` each map to the account of
 * the same name; anything else has none ([IllegalArgumentException]). The callout applies the
 * same rule; the shared vectors (clients/conformance/vectors) keep every language and the callout
 * agreeing.
 *
 * Subjects are not prefixed with the account: the account boundary is the isolation.
 */
fun accountForNamespace(namespace: String, projectAccounts: Collection<String>): String {
    if (namespace in projectAccounts) return namespace
    if (namespace.startsWith("emp-") && namespace.length > "emp-".length) return namespace
    if (namespace.startsWith("ci-")) {
        val rest = namespace.removePrefix("ci-")
        val i = rest.indexOf('-')
        if (i > 0 && i < rest.length - 1) return namespace
    }
    if (namespace == "ci") return namespace
    throw IllegalArgumentException("nats-client: namespace \"$namespace\" has no NATS account mapping")
}

/** The longest publish subject accepted, in bytes. */
const val MAX_SUBJECT_LENGTH = 255

/**
 * Checks a subject a client publishes to: concrete (no wildcards), dot-separated non-empty tokens
 * of letters, digits, `-` and `_`, at most 255 bytes, and not in the broker's reserved space (`$`
 * prefix, `_INBOX.`). Throws [IllegalArgumentException] otherwise.
 */
fun validPublishSubject(subject: String) {
    require(subject.isNotEmpty()) { "nats-client: subject is empty" }
    require(subject.toByteArray(Charsets.UTF_8).size <= MAX_SUBJECT_LENGTH) {
        "nats-client: subject is longer than $MAX_SUBJECT_LENGTH bytes"
    }
    require(!subject.startsWith("$") && !subject.startsWith("_INBOX.")) {
        "nats-client: subject \"$subject\" is in the broker's reserved space"
    }
    for (tok in subject.split('.')) {
        require(tok.isNotEmpty()) { "nats-client: subject \"$subject\" has an empty token" }
        for (ch in tok) {
            val ok = ch in 'a'..'z' || ch in 'A'..'Z' || ch in '0'..'9' || ch == '-' || ch == '_'
            require(ok) {
                "nats-client: subject \"$subject\": character '$ch' is not allowed (no wildcards, spaces or other punctuation)"
            }
        }
    }
}

/** The boolean form of [validPublishSubject]. */
fun isValidPublishSubject(subject: String): Boolean = runCatching { validPublishSubject(subject) }.isSuccess

/** A stream may bind a wildcard subject; only the reserved space is refused. */
internal fun validStreamSubject(s: String) {
    require(s.isNotEmpty() && !s.startsWith("$")) {
        "nats-client: stream subject \"$s\" is empty or in the broker's reserved space"
    }
}
