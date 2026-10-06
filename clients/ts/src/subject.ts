/**
 * The broker's mapping rule: the account a client of a namespace lands in. A
 * namespace in `projectAccounts`, "emp-<slug>", "ci-<org>-<repo>" and exactly
 * "ci" each map to the account of the same name; anything else has none (this
 * throws). The callout applies the same rule; the shared vectors
 * (clients/conformance/vectors) keep every language and the callout agreeing.
 *
 * Subjects are not prefixed with the account: the account boundary is the isolation.
 */
export function accountForNamespace(namespace: string, projectAccounts: readonly string[]): string {
  if (projectAccounts.includes(namespace)) return namespace;
  if (namespace.startsWith("emp-") && namespace.length > 4) return namespace;
  if (namespace.startsWith("ci-")) {
    const rest = namespace.slice(3);
    const i = rest.indexOf("-");
    if (i > 0 && i < rest.length - 1) return namespace;
  }
  if (namespace === "ci") return namespace;
  throw new Error(`natsclient: namespace "${namespace}" has no NATS account mapping`);
}

/** The longest publish subject accepted, in bytes. */
export const MAX_SUBJECT_LENGTH = 255;

/**
 * Checks a subject a client publishes to: concrete (no wildcards), dot-separated
 * non-empty tokens of letters, digits, '-' and '_', and not in the broker's
 * reserved space ('$' prefix, "_INBOX."). Throws on a bad subject.
 */
export function validatePublishSubject(subject: string): void {
  if (subject === "") throw new Error("natsclient: subject is empty");
  if (Buffer.byteLength(subject) > MAX_SUBJECT_LENGTH) {
    throw new Error(`natsclient: subject is longer than ${MAX_SUBJECT_LENGTH} bytes`);
  }
  if (subject.startsWith("$") || subject.startsWith("_INBOX.")) {
    throw new Error(`natsclient: subject "${subject}" is in the broker's reserved space`);
  }
  for (const tok of subject.split(".")) {
    if (tok === "") throw new Error(`natsclient: subject "${subject}" has an empty token`);
    if (!/^[A-Za-z0-9_-]+$/.test(tok)) {
      throw new Error(
        `natsclient: subject "${subject}": a character is not allowed (no wildcards, spaces or other punctuation)`,
      );
    }
  }
}

/** Boolean form of {@link validatePublishSubject}. */
export function isValidPublishSubject(subject: string): boolean {
  try {
    validatePublishSubject(subject);
    return true;
  } catch {
    return false;
  }
}
