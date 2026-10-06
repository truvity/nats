import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { accountForNamespace, isValidPublishSubject } from "../src/index.js";

const vector = <T>(name: string): T =>
  JSON.parse(readFileSync(new URL(`../../conformance/vectors/${name}`, import.meta.url), "utf8")) as T;

describe("shared vectors", () => {
  it("accountForNamespace agrees with the vectors", () => {
    const v = vector<{
      projectAccounts: string[];
      cases: { namespace: string; account?: string; error?: boolean }[];
    }>("account-for-namespace.json");
    expect(v.cases.length).toBeGreaterThan(0);
    for (const c of v.cases) {
      if (c.error) expect(() => accountForNamespace(c.namespace, v.projectAccounts), c.namespace).toThrow();
      else expect(accountForNamespace(c.namespace, v.projectAccounts), c.namespace).toBe(c.account);
    }
  });

  it("publish subjects agree with the vectors", () => {
    const v = vector<{ valid: string[]; invalid: string[]; tooLong: number }>("subjects.json");
    for (const s of v.valid) expect(isValidPublishSubject(s), s).toBe(true);
    for (const s of v.invalid) expect(isValidPublishSubject(s), JSON.stringify(s)).toBe(false);
    expect(isValidPublishSubject("a".repeat(v.tooLong - 1))).toBe(true);
    expect(isValidPublishSubject("a".repeat(v.tooLong))).toBe(false);
  });
});
