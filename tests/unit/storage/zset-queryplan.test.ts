import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage, ZSET_RANGE_SQL } from "../../../src/storage/sqlite";

/**
 * Performance regression guard (plan §1): every zset range/rank statement must
 * run as a range scan on idx_zset_range (or the PK), never a full table scan
 * or a temp B-tree sort. EXPLAIN assertions are deterministic where timing
 * tests are flaky — this is what pins the "no SPOP-style O(n) trap" contract.
 */

let store: SqliteStorage;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
});
afterEach(() => store.close());

function planFor(sql: string): string {
  return store.explainQueryPlan(sql).join("\n");
}

function expectIndexedNoSort(sql: string): void {
  const plan = planFor(sql);
  expect(plan).toMatch(/USING (COVERING )?INDEX idx_zset_range/);
  expect(plan).not.toContain("USE TEMP B-TREE");
  expect(plan).not.toMatch(/SCAN zset_members(?! USING)/);
}

describe("zset query plans", () => {
  test("ZRANGE (rank, ascending) walks idx_zset_range without sorting", () => {
    expectIndexedNoSort(ZSET_RANGE_SQL.byRankAsc);
  });

  test("ZREVRANGE walks idx_zset_range backwards without sorting", () => {
    expectIndexedNoSort(ZSET_RANGE_SQL.byRankDesc);
  });

  test("ZRANGEBYSCORE is an index range scan for every bound shape", () => {
    for (const minOp of [">", ">="] as const) {
      for (const maxOp of ["<", "<="] as const) {
        expectIndexedNoSort(ZSET_RANGE_SQL.byScore(minOp, maxOp));
      }
    }
  });

  test("ZRANK predecessor count is an index range scan", () => {
    const plan = planFor(ZSET_RANGE_SQL.rank);
    expect(plan).toMatch(/USING (COVERING )?INDEX idx_zset_range/);
    expect(plan).not.toMatch(/SCAN zset_members(?! USING)/);
  });
});
