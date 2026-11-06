import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

// Faults enter at the existing public constructor's generated-binding boundary.
test("select rejects impossible result counts and malformed transport before parsing", async ({
  page,
}) => {
  const results = await page.evaluate(async () => {
    const { Client, SelectError, Error: SqlError } = (window as any).sql;
    const malformed = [
      "not json",
      "{}",
      '[{"columns":[1],"rows":[]}]',
      '[{"columns":["secret"],"rows":[[]]}]',
      '[{"columns":["secret"],"rows":[[{"type":"integer","value":"9223372036854775808"}]]}]',
      '[{"columns":["secret"],"rows":[[{"type":"unknown","value":"private-cell"}]]}]',
    ];
    const results = [];
    for (const raw of ["[]", ...malformed]) {
      let calls = 0;
      let freed = 0;
      const client = new Client({
        execute: async () => raw,
        free: () => {
          freed++;
        },
      });
      let failure;
      try {
        await client.select("private-sql", () => {
          calls++;
        });
      } catch (error: any) {
        failure = {
          typed: error instanceof SqlError,
          mapping: error instanceof SelectError,
          reason: error.reason,
        };
      } finally {
        client.close();
      }
      results.push({ calls, freed, failure });
    }
    return results;
  });
  expect(results[0]).toEqual({
    calls: 0,
    freed: 1,
    failure: { typed: true, mapping: true, reason: "result-count" },
  });
  for (const result of results.slice(1)) {
    expect(result).toEqual({
      calls: 0,
      freed: 1,
      failure: { typed: true, mapping: false, reason: undefined },
    });
  }
});

test("select preserves the exact execution error and arbitrary thrown parser causes", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Client, Error: SqlError, SelectError } = (window as any).sql;
    const executionError = new SqlError(
      "timeout",
      "existing execution failure",
    );
    const failedClient = new Client({
      execute: async () => {
        throw executionError;
      },
      free() {},
    });
    let sameError = false;
    try {
      await failedClient.select("private-sql", () => null);
    } catch (error) {
      sameError = error === executionError;
    } finally {
      failedClient.close();
    }
    const causes = [
      undefined,
      null,
      "private-cell",
      { secret: "private-cell" },
    ];
    const failures = [];
    for (const cause of causes) {
      const client = new Client({
        execute: async () => '[{"columns":[],"rows":[[]]}]',
        free() {},
      });
      try {
        await client.select("private-sql", () => {
          throw cause;
        });
      } catch (error: any) {
        failures.push(
          error instanceof SelectError &&
            error.reason === "parser" &&
            error.rowIndex === 0 &&
            error.cause === cause &&
            !/private/.test(error.message),
        );
      } finally {
        client.close();
      }
    }
    return { sameError, failures };
  });
  expect(result).toEqual({
    sameError: true,
    failures: [true, true, true, true],
  });
});
