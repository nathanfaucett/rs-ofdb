import { expect, test } from "./test.js";

test("named transaction parameters, nested JSON numbers, and executeSql aliases stay lossless", async ({
  page,
}) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
  const value = {
    type: "json",
    value: {
      type: "array",
      value: [
        { type: "i64", value: "-9223372036854775808" },
        { type: "f64", value: "8000000000000000" },
        { type: "null" },
        { type: "bool", value: true },
        { type: "array", value: [] },
        { type: "object", value: {} },
      ],
    },
  };
  const result = await page.evaluate(async (value) => {
    const db = await (window as any).sql.Database.open("named");
    const client = db.client();
    await client.execute_raw(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, data JSON)",
    );
    const tx = await client.transaction();
    try {
      await tx.execute_raw("INSERT INTO items VALUES (:id,:data)", {
        id: { type: "integer", value: "9007199254740993" },
        data: value,
      });
      let rejected = false;
      try {
        await tx.execute("INSERT INTO items VALUES (:id,:data)", {
          id: { type: "integer", value: 1 },
          data: value,
        });
      } catch {
        rejected = true;
      }
      const visible = await tx.execute_raw(
        "SELECT id,data FROM items WHERE id = :id",
        { id: { type: "integer", value: "9007199254740993" } },
      );
      await tx.commit();
      return {
        rejected,
        visible,
        committed: await client.execute_raw("SELECT id,data FROM items"),
      };
    } finally {
      tx.close();
      client.close();
      db.close();
    }
  }, value);
  expect(result.rejected).toBe(true);
  expect(result.visible).toEqual(result.committed);
  expect(result.committed[0].rows).toEqual([
    [{ type: "integer", value: "9007199254740993" }, value],
  ]);
});
