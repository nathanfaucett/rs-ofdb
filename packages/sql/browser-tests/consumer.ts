import {
  CommitError,
  Database,
  type DbValue,
  type QueryParams,
  type Result,
  type Row,
  type RowParser,
  SelectError,
  type SelectErrorReason,
} from "@ofsdk/sql";

export async function contract(): Promise<Result[]> {
  const db = await Database.open("consumer");
  const client = db.client();
  const clone = client.clone();
  const timed = clone.withDeadline(1000n);
  const value: DbValue = { type: "integer", value: "9223372036854775807" };
  const params: QueryParams = { id: value };
  const migration = await timed.migrate([
    {
      version: 1,
      description: "m1",
      sql: "CREATE TABLE items (id INTEGER PRIMARY KEY)",
    },
  ]);
  const version: number = migration.currentVersion;
  const applied: number[] = migration.appliedVersions;
  console.log(version, applied);
  const tx = await timed.transaction();
  try {
    const result = await tx.executeSql(
      "SELECT id FROM items WHERE id = :id",
      params,
    );
    await tx.commit();
    return result;
  } catch (error) {
    if (error instanceof CommitError) {
      const outcome: "unknown" = error.outcome;
      console.log(outcome);
    }
    throw error;
  } finally {
    tx.close();
    timed.close();
    clone.close();
    client.close();
    db.close();
  }
}

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
type Assert<T extends true> = T;

export async function typedReads(client: import("@ofsdk/sql").Client) {
  const parse = (row: Row) => {
    if (row["id"]?.type !== "integer") throw new Error("expected integer id");
    return { recordId: BigInt(row["id"].value) };
  };
  const parser: RowParser<{ recordId: bigint }> = parse;
  const rows = await client.select(
    "SELECT id FROM items WHERE id = $1",
    parser,
    [{ type: "integer", value: "1" }],
  );
  const inferred: Assert<Equal<typeof rows, { recordId: bigint }[]>> = true;
  const inline = await client.select("SELECT id FROM items", (row) => {
    const input: Assert<Equal<typeof row, Row>> = true;
    console.log(input);
    return { label: "entry", count: 1n };
  });
  const inferredInline: Assert<
    Equal<typeof inline, { label: string; count: bigint }[]>
  > = true;
  try {
    await client.select("SELECT id FROM items", parse);
  } catch (error) {
    if (error instanceof SelectError) {
      const reason: SelectErrorReason = error.reason;
      const rowIndex: number | undefined = error.rowIndex;
      const cause: unknown = error.cause;
      console.log(reason, rowIndex, cause);
    }
  }
  console.log(inferred, inferredInline);
  return rows;
}

export async function rejectedTypes(): Promise<void> {
  const db = await Database.open("types");
  const client = db.client();
  // @ts-expect-error Generated u64 binding requires bigint, not number.
  client.withDeadline(1000);
  // @ts-expect-error Values must be tagged; numbers cannot enter the value transport.
  await client.execute("INSERT INTO items VALUES ($1)", [9007199254740992]);
  await client.execute("INSERT INTO items VALUES ($1)", [
    // @ts-expect-error Integers use decimal strings, never JS numbers.
    { type: "integer", value: 1 },
  ]);
  // @ts-expect-error A parser callback is required, not query parameters.
  await client.select("SELECT id FROM items", []);
  // @ts-expect-error Parsers must be synchronous.
  await client.select("SELECT id FROM items", async () => ({ id: 1 }));
  // @ts-expect-error Thenable parser results are asynchronous too.
  await client.select("SELECT id FROM items", () => Promise.resolve(1));
  const sometimesAsync = () => (Math.random() > 0.5 ? 1 : Promise.resolve(1));
  // @ts-expect-error A parser cannot sometimes return a promise.
  await client.select("SELECT id FROM items", sometimesAsync);
  // @ts-expect-error A parser cannot require an application-specific input shape.
  await client.select("SELECT id FROM items", (row: { id: number }) => row.id);
  // @ts-expect-error The output must match the parser; it is not an unchecked generic cast.
  await client.select<string>("SELECT id FROM items", () => 1);
  // @ts-expect-error Query parameters must contain tagged cells.
  await client.select("SELECT id FROM items WHERE id = $1", () => 1, [1]);
  // @ts-expect-error The parser precedes parameters.
  await client.select("SELECT id FROM items", [], () => 1);
  await client.select("SELECT id FROM items", (row) => {
    // @ts-expect-error Parser input keys are readonly.
    row["id"] = { type: "null" };
    return row;
  });
  client.close();
  db.close();
}
