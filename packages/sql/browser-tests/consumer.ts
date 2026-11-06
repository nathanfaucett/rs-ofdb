import {
  CommitError,
  Database,
  type DbValue,
  type QueryParams,
  type Result,
} from "@ofsdk/sql";

export async function contract(): Promise<Result[]> {
  const db = await Database.open("consumer");
  const client = db.client();
  const clone = client.clone();
  const timed = clone.withDeadline(1000n);
  const value: DbValue = { type: "integer", value: "9223372036854775807" };
  const params: QueryParams = { id: value };
  const migration = await timed.migrate([
    { version: 1, description: "m1", sql: "CREATE TABLE items (id INTEGER PRIMARY KEY)" },
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
  client.close();
  db.close();
}
