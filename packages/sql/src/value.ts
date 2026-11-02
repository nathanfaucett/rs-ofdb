/** Any JSON-serializable value the database can store or return. */
export type DbValue =
  | string
  | number
  | boolean
  | null
  | DbValue[]
  | { [key: string]: DbValue };
