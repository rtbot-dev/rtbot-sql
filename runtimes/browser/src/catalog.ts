import {
  CatalogSnapshot,
  StreamSchema,
  ViewMeta,
  TableSchema,
} from "./types";

export class InMemoryCatalog {
  private streams = new Map<string, StreamSchema>();
  private views = new Map<string, ViewMeta>();
  private tables = new Map<string, TableSchema>();

  registerStream(name: string, schema: StreamSchema): void {
    this.streams.set(name, schema);
  }

  registerView(name: string, meta: ViewMeta): void {
    this.views.set(name, meta);
  }

  registerTable(name: string, schema: TableSchema): void {
    this.tables.set(name, schema);
  }

  lookupStream(name: string): StreamSchema | undefined {
    return this.streams.get(name);
  }

  lookupView(name: string): ViewMeta | undefined {
    return this.views.get(name);
  }

  lookupTable(name: string): TableSchema | undefined {
    return this.tables.get(name);
  }

  drop(name: string): boolean {
    return (
      this.streams.delete(name) ||
      this.views.delete(name) ||
      this.tables.delete(name)
    );
  }

  snapshot(): CatalogSnapshot {
    const snap: CatalogSnapshot = { streams: {}, views: {}, tables: {} };
    for (const [k, v] of this.streams) snap.streams[k] = v;
    for (const [k, v] of this.views) snap.views[k] = v;
    for (const [k, v] of this.tables) snap.tables[k] = v;
    return snap;
  }
}
