import {
  CatalogSnapshot,
  StreamSchema,
  ViewMeta,
  TableSchema,
} from "./types";
import { StringDictionary } from "./string_dictionary";

export class InMemoryCatalog {
  private streams = new Map<string, StreamSchema>();
  private views = new Map<string, ViewMeta>();
  private tables = new Map<string, TableSchema>();
  private dictionaries = new Map<string, StringDictionary>();

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

  getOrCreateDictionary(key: string): StringDictionary {
    let dict = this.dictionaries.get(key);
    if (!dict) {
      dict = new StringDictionary();
      this.dictionaries.set(key, dict);
    }
    return dict;
  }

  lookupDictionary(key: string): StringDictionary | undefined {
    return this.dictionaries.get(key);
  }

  drop(name: string): boolean {
    const prefix = name + ".";
    for (const key of this.dictionaries.keys()) {
      if (key.startsWith(prefix)) {
        this.dictionaries.delete(key);
      }
    }
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

    const dicts: Record<string, Record<string, string>> = {};
    for (const [k, v] of this.dictionaries) {
      if (!v.isEmpty) {
        dicts[k] = v.toRecord();
      }
    }
    if (Object.keys(dicts).length > 0) {
      snap.dictionaries = dicts;
    }

    return snap;
  }
}
