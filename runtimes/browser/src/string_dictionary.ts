/**
 * Bidirectional string ↔ number dictionary for TEXT column encoding.
 *
 * Mirrors the Java/Python/C++ StringDictionary implementations:
 * - encode(str) returns existing ID or assigns next sequential ID (starting at 1)
 * - decode(id) returns string or undefined
 * - lookup(str) returns ID or undefined
 * - putMapping(id, str) directly inserts (for syncing from C++ compiler)
 */
export class StringDictionary {
  private strToId = new Map<string, number>();
  private idToStr = new Map<number, string>();
  private nextId = 1;

  encode(s: string): number {
    const existing = this.strToId.get(s);
    if (existing !== undefined) return existing;
    const id = this.nextId++;
    this.strToId.set(s, id);
    this.idToStr.set(id, s);
    return id;
  }

  decode(id: number): string | undefined {
    return this.idToStr.get(id);
  }

  lookup(s: string): number | undefined {
    return this.strToId.get(s);
  }

  putMapping(id: number, s: string): void {
    if (this.strToId.has(s) || this.idToStr.has(id)) return;
    this.strToId.set(s, id);
    this.idToStr.set(id, s);
    if (id >= this.nextId) {
      this.nextId = id + 1;
    }
  }

  get size(): number {
    return this.strToId.size;
  }

  get isEmpty(): boolean {
    return this.strToId.size === 0;
  }

  allEntries(): Map<number, string> {
    return new Map(this.idToStr);
  }

  restore(serialized: Record<string, string>): void {
    this.strToId.clear();
    this.idToStr.clear();
    this.nextId = 1;
    for (const [idStr, s] of Object.entries(serialized)) {
      const id = Number(idStr);
      if (Number.isNaN(id)) throw new Error(`invalid dictionary ID: ${idStr}`);
      this.strToId.set(s, id);
      this.idToStr.set(id, s);
      if (id >= this.nextId) {
        this.nextId = id + 1;
      }
    }
  }

  toRecord(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const [id, s] of this.idToStr) {
      out[String(id)] = s;
    }
    return out;
  }
}
