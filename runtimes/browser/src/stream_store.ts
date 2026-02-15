import { Message } from "./types";

export type WatchCallback = (msg: Message) => void;

export class InMemoryStreamStore {
  private store = new Map<string, Message[]>();
  private watchers = new Map<string, Set<WatchCallback>>();

  append(stream: string, msg: Message): void {
    let arr = this.store.get(stream);
    if (!arr) {
      arr = [];
      this.store.set(stream, arr);
    }
    arr.push(msg);

    const callbacks = this.watchers.get(stream);
    if (callbacks) {
      for (const cb of callbacks) cb(msg);
    }
  }

  read(stream: string): Message[] {
    return this.store.get(stream) ?? [];
  }

  watch(stream: string, callback: WatchCallback): () => void {
    let set = this.watchers.get(stream);
    if (!set) {
      set = new Set();
      this.watchers.set(stream, set);
    }
    set.add(callback);

    return () => {
      set!.delete(callback);
      if (set!.size === 0) this.watchers.delete(stream);
    };
  }

  clear(stream: string): void {
    this.store.delete(stream);
  }
}
