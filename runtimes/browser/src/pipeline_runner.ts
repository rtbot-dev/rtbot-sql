import { Message } from "./types";

export interface PipelineRunner {
  deploy(programJson: string, programId: string): void;
  feed(programId: string, timestamp: number, values: number[]): Message[];
  runOnce(
    programId: string,
    timestamp: number,
    values: number[],
  ): Message[];
  destroy(programId: string): void;
}

/**
 * Stub implementation. Actual execution requires @rtbot-dev/wasm which
 * is not yet available. This throws on all operations to make it clear
 * that runtime execution is not yet supported.
 */
export class StubPipelineRunner implements PipelineRunner {
  deploy(_programJson: string, _programId: string): void {
    throw new Error(
      "PipelineRunner not available: @rtbot-dev/wasm dependency required",
    );
  }

  feed(
    _programId: string,
    _timestamp: number,
    _values: number[],
  ): Message[] {
    throw new Error(
      "PipelineRunner not available: @rtbot-dev/wasm dependency required",
    );
  }

  runOnce(
    _programId: string,
    _timestamp: number,
    _values: number[],
  ): Message[] {
    throw new Error(
      "PipelineRunner not available: @rtbot-dev/wasm dependency required",
    );
  }

  destroy(_programId: string): void {
    throw new Error(
      "PipelineRunner not available: @rtbot-dev/wasm dependency required",
    );
  }
}
