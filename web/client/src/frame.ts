import { type ViewerReply } from '@ursid/grid';

/** A binary frame's header: the viewer reply plus the view it is for */
export type FrameHeader = ViewerReply & { view: number };

/** A binary frame from the gateway, split into its parts (views into the frame's bytes) */
export type Frame = {
  header: FrameHeader;
  // Arrow IPC stream of the rows
  rows: Uint8Array;
  // The rows' keys, little-endian u64 each
  keys: Uint8Array;
};

const decoder = new TextDecoder();

/**
 * Split a binary frame: a little-endian u32 header length, the header (JSON),
 * a little-endian u32 rows length, the rows (Arrow IPC stream), then the keys
 * for the rest of the frame.
 */
export function parseFrame(data: ArrayBuffer): Frame {
  const view = new DataView(data);
  const headerLength = view.getUint32(0, true);
  const rowsAt = 4 + headerLength + 4;
  if (rowsAt > data.byteLength) {
    throw new Error(`frame of ${data.byteLength} bytes cut off in its header`);
  }
  const header = JSON.parse(
    decoder.decode(new Uint8Array(data, 4, headerLength))
  ) as FrameHeader;
  const rowsLength = view.getUint32(4 + headerLength, true);
  const keysAt = rowsAt + rowsLength;
  if (keysAt > data.byteLength || (data.byteLength - keysAt) % 8 !== 0) {
    throw new Error(
      `frame of ${data.byteLength} bytes does not hold ${rowsLength} bytes of rows and whole keys`
    );
  }
  return {
    header,
    rows: new Uint8Array(data, rowsAt, rowsLength),
    keys: new Uint8Array(data, keysAt),
  };
}
