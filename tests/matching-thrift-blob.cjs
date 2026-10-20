// Go's task-list metadata decoder for the production MySQL Matching gate.
//
// Cadence stores task-list metadata as a ThriftRW payload. ThriftRW is the
// Thrift *binary* protocol, not the compact one, and the field ids are Go's
// `sqlblobs.TaskListInfo`: kind 10, ack level 12, expiry 14, last update 16
// and the optional adaptive partition config 18.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const THRIFT = { STOP: 0, BOOL: 2, BYTE: 3, DOUBLE: 4, I16: 6, I32: 8, I64: 10, STRING: 11, STRUCT: 12, MAP: 13, SET: 14, LIST: 15 };

function thriftValue(reader, type) {
  switch (type) {
    case THRIFT.BOOL: return reader.i8() !== 0;
    case THRIFT.BYTE: return reader.i8();
    case THRIFT.I16: return reader.i16();
    case THRIFT.I32: return reader.i32();
    case THRIFT.I64: return reader.i64();
    case THRIFT.DOUBLE: return reader.f64();
    case THRIFT.STRING: return reader.binary();
    case THRIFT.STRUCT: return thriftStruct(reader);
    case THRIFT.MAP: {
      // ThriftRW writes the key type, the value type and then the size.
      const keyType = reader.i8();
      const valueType = reader.i8();
      const size = reader.i32();
      const map = {};
      for (let index = 0; index < size; index += 1) {
        map[String(thriftValue(reader, keyType))] = thriftValue(reader, valueType);
      }
      return map;
    }
    case THRIFT.SET:
    case THRIFT.LIST: {
      // ThriftRW writes the element type before the size.
      const elementType = reader.i8();
      const size = reader.i32();
      const values = [];
      for (let index = 0; index < size; index += 1) values.push(thriftValue(reader, elementType));
      return values;
    }
    default: throw new Error(`unsupported ThriftRW type ${type}`);
  }
}

function thriftStruct(reader) {
  const fields = {};
  for (;;) {
    const type = reader.i8();
    if (type === THRIFT.STOP) break;
    const id = reader.i16();
    fields[id] = thriftValue(reader, type);
  }
  return fields;
}

function thriftReader(buffer) {
  let offset = 0;
  const take = count => {
    const slice = buffer.subarray(offset, offset + count);
    offset += count;
    return slice;
  };
  return {
    i8: () => take(1).readInt8(0),
    i16: () => take(2).readInt16BE(0),
    i32: () => take(4).readInt32BE(0),
    i64: () => take(8).readBigInt64BE(0),
    f64: () => take(8).readDoubleBE(0),
    binary: () => {
      const length = take(4).readInt32BE(0);
      return Buffer.from(take(length));
    },
  };
}

function decodeTaskListBlob(hex) {
  const fields = thriftStruct(thriftReader(Buffer.from(hex, 'hex')));
  return {
    kind: fields[10] === undefined ? null : Number(fields[10]),
    ackLevel: fields[12] === undefined ? null : Number(fields[12]),
    expiryNanos: fields[14] === undefined ? null : String(fields[14]),
    lastUpdatedNanos: fields[16] === undefined ? null : String(fields[16]),
    hasAdaptivePartitionConfig: fields[18] !== undefined,
  };
}

// The decoder decides pass/fail for the production GC gate, so it is checked
// against the committed Go/Rust blob fixtures before it is used.
function verifyTaskListBlobDecoder(fixturesPath = path.join(__dirname, 'fixtures/matching-blobs.json')) {
  const fixtures = JSON.parse(fs.readFileSync(fixturesPath, 'utf8'));
  const cases = fixtures.filter(entry => entry.kind === 'task_list');
  if (cases.length < 1) throw new Error('no task-list blob fixtures are available for the decoder self-check');
  for (const entry of cases) {
    const decoded = decodeTaskListBlob(entry.thrift_hex);
    assert.equal(decoded.kind, entry.data.kind, 'decoded task-list kind does not match the fixture');
    assert.equal(decoded.ackLevel, entry.data.ack_level, 'decoded task-list ack level does not match the fixture');
    assert.equal(Number(decoded.expiryNanos), entry.data.expiry_nanos, 'decoded task-list expiry does not match the fixture');
    assert.equal(Number(decoded.lastUpdatedNanos), Number(entry.data.last_updated_nanos), 'decoded task-list last update does not match the fixture');
  }
  return cases.length;
}

module.exports = { THRIFT, decodeTaskListBlob, thriftReader, thriftStruct, thriftValue, verifyTaskListBlobDecoder };
