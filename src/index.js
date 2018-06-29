const Stream = require('stream');
const stringify = require('fast-safe-stringify');
const AWS = require('aws-sdk');

const internals = {
  defaults: {
    // Lower than high water mark, since on low throughput we still want to push early enough
    threshold: 20,
  },
};

function serialize(data) {
  if (typeof data === 'string') {
    return data;
  }

  if (Buffer.isBuffer(data)) {
    return data.toString('utf8');
  }

  const payload = Object.assign({}, data);
  // Add Kibana/Logstash compatible timestamp
  if (!payload['@timestamp']) {
    if (payload.timestamp) {
      payload['@timestamp'] = new Date(payload.timestamp).toISOString();
    } else {
      payload['@timestamp'] = new Date().toISOString();
    }
  }

  return `${stringify(payload)}\n`;
}

class GoodKinesis extends Stream.Writable {
  constructor(options) {
    super({
      // Switch stream to object mode
      objectMode: true,
      decodeStrings: false,
      // batch supports up to 500, a bit lower here (by guesstimate) to start sending earlier
      highWaterMark: 200,
    });
    if (!options.streamName) {
      throw new Error('Missing stream name');
    }

    this.client = new AWS.Kinesis(options);
    this.options = Object.assign({}, internals.defaults, options);

    this.client.describeStreamSummary({ StreamName: options.streamName }, (err, data) => {
      if (err) {
        this.emit('error', err);
        return;
      }

      if (data.StreamDescriptionSummary.StreamStatus !== 'ACTIVE'
          && data.StreamDescriptionSummary.StreamStatus !== 'UPDATING') {
        this.emit('error', new Error('Kinesis stream is not in status ACTIVE or UPDATING'));
      }
    });

    this.buffer = [];
    this.failureCount = 0;

    this.once('finish', () => {
      this.flush(() => {});
    });
  }

  _write(data, encoding, callback) {
    this.buffer.push(data);

    if (this.buffer.length >= this.options.threshold) {
      this.flush(callback);
    } else {
      setImmediate(callback);
    }
  }

  _writev(chunks, callback) {
    this.buffer = this.buffer.concat(chunks.map(chunk => chunk.chunk));
    this.flush(callback);
  }

  flush(callback) {
    if (this.buffer.length === 1) {
      const params = {
        DeliveryStreamName: this.options.streamName,
        Record: {
          StreamName: this.options.streamName,
          PartitionKey: Math.random().toString(36).substring(2),
          Data: serialize(this.buffer[0]),
        },
      };

      this.client.putRecord(params, callback);
    } else {
      // Chunk into 500s, which is the max supported by Firehose PutRecordBatch
      while (this.buffer.length > 0) {
        const params = {
          StreamName: this.options.streamName,
          Records: this.buffer.splice(0, 500).map(chunk => ({
            PartitionKey: Math.random().toString(36).substring(2),
            Data: serialize(chunk),
          })),
        };

        this.client.putRecords(params, callback);
      }
    }

    this.buffer = [];
  }
}

class GoodFirehose extends Stream.Writable {
  constructor(options) {
    super({
      // Switch stream to object mode
      objectMode: true,
      decodeStrings: false,
      // batch supports up to 500, a bit lower here (by guesstimate) to start sending earlier
      highWaterMark: 200,
    });
    if (!options.streamName) {
      throw new Error('Missing delivery stream name');
    }

    this.client = new AWS.Firehose(options);
    this.options = Object.assign({}, internals.defaults, options);

    this.client.describeDeliveryStream({ DeliveryStreamName: options.streamName }, (err, data) => {
      if (err) {
        this.emit('error', err);
        return;
      }

      if (data.DeliveryStreamDescription.DeliveryStreamStatus !== 'ACTIVE') {
        this.emit('error', new Error('Delivery stream is not in status ACTIVE'));
      }
    });

    this.buffer = [];
    this.failureCount = 0;

    this.once('finish', () => {
      this.flush(() => {});
    });
  }

  _write(data, encoding, callback) {
    this.buffer.push(data);

    if (this.buffer.length >= this.options.threshold) {
      this.flush(callback);
    } else {
      setImmediate(callback);
    }
  }

  _writev(chunks, callback) {
    this.buffer = this.buffer.concat(chunks.map(chunk => chunk.chunk));
    this.flush(callback);
  }

  flush(callback) {
    if (this.buffer.length === 1) {
      const params = {
        DeliveryStreamName: this.options.streamName,
        Record: {
          Data: serialize(this.buffer[0]),
        },
      };

      this.client.putRecord(params, callback);
    } else {
      // Chunk into 500s, which is the max supported by Firehose PutRecordBatch
      while (this.buffer.length > 0) {
        const params = {
          DeliveryStreamName: this.options.streamName,
          Records: this.buffer.splice(0, 500).map(chunk => ({ Data: serialize(chunk) })),
        };

        this.client.putRecordBatch(params, callback);
      }
    }

    this.buffer = [];
  }
}

module.exports = {
  Kinesis: GoodKinesis,
  Firehose: GoodFirehose,
};
