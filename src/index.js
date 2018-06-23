'use strict'

const Stream = require('stream');
const stringify = require('fast-safe-stringify');
const crypto = require('crypto');
const AWS = require('aws-sdk');

function serialize(data) {
  if (typeof data === 'string') {
    return data;
  }

  if (Buffer.isBuffer(data)) {
    return data.toString('utf8');
  }

  return stringify(data) + "\n";
}

class GoodKinesis extends Stream.Writable {
  constructor(options) {
    super({ objectMode: true, decodeStrings: false });
    if (!options.streamName) {
      throw new Error('Missing stream name');
    }

    this.client = new AWS.Kinesis(options);
    this.options = options;

    this.client.describeStreamSummary({ StreamName: options.streamName }, (err, data) => {
      if (err) {
        this.emit('error', err);
        return;
      }

      if (data.StreamDescriptionSummary.StreamStatus != 'ACTIVE' &&
          data.StreamDescriptionSummary.StreamStatus != 'UPDATING') {
        this.emit('error', new Error('Kinesis stream is not in status ACTIVE or UPDATING'));
      }
    });
  }

  _write(data, encoding, callback) {
    crypto.randomBytes(64, (err, buf) => {
      if (err) {
        callback(err);
        return;
      }

      const params = {
        StreamName: this.options.streamName,
        PartitionKey: buf.toString('hex'),
        Data: serialize(data)
      };
      this.client.putRecord(params, callback)
    });
  }
}

class GoodFirehose extends Stream.Writable {
  constructor(options) {
    super({ objectMode: true, decodeStrings: false });
    if (!options.streamName) {
      throw new Error('Missing delivery stream name');
    }

    this.client = new AWS.Firehose(options);
    this.options = options;

    this.client.describeDeliveryStream({ DeliveryStreamName: options.streamName }, (err, data) => {
      if (err) {
        this.emit('error', err);
        return;
      }

      if (data.DeliveryStreamDescription.DeliveryStreamStatus != 'ACTIVE') {
        this.emit('error', new Error('Delivery stream is not in status ACTIVE'));
      }
    });
  }

  _write(data, encoding, callback) {
    const params = {
      DeliveryStreamName: this.options.streamName,
      Record: {
        Data: serialize(data)
      }
    };

    this.client.putRecord(params, callback)
  }
}

module.exports = {
  'Kinesis': GoodKinesis,
  'Firehose': GoodFirehose,
};
