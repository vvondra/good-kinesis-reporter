/* global describe it afterEach */
/* eslint no-underscore-dangle: ["error", { "allow": ["_read"] }] */
/* eslint no-new: ["off"] */

const assert = require('assert');
const AWS = require('aws-sdk-mock');
const sinon = require('sinon');
const StreamTest = require('streamtest');
const goodkinesis = require('../src/index');

describe('GoodKinesisReporter', () => {
  afterEach(() => {
    AWS.restore();
  });

  describe('Kinesis module', () => {
    it('should write records', (done) => {
      const summaryResponse = {
        StreamDescriptionSummary: {
          StreamStatus: 'ACTIVE',
        },
      };
      const putRecordSpy = sinon.stub().yields(null, 'ok');

      AWS.mock('Kinesis', 'describeStreamSummary', summaryResponse);
      AWS.mock('Kinesis', 'putRecord', putRecordSpy);
      const kinesisStream = new goodkinesis.Kinesis({ streamName: 'vojtech-testing', region: 'eu-west-1' });

      const readable = StreamTest.v2.fromObjects([
        { id: 1, text: 'test write' },
        `${JSON.stringify({ id: 2, text: 'test write' })}\n`, // asume already applied by SafeJSON
        Buffer.from(`${JSON.stringify({ id: 3, text: 'test write' })}\n`, 'utf8'),
      ]);

      readable.pipe(kinesisStream);
      readable.on('end', () => {
        for (let i = 1; i < 4; i += 1) {
          sinon.assert.calledWithMatch(
            putRecordSpy,
            {
              Data: `{"id":${i},"text":"test write"}\n`,
            },
          );
        }

        assert.ok(putRecordSpy.calledThrice, 'should put records twice to Kinesis');
        done();
      });
    });

    it('should throw an error if stream name not supplied', () => {
      assert.throws(() => {
        new goodkinesis.Kinesis();
      });
    });

    it('should throw an error if stream not ready', () => {
      const summaryResponse = {
        StreamDescriptionSummary: {
          StreamStatus: 'CREATING',
        },
      };

      AWS.mock('Kinesis', 'describeStreamSummary', summaryResponse);

      assert.throws(() => {
        new goodkinesis.Kinesis({ streamName: 'vojtech-testing', region: 'eu-west-1' });
      });
    });
  });

  describe('Firehose module', () => {
    it('should write records', (done) => {
      const summaryResponse = {
        DeliveryStreamDescription: {
          DeliveryStreamStatus: 'ACTIVE',
        },
      };
      const putRecordSpy = sinon.stub().yields(null, 'ok');

      AWS.mock('Firehose', 'describeDeliveryStream', summaryResponse);
      AWS.mock('Firehose', 'putRecord', putRecordSpy);
      const kinesisStream = new goodkinesis.Firehose({ streamName: 'vojtech-testing', region: 'eu-west-1' });

      const readable = StreamTest.v2.fromObjects([
        { id: 1, text: 'test write' },
        JSON.stringify({ id: 2, text: 'test write' }),
      ]);

      readable.pipe(kinesisStream);
      readable.on('end', () => {
        assert.ok(putRecordSpy.calledTwice, 'should put records twice to Firehose');
        done();
      });
    });

    it('should batch write records', (done) => {
      const summaryResponse = {
        DeliveryStreamDescription: {
          DeliveryStreamStatus: 'ACTIVE',
        },
      };
      const putRecordSpy = sinon.stub().yields(null, 'ok');

      AWS.mock('Firehose', 'describeDeliveryStream', summaryResponse);
      AWS.mock('Firehose', 'putRecordBatch', putRecordSpy);
      const kinesisStream = new goodkinesis.Firehose({ streamName: 'vojtech-testing', region: 'eu-west-1' });
      kinesisStream.cork();

      const readable = StreamTest.v2.fromObjects([
        { id: 1, text: 'test write' },
        JSON.stringify({ id: 2, text: 'test write' }),
      ]);

      readable.pipe(kinesisStream);
      readable.on('end', () => {
        assert.ok(putRecordSpy.calledOnce, 'should put records once in Batch to Firehose');
        done();
      });

      // Not using nextTick since that's when the pipe happens causing a flappy test
      setTimeout(() => { kinesisStream.uncork(); }, 200);
    });

    it('should throw an error if stream name not supplied', () => {
      assert.throws(() => {
        new goodkinesis.Firehose();
      });
    });

    it('should throw an error if stream not ready', () => {
      const summaryResponse = {
        DeliveryStreamDescription: {
          DeliveryStreamStatus: 'DELETING',
        },
      };

      AWS.mock('Firehose', 'describeDeliveryStream', summaryResponse);

      assert.throws(() => {
        new goodkinesis.Firehose({ streamName: 'vojtech-testing', region: 'eu-west-1' });
      });
    });
  });
});
