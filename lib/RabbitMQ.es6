"use strict";

import * as amqp from "amqplib";
import {EventEmitter} from "events";

export class RabbitMQ extends EventEmitter {

  constructor(rabbitConfig, loggerInstance) {

    super();

    if (!rabbitConfig || !rabbitConfig.url) {
      throw new Error("Missing RabbitMq Dependencies");
    }

    /** @member {string} config for rabbit mq. */
    this.rabbitConfig_ = rabbitConfig;

    /** @member {string} url to connect with rabbit mq. */
    this.connectionUrl_ = rabbitConfig.url;

    /** @member {string} url to connect with rabbit mq. */
    this.connection_ = null;

    /** @member {string} url to connect with rabbit mq. */
    this.channel_ = null;

    /** @member {string} url to connect with rabbit mq. */
    this.connectionOptions_ = rabbitConfig.options || {};

    this.queueOptions_ = {
      "durable": true,
      "autoDelete": false,
      "arguments": {
        "messageTtl": 24 * 60 * 60 * 1000,
        "maxLength": 1000,
        "deadLetterExchange": `deadLetter${rabbitConfig.queueName}`
      }
    };

    this.exchangeOptions_ = {
      "durable": true,
      "autoDelete": false,
      "alternateExchange": "",
      "arguments": {}
    };

    /** @member {Object} logger instance for logging. */
    this.logger_ = loggerInstance;

    this.setChannelPrefetch();
  }

  connect() {
    this.connection_ = this.connection_ || amqp.connect(this.connectionUrl_, this.connectionOptions_);
    this.logger_.debug(`${RabbitMQ.name}.connect(), Creating New Connection ===> `);
    return this.connection_;
  }

  createChannel() {

    this.channel_ = this.channel_ ||
      this.connect()
        .then(conn => {
          this.logger_.debug(`${RabbitMQ.name}.createChannel(), Creating New Channel ==> `);
          return conn.createChannel();
        })
        .catch(err => {
          this.logger_.debug("Error in creating RabbitMQ Channel ", err);
        });

    return this.channel_;
  }

  setChannelPrefetch() {

    this.createChannel()
      .then(channel => {

        this.logger_.debug(`${RabbitMQ.name}.setChannelPrefetch(): Initializing channel prefetch ==> `);
        channel.prefetch(this.rabbitConfig_.prefetchCount, false);
        channel.on("close", this.channelClose_);

        this.logger_.debug("Prefetch count: %s successfully set for channel", this.rabbitConfig_.prefetchCount);
      });
  }

  publish(message, queue = null) {

    let {queueName, exchangeName, exchangeType} = this.rabbitConfig_;

    queueName = queue || queueName;

    return this.createChannel()
      .then(channel => {
        this.logger_.debug(`${RabbitMQ.name}.publish(): Publishing the message in queue: ${queueName} `);

        channel.prefetch(this.rabbitConfig_.prefetchCount, false);

        channel.assertExchange(exchangeName, exchangeType, this.exchangeOptions_);

        channel.assertQueue(queueName, this.queueOptions_)
          .then(queueRes => {
            channel.bindQueue(queueRes.queue, exchangeName, queueRes.queue);

            channel.publish(exchangeName, queueRes.queue, new Buffer(message));
            this.logger_.debug(" [x] Sent %s", message);
          });
      })
      .catch(err => {
        this.logger_.debug("Error in publishing the message", err);
        this.emit("error", err);
      });
  }

  consume(queue = null) {

    let {queueName, exchangeName, exchangeType} = this.rabbitConfig_;

    queueName = queue || queueName;

    return this.createChannel()
      .then(channel => {

        this.logger_.debug(`${RabbitMQ.name}.consume(): Consuming the message from queue: ${queueName} `);

        channel.prefetch(this.rabbitConfig_.prefetchCount, false);

        channel.assertExchange(exchangeName, exchangeType, this.exchangeOptions_);

        channel.assertQueue(queueName, this.queueOptions_)
          .then(queueRes => {

            channel.bindQueue(queueRes.queue, exchangeName, queueRes.queue);

            channel.consume(queueRes.queue, msg => {
              this.logger_.debug("Consuming Message...", msg.content.toString());

              this.emit("msgReceived", msg);

            }, {"noAck": false});

          });
      })
      .catch(err => {
        this.logger_.debug("Error in consuming the message", err);
        this.emit("error", err);
      });
  }

  acknowledgeMessage(msg, allUpTo = false) {

    return this.createChannel()
      .then(channel => {
        channel.ack(msg, allUpTo);
        this.logger_.debug("Message has been acknowledged... ", msg.content.toString());
      });
  }

  channelClose_(error) {
    console.log("======on channel close==============================> ", error);
    this.channel_ = null;
    this.connection_ = null;
  }

}

// let config = {
//     "url": "amqp://integration:integration@10.18.6.109:5672",
//     "queueName": "test-canta",
//     "exchangeName": "test-cantaHealth",
//     "exchangeType": "direct",
//     "prefetchCount": 1,
//     "options": {}
//   },
//   rabbit = new RabbitMQ(config),
//   count = 0;
//
// setInterval(() => {
//
//   rabbit.publish(JSON.stringify({"msg": `Hello This is cantahealth ${count} !!!!`}));
//   count++;
//
// }, 3000);
//
// rabbit.on("msgReceived", msg => {
//   console.log("In Ack=======================================>", msg.content.toString());
//
//   rabbit.acknowledgeMessage(msg);
// });
//
// rabbit.consume();
