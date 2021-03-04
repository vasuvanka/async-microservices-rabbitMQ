const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const amqp = require('amqplib');
const { queue_req, exchnage_name, routing_key_res } = require('../constants');

async function listenForMessages() {
    // connect to Rabbit MQ
    let connection = await amqp.connect(process.env.CLOUD_AMQP_URL);

    // create a channel and prefetch 1 message at a time
    let channel = await connection.createChannel();
    await channel.prefetch(1);

    // create a second channel to send back the results
    let resultsChannel = await connection.createConfirmChannel();

    // start consuming messages
    await consume({ connection, channel, resultsChannel });
}

// utility function to publish messages to a channel
function publishToChannel(channel, { routingKey, exchangeName, data }) {
    return new Promise((resolve, reject) => {
        channel.publish(exchangeName, routingKey, Buffer.from(JSON.stringify(data), 'utf-8'), { persistent: true }, function (err, ok) {
            if (err) {
                return reject(err);
            }

            resolve();
        })
    });
}

// consume messages from RabbitMQ
function consume({ connection, channel, resultsChannel }) {
    return new Promise((resolve, reject) => {
        channel.consume(queue_req, async function (msg) {

            try {
                let msgBody = msg.content.toString();
                // parse message
                const data = JSON.parse(msgBody);
                console.log("Received a request message, requestId:", data.id);

                // process data
                const newData = await processMessage(data);

                // publish results to channel
                await publishToChannel(resultsChannel, {
                    exchangeName: exchnage_name,
                    routingKey: routing_key_res,
                    data: newData
                });
                console.log("Published results for requestId:", data.id);

                // acknowledge message as processed successfully
                await channel.ack(msg);
            } catch (err) {
                console.log(err.message);
            }

        });

        // handle connection closed
        connection.on("close", (err) => {
            return reject(err);
        });

        // handle errors
        connection.on("error", (err) => {
            return reject(err);
        });
    });
}

// simulate data processing that takes 5 seconds
function processMessage(data) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            data.message = "ack"
            resolve(data)
        }, 5000);
    });
}

listenForMessages();