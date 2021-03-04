const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const amqp = require('amqplib');
const bodyParser = require('body-parser')
const express = require('express')
const { uniq } = require('@vasuvanka/uniq');
const { queue_res, exchnage_name, routing_key_req } = require('../constants');
const app = express()
const rabbitMQ = {
    connection: null
}

async function connect() {
    rabbitMQ.connection = await amqp.connect(process.env.CLOUD_AMQP_URL)
    return "connected"
}


app.use(bodyParser.json())

app.get("/", (req, res) => {
    res.json({
        message: 'welcome to rabbitMQ test'
    })
})

app.post("/", async (req, res) => {
    const { body } = req;
    id = uniq(10)
    const data = { id, data: body }
    try {
        let channel = await rabbitMQ.connection.createConfirmChannel();
        console.log("Published a request message, requestId:", id);
        await publishToChannel(channel, { routingKey: routing_key_req, exchangeName: exchnage_name, data });
        res.json(data)
    } catch (err) {
        res.sendStatus(500).send({
            message: err
        })
    }
})

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

async function listenForResults() {
    console.log("listening for incoming messages");
    try {
        // create a channel and prefetch 1 message at a time
        const connection = await amqp.connect(process.env.CLOUD_AMQP_URL)
        let channel = await connection.createChannel();
        await channel.prefetch(1);

        // start consuming messages
        await consume({ connection, channel });
    } catch (err) {
        console.log(err);
    }
}

// consume messages from RabbitMQ
function consume({ connection, channel, resultsChannel }) {
    return new Promise((resolve, reject) => {
        channel.consume(queue_res, async function (msg) {
            try {
                let msgBody = msg.content.toString();
                let data = JSON.parse(msgBody);
                console.log("Received a result message, requestId:", data.id, "processingResults:", data);

                // acknowledge message as received
                await channel.ack(msg);
            } catch (err) {
                console.log(err);
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


app.listen(3000)

connect().then().catch(console.error)
listenForResults()