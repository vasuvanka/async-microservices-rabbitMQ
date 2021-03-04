const path = require('path');
require('dotenv').config({ path: path.resolve(process.cwd(), '.env') });

const rabmq = require('amqplib');
const { exchnage_name, queue_req, queue_res } = require('./constants');

async function setup() {
    console.log("Setting up RabbitMQ Exchanges/Queues");
    const conn = await rabmq.connect(process.env.CLOUD_AMQP_URL)

    const channel = await conn.createChannel()

    // create exchange
    await channel.assertExchange(exchnage_name, "direct", { durable: true });

    // create queues
    await channel.assertQueue(queue_req, { durable: true });
    await channel.assertQueue(queue_res, { durable: true });

    // bind queues
    await channel.bindQueue(queue_req, exchnage_name, "request");
    await channel.bindQueue(queue_res, exchnage_name, "response");

    console.log("Setup DONE");
    process.exit();
}

setup()