const { Kafka, CompressionTypes, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = 'hyperweave-rc.dodd.svc.cluster.local.'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`hyperweave-rc-0.${host}:9092`, `hyperweave-rc-1.${host}:9092`],
  clientId: 'example-producer',
})

const topic = 'topic-admin-test-0'
const producer = kafka.producer()

const getRandomNumber = () => Math.round(Math.random() * 1000)
const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
  headers: {
    'correlation-id': `${num}-${Date.now()}`,
  },
})

let msgNumber = 0
let requestNumber = 0
const sendMessage = () => {
  const messages = Array(getRandomNumber())
    .fill()
    .map(_ => createMessage(getRandomNumber()))

  const requestId = requestNumber++
  msgNumber += messages.length
  kafka.logger().info(`Sending ${messages.length} messages #${requestId}...`)
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages,
    })
    .then(response => {
      kafka.logger().info(`Messages sent #${requestId}`, {
        response,
        msgNumber,
      })
    })
    .catch(e => kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack }))
}

let intervalId
const run = async () => {
  await producer.connect()
  intervalId = setInterval(sendMessage, 3000)
}

run().catch(e => kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/producer] disconnecting')
    clearInterval(intervalId)
    await producer.disconnect()
  })
})
