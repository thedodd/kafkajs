const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = 'hyperweave-rc.dodd.svc.cluster.local.'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`hyperweave-rc-0.${host}:9092`, `hyperweave-rc-1.${host}:9092`],
  clientId: 'example-consumer',
})

// const topic = 'testing.snapshots'
const topic = 'topic-admin-test-0'
const consumer = kafka.consumer({ groupId: 'example-consumer-0' })

let msgNumber = 0
const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      msgNumber++
      console.log(`--- ${message.timestamp}\n${message.value.toString()}`)
    },
  })
}

run().catch(e => kafka.logger().error(`[example/consumer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/consumer] disconnecting')
    await consumer.disconnect()
  })
})
