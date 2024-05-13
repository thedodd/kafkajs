const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = 'hyperweave-rc.dodd.svc.cluster.local.'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`hyperweave-rc-0.${host}:9092`, `hyperweave-rc-1.${host}:9092`],
  clientId: 'test-admin-id',
})

const topic = 'topic-admin-test-0'

const admin = kafka.admin()

const run = async () => {
  await admin.connect()
  const res0 = await admin.createTopics({
    topics: [{ topic, numPartitions: 2, replicationFactor: 0 }],
    waitForLeaders: true,
  })
  kafka.logger().info(`${JSON.stringify(res0)}`)
  const res = await admin.fetchTopicMetadata({ topics: [topic] })
  kafka.logger().info(`${JSON.stringify(res)}`)
  //  await admin.createPartitions({
  //    topicPartitions: [{ topic: topic, count: 3 }],
  //  })
}

run().catch(e => kafka.logger().error(`[example/admin] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await admin.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/admin] disconnecting')
    await admin.disconnect()
  })
})
