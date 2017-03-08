const net = require('net')
const kafka = require('kafka-node')

const client = new kafka.Client('localhost:2181', 'test-client')
const producer = new kafka.Producer(client)
producer.addListener('ready', () => {
  console.log('Kafka producer is ready')
})

producer.createTopics(['new-test'], true, () => {})

const server = net.createServer((sock) => {
  sock.on('data', dataHandler)
})

function dataHandler(data) {
  payload = [
    { 
      topic: 'new-test',
      messages: data,
      partition: 0
    }
  ]
  producer.send(payload, (err, data) => {
    if (err) {
      console.log('err', err)
    } else {
      console.log('Message queued...')
    }
  })
}

server.listen(1337)

const consumer = new kafka.Consumer(client, [ { topic: 'new-test', partition: 0 } ])

consumer.on('message', (message) => {
  console.log('message:', message)
})
