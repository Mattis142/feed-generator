import dotenv from 'dotenv'
import FeedGenerator from './server'
import { logger } from './logger'

const run = async () => {
  dotenv.config()
  const hostname = maybeStr(process.env.FEEDGEN_HOSTNAME) ?? 'example.com'
  const serviceDid =
    maybeStr(process.env.FEEDGEN_SERVICE_DID) ?? `did:web:${hostname}`
  const server = FeedGenerator.create({
    port: maybeInt(process.env.FEEDGEN_PORT) ?? 3000,
    listenhost: maybeStr(process.env.FEEDGEN_LISTENHOST) ?? 'localhost',
    postgresConnectionString: (process.env.USE_REMOTE_DB === 'true' && process.env.POSTGRES_CONNECTION_STRING_REMOTE) ? process.env.POSTGRES_CONNECTION_STRING_REMOTE : (maybeStr(process.env.POSTGRES_CONNECTION_STRING) ?? 'postgresql://bsky:bskypassword@localhost:5432/repo'),
    subscriptionEndpoint:
      maybeStr(process.env.FEEDGEN_SUBSCRIPTION_ENDPOINT) ??
      'wss://bsky.network',
    publisherDid:
      maybeStr(process.env.FEEDGEN_PUBLISHER_DID) ?? 'did:example:alice',
    subscriptionReconnectDelay:
      maybeInt(process.env.FEEDGEN_SUBSCRIPTION_RECONNECT_DELAY) ?? 3000,
    hostname,
    serviceDid,
    whitelist: (process.env.FEEDGEN_WHITELIST ?? '').split(',').filter(Boolean),
  })
  await server.start()
  console.log(
    `🤖 running feed generator HTTP server at http://${server.cfg.listenhost}:${server.cfg.port}`,
  )
}

const maybeStr = (val?: string) => {
  if (!val) return undefined
  return val
}

const maybeInt = (val?: string) => {
  if (!val) return undefined
  const int = parseInt(val, 10)
  if (isNaN(int)) return undefined
  return int
}

run().catch(err => {
  logger.error('FATAL STARTUP ERROR', err)
  process.exit(1)
})
