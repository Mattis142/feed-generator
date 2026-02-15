import { spawn, execSync, ChildProcess } from 'child_process'
import fs from 'fs'
import path from 'path'
import dotenv from 'dotenv'

const ENV_PATH = path.join(__dirname, '../.env')

const RECONNECT_DELAY_MS = 5000
const HEALTH_CHECK_INTERVAL_MS = 30000
const TUNNEL_TIMEOUT_MS = 15000
const MIN_TUNNEL_UPTIME_MS = 10000

let serverProcess: ChildProcess | null = null
let serverStarted = false
let tunnelProcess: ChildProcess | null = null
let currentHostname = ''
let lastHealthCheck = 0
let tunnelStartTime = 0
let lastReconnectAttempt = 0

const run = async () => {
    console.log('🚀 Starting Turbo Dev Mode with Auto-Reconnect...')

    // 1. Kill existing processes on port 3000 and any ssh tunnels (only on first run)
    console.log('🧹 Cleaning up old processes...')
    try {
        execSync("lsof -i :3000 -t | xargs kill -9", { stdio: 'ignore' })
        execSync("ps aux | grep 'ngrok' | grep -v grep | awk '{print $2}' | xargs kill -9", { stdio: 'ignore' })
    } catch (e) {
        // Ignore errors if no processes found
    }

    const startTunnel = (isReconnect: boolean) => {
        if (tunnelProcess) {
            tunnelProcess.kill()
            tunnelProcess = null
        }

        if (isReconnect) {
            console.log(`\n🔄 Tunnel dropped. Reconnecting in ${RECONNECT_DELAY_MS / 1000}s...`)
        } else {
            console.log('🚇 Opening tunnel to localhost:3000...')
        }

        tunnelStartTime = Date.now()
        tunnelProcess = spawn('ngrok', ['http', '3000', '--log=stdout'], {
            stdio: ['ignore', 'pipe', 'pipe']
        })

        let hostname = ''
        let tunnelReady = false

        const onTunnelReady = (matchedHostname: string) => {
            hostname = matchedHostname
            const oldHostname = currentHostname
            currentHostname = hostname
            tunnelReady = true
            
            if (oldHostname && oldHostname !== hostname) {
                console.log(`\n🔄 Hostname changed: ${oldHostname} → ${hostname}`)
            } else {
                console.log(`\n✅ Tunnel active: ${hostname}`)
            }
            
            updateEnv(hostname)
            publishFeed().then(() => {
                if (!serverStarted) {
                    startServer()
                    serverStarted = true
                } else {
                    console.log('🛰️ Server already running on port 3000, feed re-published with new URL.')
                }
                startHealthCheck()
            }).catch(err => {
                console.error('❌ Failed to publish feed:', err)
            })
        }

        if (tunnelProcess.stdout) {
            tunnelProcess.stdout.on('data', (data) => {
                const output = data.toString()
                process.stdout.write(output) // Forward tunnel logs

                // Ngrok URL pattern: https://<random>.ngrok.io, https://<random>.ngrok-free.app, or https://<random>.ngrok-free.dev
                const match = output.match(/https:\/\/([a-z0-9-]+\.ngrok(-free)?\.(app|dev|io))/)
                if (match && !hostname) {
                    onTunnelReady(match[1])
                }
            })
        }

        if (tunnelProcess.stderr) {
            tunnelProcess.stderr.on('data', (data) => {
                const errorStr = data.toString()
                // Only log non-critical ngrok warnings
                if (!errorStr.includes('lvl=info') && !errorStr.includes('lvl=debug')) {
                    console.error(`Tunnel Error: ${errorStr}`)
                }
                // Check for connection errors that might require immediate reconnection
                if (errorStr.includes('connection reset') || 
                    errorStr.includes('Broken pipe') ||
                    errorStr.includes('Connection refused') ||
                    errorStr.includes('Network is unreachable') ||
                    errorStr.includes('client session ended')) {
                    console.log('🔍 Detected critical connection issue, preparing to reconnect...')
                }
            })
        }

        tunnelProcess.on('close', (code) => {
            console.log(`\n⚠️ Tunnel closed with code ${code}`)
            tunnelReady = false
            if (tunnelProcess) {
                // Debounce reconnections - don't reconnect if we just tried
                const now = Date.now()
                if (now - lastReconnectAttempt > RECONNECT_DELAY_MS) {
                    lastReconnectAttempt = now
                    // Auto-reconnect: spawn a new tunnel after a delay
                    setTimeout(() => startTunnel(true), RECONNECT_DELAY_MS)
                } else {
                    console.log('🔄 Reconnection already in progress, waiting...')
                }
            }
        })

        // Add timeout protection
        setTimeout(() => {
            if (!tunnelReady && tunnelProcess) {
                console.log('⏰ Tunnel setup timeout, forcing reconnect...')
                tunnelProcess.kill()
                setTimeout(() => startTunnel(true), 1000)
            }
        }, TUNNEL_TIMEOUT_MS)
    }

    let healthCheckActive = false

    const startHealthCheck = () => {
        if (healthCheckActive) return
        healthCheckActive = true
        
        const checkTunnel = () => {
            if (!tunnelProcess || !currentHostname) return

            // Don't health check if tunnel hasn't been up long enough
            const uptime = Date.now() - tunnelStartTime
            if (uptime < MIN_TUNNEL_UPTIME_MS) {
                return
            }

            // Check if tunnel process is still alive first
            if (tunnelProcess.killed) {
                console.log('🔍 Tunnel process already killed, skipping health check')
                return
            }

            // Simple health check - try to curl the tunnel URL with longer timeout
            const check = spawn('curl', ['-s', '--max-time', '10', `https://${currentHostname}`], {
                stdio: 'ignore'
            })

            let finished = false

            check.on('close', (code) => {
                if (finished) return // Prevent multiple calls
                finished = true

                // Double-check that tunnel is still alive before killing it
                if (!tunnelProcess || tunnelProcess.killed) {
                    return
                }

                if (code !== 0) {
                    console.log(`🔍 Tunnel health check failed for ${currentHostname}, reconnecting...`)
                    if (tunnelProcess && !tunnelProcess.killed) {
                        tunnelProcess.kill()
                    }
                } else {
                    lastHealthCheck = Date.now()
                    const uptimeSecs = Math.floor(uptime / 1000)
                    console.log(`💚 Tunnel healthy (${uptimeSecs}s uptime) - ${currentHostname}`)
                }
            })

            // Add timeout to prevent hanging
            setTimeout(() => {
                if (!finished) {
                    finished = true
                    console.log(`🔍 Tunnel health check timed out for ${currentHostname}, reconnecting...`)
                    if (tunnelProcess && !tunnelProcess.killed) {
                        tunnelProcess.kill()
                    }
                }
            }, 12000) // 12 seconds total timeout
        }

        // Run health checks periodically
        setInterval(checkTunnel, HEALTH_CHECK_INTERVAL_MS)
        console.log(`🔍 Started health monitoring (every ${HEALTH_CHECK_INTERVAL_MS / 1000}s)`)
    }

    // Graceful shutdown
    const gracefulShutdown = () => {
        console.log('\n🛑 Shutting down gracefully...')
        if (tunnelProcess) {
            tunnelProcess.kill()
            tunnelProcess = null
        }
        if (serverProcess) {
            serverProcess.kill()
            serverProcess = null
        }
        process.exit(0)
    }

    process.on('SIGINT', gracefulShutdown)
    process.on('SIGTERM', gracefulShutdown)

    startTunnel(false)
}

const updateEnv = (hostname: string) => {
    console.log(`📝 Updating .env with FEEDGEN_HOSTNAME=${hostname}...`)
    const envContent = fs.readFileSync(ENV_PATH, 'utf-8')
    const updatedContent = envContent.replace(/FEEDGEN_HOSTNAME=.*/, `FEEDGEN_HOSTNAME=${hostname}`)
    fs.writeFileSync(ENV_PATH, updatedContent)
}

const publishFeed = async () => {
    console.log('📡 Publishing feed to Bluesky...')
    return new Promise((resolve, reject) => {
        const publish = spawn('npx', ['ts-node', 'scripts/publishFeedNonInteractive.ts'], {
            stdio: 'inherit'
        })
        publish.on('close', (code) => {
            if (code === 0) {
                console.log('✅ Feed published successfully.')
                // Wait for DID resolution to propagate (important for localhost.run)
                console.log('⏳ Waiting for DID resolution to propagate...')
                setTimeout(() => {
                    console.log('✅ DID resolution should now be active.')
                    resolve(true)
                }, 5000) // 5 second delay for DID propagation
            } else {
                reject(new Error(`Publishing failed with code ${code}`))
            }
        })
    })
}

const startServer = () => {
    if (serverProcess) {
        serverProcess.kill()
    }
    console.log('🛰️ Starting Feed Generator server...')
    serverProcess = spawn('npx', ['ts-node', 'src/index.ts'], {
        stdio: 'inherit',
        env: { ...process.env, ...dotenv.parse(fs.readFileSync(ENV_PATH)) }
    })

    serverProcess.on('error', (err) => {
        console.error('Failed to start server:', err)
    })
}

run().catch((err) => {
    console.error('💥 Turbo Dev failed:', err)
    process.exit(1)
})
