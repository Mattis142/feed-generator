import { spawn, execSync, ChildProcess } from 'child_process'
import fs from 'fs'
import path from 'path'
import dotenv from 'dotenv'

const ENV_PATH = path.join(__dirname, '../.env')

const run = async () => {
    console.log('🚀 Starting Turbo Dev Mode...')

    // 1. Kill existing processes on port 3000 and any ssh tunnels
    console.log('🧹 Cleaning up old processes...')
    try {
        execSync("lsof -i :3000 -t | xargs kill -9", { stdio: 'ignore' })
        execSync("ps aux | grep 'ssh -R' | grep 'lhr.life' | grep -v grep | awk '{print $2}' | xargs kill -9", { stdio: 'ignore' })
    } catch (e) {
        // Ignore errors if no processes found
    }

    // 2. Start the tunnel
    console.log('🚇 Opening tunnel to localhost:3000...')
    const tunnel = spawn('ssh', ['-R', '80:localhost:3000', 'nokey@localhost.run'], {
        stdio: ['ignore', 'pipe', 'pipe']
    })

    let hostname = ''

    // Parse hostname from tunnel output
    tunnel.stdout.on('data', (data) => {
        const output = data.toString()
        process.stdout.write(output) // Forward tunnel logs

        const match = output.match(/https:\/\/([a-z0-9]+\.lhr\.life)/)
        if (match && !hostname) {
            hostname = match[1]
            console.log(`\n✅ Tunnel active: ${hostname}`)

            // 3. Update .env
            updateEnv(hostname)

            // 4. Publish Feed
            publishFeed().then(() => {
                // 5. Start the server
                startServer()
            })
        }
    })

    tunnel.stderr.on('data', (data) => {
        console.error(`Tunnel Error: ${data}`)
    })

    tunnel.on('close', (code) => {
        console.log(`Tunnel closed with code ${code}`)
        process.exit(code || 0)
    })
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
                resolve(true)
            } else {
                reject(new Error(`Publishing failed with code ${code}`))
            }
        })
    })
}

let serverProcess: ChildProcess | null = null

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
