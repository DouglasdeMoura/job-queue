import { EventEmitter } from 'node:events'
import { randomUUID } from 'node:crypto'
import { DatabaseSync } from 'node:sqlite'
import type { Logger } from 'pino'
import type { Storage } from './types.ts'
import { abstractLogger } from '../utils/logging.ts'
import { StorageError } from '../errors.ts'

const SCHEMA_VERSION = 1

const CLEANUP_LOCK_KEY = 'cleanup-leader'
const CLEANUP_LOCK_TTL_MS = 10_000
const CLEANUP_ACQUIRE_RETRY_MS = 5_000

// busy_timeout stays at 100ms (Rails 8 default is 5000ms). With node:sqlite's
// synchronous API, a multi-second busy_timeout would stall the Node event loop
// under writer contention; ED1 in the design review pairs this short timeout
// with an in-process write mutex + jittered retry.
const DEFAULT_BUSY_TIMEOUT_MS = 100
const DEFAULT_CLEANUP_INTERVAL_MS = 30_000
const DEFAULT_VACUUM_INTERVAL_MS = 24 * 60 * 60 * 1000
// Caps the on-disk size of the WAL file after checkpoint. Without it, the WAL
// can grow unbounded between checkpoints.
const DEFAULT_JOURNAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024
// Memory-mapped I/O for SQLite reads.
const DEFAULT_MMAP_SIZE_BYTES = 128 * 1024 * 1024
// Page cache size in number of pages (positive value).
// With the SQLite 4 KiB default page size this is ~8 MiB.
const DEFAULT_CACHE_SIZE_PAGES = 2000

const DEFAULT_PRAGMAS: Record<string, string | number> = {
  journal_mode: 'WAL',
  synchronous: 'NORMAL',
  busy_timeout: DEFAULT_BUSY_TIMEOUT_MS,
  journal_size_limit: DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
  mmap_size: DEFAULT_MMAP_SIZE_BYTES,
  cache_size: DEFAULT_CACHE_SIZE_PAGES,
  temp_store: 'MEMORY'
}

const WRITE_RETRY_BASE_MS = 5
const WRITE_RETRY_MAX_MS = 50
const WRITE_RETRY_ATTEMPTS = 5

interface DequeueWaiter {
  workerId: string
  resolve: (value: Buffer | null) => void
  timeoutId: ReturnType<typeof setTimeout>
}

interface VacuumOption {
  enabled: boolean
  intervalMs: number
}

interface SQLiteStorageConfig {
  /**
   * Database path. Use ':memory:' for an in-memory database (default).
   * For persistence pass an explicit filesystem path. No silent cwd writes.
   */
  path?: string

  /**
   * Table name prefix.
   * Default: 'jq_'.
   */
  tablePrefix?: string

  /**
   * Background cleanup interval in milliseconds. Pass false to disable
   * (useful if you run cleanup externally via SQL).
   * Default: 30000.
   */
  cleanupIntervalMs?: number | false

  /**
   * Periodic VACUUM / pragma optimize cadence.
   * Pass false to disable.
   * Default: { enabled: true, intervalMs: 24 * 60 * 60 * 1000 }.
   */
  vacuum?: VacuumOption | false

  /**
   * PRAGMA overrides merged over the defaults.
   * Useful for tuning busy_timeout, cache_size, mmap_size, etc.
   */
  pragmas?: Record<string, string | number>

  /**
   * Pino logger for structured operational log lines.
   */
  logger?: Logger
}

function toBuffer (value: unknown): Buffer {
  if (Buffer.isBuffer(value)) return value
  if (value instanceof Uint8Array) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength)
  }
  if (typeof value === 'string') return Buffer.from(value)
  throw new StorageError(`SQLiteStorage: expected Buffer/Uint8Array, got ${typeof value}`)
}

function sleep (ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * SQLite storage implementation.
 *
 * Single-process only. Uses node:sqlite (built in since Node 22.5) with WAL mode.
 * Cross-process use is intentionally unsupported: every call asserts that the
 * caller's process.pid matches the pid at connect(), and throws StorageError
 * otherwise. For multi-process queues, use PgStorage.
 *
 * Notification semantics: subscribeToJob handlers must run in the same process
 * as the publisher. SQLite has no LISTEN/NOTIFY equivalent; we use an in-process
 * EventEmitter and document the contract explicitly.
 *
 * Atomic dequeue uses BEGIN IMMEDIATE; in-process writers are serialized through
 * an async mutex to avoid event-loop stalls when contention would otherwise let
 * busy_timeout block the synchronous driver.
 */
export class SQLiteStorage implements Storage {
  #path: string
  #tablePrefix: string
  #cleanupIntervalMs: number | false
  #vacuum: VacuumOption | false
  #pragmas: Record<string, string | number>
  #logger: Logger

  #db: DatabaseSync | null = null
  #connectedPid: number | null = null

  #eventEmitter = new EventEmitter({ captureRejections: true })
  #notifyEmitter = new EventEmitter({ captureRejections: true })
  #dequeueWaiters: DequeueWaiter[] = []

  #cleanupInterval: ReturnType<typeof setInterval> | null = null
  #leadershipTimer: ReturnType<typeof setInterval> | null = null
  #vacuumInterval: ReturnType<typeof setInterval> | null = null
  #writeMutex: Promise<void> = Promise.resolve()

  #instanceId = randomUUID()
  #isCleanupLeader = false

  // Namespace support — shares the database handle with a root instance.
  #parentStorage: SQLiteStorage | null = null
  #refCount = 0

  // Table names (computed from prefix).
  #jobsTable: string
  #queueTable: string
  #processingTable: string
  #resultsTable: string
  #errorsTable: string
  #workersTable: string
  #locksTable: string
  #metaTable: string

  constructor (config: SQLiteStorageConfig = {}) {
    this.#path = config.path ?? ':memory:'
    this.#tablePrefix = config.tablePrefix ?? 'jq_'

    this.#cleanupIntervalMs = config.cleanupIntervalMs ?? DEFAULT_CLEANUP_INTERVAL_MS
    this.#vacuum =
      config.vacuum === undefined ? { enabled: true, intervalMs: DEFAULT_VACUUM_INTERVAL_MS } : config.vacuum

    this.#pragmas = { ...DEFAULT_PRAGMAS, ...(config.pragmas ?? {}) }
    this.#logger = (config.logger ?? abstractLogger).child({
      component: 'sqlite-storage',
      tablePrefix: this.#tablePrefix
    })

    this.#jobsTable = `${this.#tablePrefix}jobs`
    this.#queueTable = `${this.#tablePrefix}queue`
    this.#processingTable = `${this.#tablePrefix}processing`
    this.#resultsTable = `${this.#tablePrefix}results`
    this.#errorsTable = `${this.#tablePrefix}errors`
    this.#workersTable = `${this.#tablePrefix}workers`
    this.#locksTable = `${this.#tablePrefix}locks`
    this.#metaTable = `${this.#tablePrefix}meta`

    this.#eventEmitter.setMaxListeners(0)
    this.#notifyEmitter.setMaxListeners(0)
  }

  // ═══════════════════════════════════════════════════════════════════
  // LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  async connect (): Promise<void> {
    if (this.#parentStorage) {
      if (this.#db) return // already connected
      this.#parentStorage.#refCount++
      await this.#parentStorage.connect()
      this.#db = this.#parentStorage.#db
      this.#connectedPid = this.#parentStorage.#connectedPid
      this.#createSchema()
      return
    }

    if (this.#db) return // idempotent

    try {
      this.#db = new DatabaseSync(this.#path)
    } catch (err) {
      const error = err as NodeJS.ErrnoException
      throw new StorageError(`SQLiteStorage: failed to open '${this.#path}': ${error.message}`, error)
    }
    this.#connectedPid = process.pid

    this.#applyPragmas()
    this.#assertWalActive()
    this.#createSchema()
    this.#checkSchemaVersion()
    this.#startCleanupLeaderLoop()
    this.#startVacuumLoop()
  }

  async disconnect (): Promise<void> {
    if (this.#parentStorage) {
      this.#clearDequeueWaiters()
      this.#eventEmitter.removeAllListeners()
      this.#notifyEmitter.removeAllListeners()
      this.#db = null
      this.#connectedPid = null
      this.#parentStorage.#refCount--
      return
    }

    if (this.#refCount > 0) return // children still connected

    if (this.#leadershipTimer) {
      clearInterval(this.#leadershipTimer)
      this.#leadershipTimer = null
    }
    if (this.#cleanupInterval) {
      clearInterval(this.#cleanupInterval)
      this.#cleanupInterval = null
    }
    if (this.#vacuumInterval) {
      clearInterval(this.#vacuumInterval)
      this.#vacuumInterval = null
    }

    if (this.#isCleanupLeader) {
      try {
        await this.releaseLeaderLock(CLEANUP_LOCK_KEY, this.#instanceId)
      } catch {
        // best-effort
      }
      this.#isCleanupLeader = false
    }

    this.#clearDequeueWaiters()
    this.#eventEmitter.removeAllListeners()
    this.#notifyEmitter.removeAllListeners()

    if (this.#db) {
      try {
        this.#db.close()
      } catch {
        // best-effort
      }
      this.#db = null
    }
    this.#connectedPid = null
  }

  #applyPragmas (): void {
    const db = this.#db!
    for (const [key, value] of Object.entries(this.#pragmas)) {
      const formattedValue = typeof value === 'string' ? value : String(value)
      db.exec(`PRAGMA ${key} = ${formattedValue}`)
    }
  }

  #assertWalActive (): void {
    if (this.#path === ':memory:') return // WAL not applicable for :memory:
    const requestedJournalMode = String(this.#pragmas.journal_mode ?? '').toLowerCase()
    if (requestedJournalMode !== 'wal') return
    const row = this.#db!.prepare('PRAGMA journal_mode').get() as { journal_mode?: string } | undefined
    const actual = row?.journal_mode?.toLowerCase()
    if (actual !== 'wal') {
      this.#logger.warn(
        { requested: 'wal', actual },
        'SQLiteStorage: WAL mode requested but not active. This usually means the path is on a filesystem that does not support WAL (NFS, some bind-mounts). Cleanup and dequeue throughput will be reduced.'
      )
    }
  }

  #createSchema (): void {
    const db = this.#db!
    db.exec(`
      CREATE TABLE IF NOT EXISTS "${this.#jobsTable}" (
        id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        expires_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS "${this.#queueTable}" (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        message BLOB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS "${this.#processingTable}" (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        worker_id TEXT NOT NULL,
        message BLOB NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#processingTable}_worker_idx"
        ON "${this.#processingTable}" (worker_id);
      CREATE TABLE IF NOT EXISTS "${this.#resultsTable}" (
        id TEXT PRIMARY KEY,
        data BLOB NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#resultsTable}_expires_idx"
        ON "${this.#resultsTable}" (expires_at);
      CREATE TABLE IF NOT EXISTS "${this.#errorsTable}" (
        id TEXT PRIMARY KEY,
        data BLOB NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#errorsTable}_expires_idx"
        ON "${this.#errorsTable}" (expires_at);
      CREATE TABLE IF NOT EXISTS "${this.#workersTable}" (
        worker_id TEXT PRIMARY KEY,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#workersTable}_expires_idx"
        ON "${this.#workersTable}" (expires_at);
      CREATE TABLE IF NOT EXISTS "${this.#locksTable}" (
        lock_key TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS "${this.#metaTable}" (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `)
  }

  #checkSchemaVersion (): void {
    const db = this.#db!
    const row = db.prepare(`SELECT value FROM "${this.#metaTable}" WHERE key = 'schema_version'`).get() as
      | { value?: string }
      | undefined

    if (!row) {
      db.prepare(`INSERT OR IGNORE INTO "${this.#metaTable}" (key, value) VALUES ('schema_version', ?)`).run(
        String(SCHEMA_VERSION)
      )
      return
    }

    const found = parseInt(row.value ?? '0', 10)
    if (Number.isNaN(found) || found > SCHEMA_VERSION) {
      throw new StorageError(
        `SQLiteStorage: database schema version ${row.value ?? '(invalid)'} is not supported ` +
          `by this library (supports schema v${SCHEMA_VERSION}). Downgrade is not supported.`
      )
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // INTERNAL HELPERS
  // ═══════════════════════════════════════════════════════════════════

  #assertConnected (): DatabaseSync {
    if (!this.#db) {
      throw new StorageError('SQLiteStorage: not connected. Call connect() first.')
    }
    if (this.#connectedPid !== null && process.pid !== this.#connectedPid) {
      throw new StorageError(
        'SQLiteStorage: detected use from a forked process ' +
          `(connected pid=${this.#connectedPid}, current pid=${process.pid}). ` +
          'SQLiteStorage is single-process only; forked children must call connect() after fork(). ' +
          'For multi-process queues, use PgStorage.'
      )
    }
    return this.#db
  }

  /**
   * Serializes write operations through an async mutex and retries with jitter
   * on SQLITE_BUSY. Reads do NOT take the mutex; SQLite handles concurrent reads
   * via WAL.
   */
  async #runWrite<T> (fn: () => T): Promise<T> {
    const release = await this.#acquireMutex()
    try {
      let lastError: unknown
      for (let attempt = 0; attempt < WRITE_RETRY_ATTEMPTS; attempt++) {
        try {
          return fn()
        } catch (err) {
          lastError = err
          if (!this.#isBusyError(err)) {
            throw err
          }
          const backoff = Math.min(WRITE_RETRY_BASE_MS * 2 ** attempt, WRITE_RETRY_MAX_MS)
          const jitter = Math.floor(Math.random() * backoff)
          this.#logger.warn(
            { attempt: attempt + 1, backoffMs: backoff + jitter, err: (err as Error).message },
            'SQLiteStorage: writer locked, retrying'
          )
          await sleep(backoff + jitter)
        }
      }
      throw new StorageError(
        `SQLiteStorage: database is locked after ${WRITE_RETRY_ATTEMPTS} retries ` +
          '(~' +
          WRITE_RETRY_ATTEMPTS * WRITE_RETRY_MAX_MS +
          'ms). Another writer is holding the lock. ' +
          'Either reduce contention, increase busy_timeout via pragmas, or switch to PgStorage.',
        lastError instanceof Error ? lastError : undefined
      )
    } finally {
      release()
    }
  }

  #acquireMutex (): Promise<() => void> {
    let resolver!: () => void
    const next = new Promise<void>(resolve => {
      resolver = resolve
    })
    const previous = this.#writeMutex
    this.#writeMutex = previous.then(() => next)
    return previous.then(() => resolver)
  }

  #isBusyError (err: unknown): boolean {
    if (!(err instanceof Error)) return false
    const code = (err as NodeJS.ErrnoException).code
    if (code === 'SQLITE_BUSY' || code === 'SQLITE_LOCKED') return true
    return /SQLITE_BUSY|SQLITE_LOCKED|database is locked/i.test(err.message ?? '')
  }

  #clearDequeueWaiters (): void {
    for (const waiter of this.#dequeueWaiters) {
      clearTimeout(waiter.timeoutId)
      waiter.resolve(null)
    }
    this.#dequeueWaiters = []
  }

  #notifyDequeueWaiters (): void {
    // Pop waiters and try to give each a message. Each attempt is its own write tx.
    const waiters = this.#dequeueWaiters.splice(0)
    for (const waiter of waiters) {
      this.#tryDequeue(waiter.workerId)
        .then(msg => {
          if (msg) {
            clearTimeout(waiter.timeoutId)
            waiter.resolve(msg)
          } else {
            this.#dequeueWaiters.push(waiter)
          }
        })
        .catch(err => {
          this.#logger.error({ err }, 'SQLiteStorage: dequeue waiter failed; will retry')
          this.#dequeueWaiters.push(waiter)
        })
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // QUEUE OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  async enqueue (id: string, message: Buffer, timestamp: number): Promise<string | null> {
    const db = this.#assertConnected()
    const state = `queued:${timestamp}`
    const now = Date.now()

    const existing = await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        const row = db.prepare(`SELECT state, expires_at FROM "${this.#jobsTable}" WHERE id = ?`).get(id) as
          | { state?: string; expires_at?: number | null }
          | undefined

        if (row) {
          const expiresAt = row.expires_at ?? null
          if (expiresAt && now >= expiresAt) {
            db.prepare(`DELETE FROM "${this.#jobsTable}" WHERE id = ?`).run(id)
          } else {
            db.exec('COMMIT')
            return row.state ?? null
          }
        }

        db.prepare(`INSERT INTO "${this.#jobsTable}" (id, state) VALUES (?, ?)`).run(id, state)
        db.prepare(`INSERT INTO "${this.#queueTable}" (message) VALUES (?)`).run(message)
        db.exec('COMMIT')
        return null
      } catch (err) {
        try {
          db.exec('ROLLBACK')
        } catch {
          // ignore
        }
        throw err
      }
    })

    if (existing === null) {
      this.#eventEmitter.emit('event', id, 'queued')
      this.#notifyDequeueWaiters()
    }

    return existing
  }

  async dequeue (workerId: string, timeout: number): Promise<Buffer | null> {
    this.#assertConnected()
    const immediate = await this.#tryDequeue(workerId)
    if (immediate) return immediate

    return new Promise<Buffer | null>(resolve => {
      const timeoutId = setTimeout(() => {
        const index = this.#dequeueWaiters.findIndex(w => w.resolve === resolve)
        if (index !== -1) this.#dequeueWaiters.splice(index, 1)
        resolve(null)
      }, timeout * 1000)

      this.#dequeueWaiters.push({ workerId, resolve, timeoutId })
    })
  }

  async #tryDequeue (workerId: string): Promise<Buffer | null> {
    const db = this.#assertConnected()
    return this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        const row = db
          .prepare(
            `DELETE FROM "${this.#queueTable}"
             WHERE seq = (SELECT seq FROM "${this.#queueTable}" ORDER BY seq LIMIT 1)
             RETURNING message`
          )
          .get() as { message?: unknown } | undefined

        if (!row || row.message === undefined) {
          db.exec('COMMIT')
          return null
        }

        const message = toBuffer(row.message)
        db.prepare(`INSERT INTO "${this.#processingTable}" (worker_id, message) VALUES (?, ?)`).run(workerId, message)
        db.exec('COMMIT')
        return message
      } catch (err) {
        try {
          db.exec('ROLLBACK')
        } catch {
          // ignore
        }
        throw err
      }
    })
  }

  async requeue (id: string, message: Buffer, workerId: string): Promise<void> {
    const db = this.#assertConnected()
    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        db.prepare(`DELETE FROM "${this.#processingTable}" WHERE worker_id = ? AND message = ?`).run(workerId, message)
        db.prepare(`INSERT INTO "${this.#queueTable}" (message) VALUES (?)`).run(message)
        db.exec('COMMIT')
      } catch (err) {
        try {
          db.exec('ROLLBACK')
        } catch {
          // ignore
        }
        throw err
      }
    })
    this.#notifyDequeueWaiters()
  }

  async ack (id: string, message: Buffer, workerId: string): Promise<void> {
    const db = this.#assertConnected()
    await this.#runWrite(() => {
      db.prepare(`DELETE FROM "${this.#processingTable}" WHERE worker_id = ? AND message = ?`).run(workerId, message)
    })
  }

  // ═══════════════════════════════════════════════════════════════════
  // JOB STATE
  // ═══════════════════════════════════════════════════════════════════

  async getJobState (id: string): Promise<string | null> {
    const db = this.#assertConnected()
    const row = db.prepare(`SELECT state, expires_at FROM "${this.#jobsTable}" WHERE id = ?`).get(id) as
      | { state?: string; expires_at?: number | null }
      | undefined

    if (!row) return null
    const expiresAt = row.expires_at ?? null
    if (expiresAt && Date.now() >= expiresAt) {
      await this.#runWrite(() => {
        db.prepare(`DELETE FROM "${this.#jobsTable}" WHERE id = ?`).run(id)
      })
      return null
    }
    return row.state ?? null
  }

  async setJobState (id: string, state: string): Promise<void> {
    const db = this.#assertConnected()
    await this.#runWrite(() => {
      db.prepare(`UPDATE "${this.#jobsTable}" SET state = ? WHERE id = ?`).run(state, id)
    })
  }

  async deleteJob (id: string): Promise<boolean> {
    const db = this.#assertConnected()
    const changes = await this.#runWrite(() => {
      const result = db.prepare(`DELETE FROM "${this.#jobsTable}" WHERE id = ?`).run(id)
      return result.changes
    })
    if (changes > 0) {
      this.#eventEmitter.emit('event', id, 'cancelled')
      return true
    }
    return false
  }

  async getJobStates (ids: string[]): Promise<Map<string, string | null>> {
    const result = new Map<string, string | null>()
    if (ids.length === 0) return result

    const db = this.#assertConnected()
    const placeholders = ids.map(() => '?').join(',')
    const rows = db
      .prepare(`SELECT id, state, expires_at FROM "${this.#jobsTable}" WHERE id IN (${placeholders})`)
      .all(...ids) as Array<{ id: string; state: string; expires_at: number | null }>

    const now = Date.now()
    const found = new Set<string>()
    const expiredIds: string[] = []

    for (const row of rows) {
      found.add(row.id)
      if (row.expires_at && now >= row.expires_at) {
        expiredIds.push(row.id)
        result.set(row.id, null)
      } else {
        result.set(row.id, row.state)
      }
    }

    if (expiredIds.length > 0) {
      const expiredPlaceholders = expiredIds.map(() => '?').join(',')
      await this.#runWrite(() => {
        db.prepare(`DELETE FROM "${this.#jobsTable}" WHERE id IN (${expiredPlaceholders})`).run(...expiredIds)
      })
    }

    for (const id of ids) {
      if (!found.has(id)) result.set(id, null)
    }

    return result
  }

  async setJobExpiry (id: string, ttlMs: number): Promise<void> {
    const db = this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      db.prepare(`UPDATE "${this.#jobsTable}" SET expires_at = ? WHERE id = ?`).run(expiresAt, id)
    })
  }

  // ═══════════════════════════════════════════════════════════════════
  // RESULTS
  // ═══════════════════════════════════════════════════════════════════

  async setResult (id: string, result: Buffer, ttlMs: number): Promise<void> {
    const db = this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      db.prepare(
        `INSERT INTO "${this.#resultsTable}" (id, data, expires_at)
         VALUES (?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
      ).run(id, result, expiresAt)
    })
  }

  async getResult (id: string): Promise<Buffer | null> {
    const db = this.#assertConnected()
    const row = db.prepare(`SELECT data, expires_at FROM "${this.#resultsTable}" WHERE id = ?`).get(id) as
      | { data?: unknown; expires_at?: number }
      | undefined
    if (!row) return null
    if (row.expires_at !== undefined && Date.now() > row.expires_at) {
      await this.#runWrite(() => {
        db.prepare(`DELETE FROM "${this.#resultsTable}" WHERE id = ?`).run(id)
      })
      return null
    }
    return toBuffer(row.data)
  }

  async setError (id: string, error: Buffer, ttlMs: number): Promise<void> {
    const db = this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      db.prepare(
        `INSERT INTO "${this.#errorsTable}" (id, data, expires_at)
         VALUES (?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
      ).run(id, error, expiresAt)
    })
  }

  async getError (id: string): Promise<Buffer | null> {
    const db = this.#assertConnected()
    const row = db.prepare(`SELECT data, expires_at FROM "${this.#errorsTable}" WHERE id = ?`).get(id) as
      | { data?: unknown; expires_at?: number }
      | undefined
    if (!row) return null
    if (row.expires_at !== undefined && Date.now() > row.expires_at) {
      await this.#runWrite(() => {
        db.prepare(`DELETE FROM "${this.#errorsTable}" WHERE id = ?`).run(id)
      })
      return null
    }
    return toBuffer(row.data)
  }

  // ═══════════════════════════════════════════════════════════════════
  // WORKERS
  // ═══════════════════════════════════════════════════════════════════

  async registerWorker (workerId: string, ttlMs: number): Promise<void> {
    const db = this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      db.prepare(
        `INSERT INTO "${this.#workersTable}" (worker_id, expires_at)
         VALUES (?, ?)
         ON CONFLICT(worker_id) DO UPDATE SET expires_at = excluded.expires_at`
      ).run(workerId, expiresAt)
    })
  }

  async refreshWorker (workerId: string, ttlMs: number): Promise<void> {
    return this.registerWorker(workerId, ttlMs)
  }

  async unregisterWorker (workerId: string): Promise<void> {
    if (!this.#db) return
    const db = this.#db
    await this.#runWrite(() => {
      db.prepare(`DELETE FROM "${this.#workersTable}" WHERE worker_id = ?`).run(workerId)
      db.prepare(`DELETE FROM "${this.#processingTable}" WHERE worker_id = ?`).run(workerId)
    })
  }

  async getWorkers (): Promise<string[]> {
    const db = this.#assertConnected()
    const rows = db
      .prepare(`SELECT worker_id FROM "${this.#workersTable}" WHERE expires_at > ?`)
      .all(Date.now()) as Array<{ worker_id: string }>
    return rows.map(r => r.worker_id)
  }

  async getProcessingJobs (workerId: string): Promise<Buffer[]> {
    const db = this.#assertConnected()
    const rows = db
      .prepare(`SELECT message FROM "${this.#processingTable}" WHERE worker_id = ?`)
      .all(workerId) as Array<{ message: unknown }>
    return rows.map(r => toBuffer(r.message))
  }

  // ═══════════════════════════════════════════════════════════════════
  // NOTIFICATIONS (in-process only)
  // ═══════════════════════════════════════════════════════════════════

  async subscribeToJob (
    id: string,
    handler: (status: 'completed' | 'failed' | 'failing') => void
  ): Promise<() => Promise<void>> {
    const eventName = `notify:${id}`
    this.#notifyEmitter.on(eventName, handler)
    return async () => {
      this.#notifyEmitter.off(eventName, handler)
    }
  }

  async notifyJobComplete (id: string, status: 'completed' | 'failed' | 'failing'): Promise<void> {
    this.#assertConnected()
    this.#notifyEmitter.emit(`notify:${id}`, status)
  }

  // ═══════════════════════════════════════════════════════════════════
  // EVENTS
  // ═══════════════════════════════════════════════════════════════════

  async subscribeToEvents (handler: (id: string, event: string) => void): Promise<() => Promise<void>> {
    this.#eventEmitter.on('event', handler)
    return async () => {
      this.#eventEmitter.off('event', handler)
    }
  }

  async publishEvent (id: string, event: string): Promise<void> {
    this.#assertConnected()
    this.#eventEmitter.emit('event', id, event)
  }

  // ═══════════════════════════════════════════════════════════════════
  // ATOMIC OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  async completeJob (id: string, message: Buffer, workerId: string, result: Buffer, resultTTL: number): Promise<void> {
    const db = this.#assertConnected()
    const timestamp = Date.now()
    const state = `completed:${timestamp}`
    const expiresAt = timestamp + resultTTL

    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        db.prepare(`UPDATE "${this.#jobsTable}" SET state = ?, expires_at = ? WHERE id = ?`).run(state, expiresAt, id)
        db.prepare(
          `INSERT INTO "${this.#resultsTable}" (id, data, expires_at)
           VALUES (?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
        ).run(id, result, expiresAt)
        db.prepare(`DELETE FROM "${this.#processingTable}" WHERE worker_id = ? AND message = ?`).run(workerId, message)
        db.exec('COMMIT')
      } catch (err) {
        try {
          db.exec('ROLLBACK')
        } catch {
          // ignore
        }
        throw err
      }
    })

    this.#notifyEmitter.emit(`notify:${id}`, 'completed')
    this.#eventEmitter.emit('event', id, 'completed')
  }

  async failJob (id: string, message: Buffer, workerId: string, error: Buffer, errorTTL: number): Promise<void> {
    const db = this.#assertConnected()
    const timestamp = Date.now()
    const state = `failed:${timestamp}`
    const expiresAt = timestamp + errorTTL

    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        db.prepare(`UPDATE "${this.#jobsTable}" SET state = ?, expires_at = ? WHERE id = ?`).run(state, expiresAt, id)
        db.prepare(
          `INSERT INTO "${this.#errorsTable}" (id, data, expires_at)
           VALUES (?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
        ).run(id, error, expiresAt)
        db.prepare(`DELETE FROM "${this.#processingTable}" WHERE worker_id = ? AND message = ?`).run(workerId, message)
        db.exec('COMMIT')
      } catch (err) {
        try {
          db.exec('ROLLBACK')
        } catch {
          // ignore
        }
        throw err
      }
    })

    this.#notifyEmitter.emit(`notify:${id}`, 'failed')
    this.#eventEmitter.emit('event', id, 'failed')
  }

  async retryJob (id: string, message: Buffer, workerId: string, attempts: number): Promise<void> {
    const db = this.#assertConnected()
    const timestamp = Date.now()
    const state = `failing:${timestamp}:${attempts}`

    // Find the old processing row by worker_id. We don't parse JSON; we match all
    // processing rows for the worker and remove them. Single-process semantics
    // mean only one row per (worker_id, job-in-flight) is expected.
    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        db.prepare(`UPDATE "${this.#jobsTable}" SET state = ? WHERE id = ?`).run(state, id)
        // Delete the processing row that matches by worker_id (single-process: at most one in flight).
        db.prepare(`DELETE FROM "${this.#processingTable}" WHERE worker_id = ?`).run(workerId)
        db.prepare(`INSERT INTO "${this.#queueTable}" (message) VALUES (?)`).run(message)
        db.exec('COMMIT')
      } catch (err) {
        try {
          db.exec('ROLLBACK')
        } catch {
          // ignore
        }
        throw err
      }
    })

    this.#notifyEmitter.emit(`notify:${id}`, 'failing')
    this.#eventEmitter.emit('event', id, 'failing')
    this.#notifyDequeueWaiters()
  }

  // ═══════════════════════════════════════════════════════════════════
  // LEADER ELECTION
  // ═══════════════════════════════════════════════════════════════════

  async acquireLeaderLock (lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const db = this.#assertConnected()
    const expiresAt = Date.now() + ttlMs

    return this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        const row = db
          .prepare(`SELECT owner_id, expires_at FROM "${this.#locksTable}" WHERE lock_key = ?`)
          .get(lockKey) as { owner_id?: string; expires_at?: number } | undefined

        const now = Date.now()
        if (row && row.expires_at !== undefined && now < row.expires_at) {
          db.exec('COMMIT')
          return false
        }

        db.prepare(
          `INSERT INTO "${this.#locksTable}" (lock_key, owner_id, expires_at)
           VALUES (?, ?, ?)
           ON CONFLICT(lock_key) DO UPDATE
             SET owner_id = excluded.owner_id, expires_at = excluded.expires_at`
        ).run(lockKey, ownerId, expiresAt)
        db.exec('COMMIT')
        return true
      } catch (err) {
        try {
          db.exec('ROLLBACK')
        } catch {
          // ignore
        }
        throw err
      }
    })
  }

  async renewLeaderLock (lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const db = this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    return this.#runWrite(() => {
      const result = db
        .prepare(`UPDATE "${this.#locksTable}" SET expires_at = ? WHERE lock_key = ? AND owner_id = ?`)
        .run(expiresAt, lockKey, ownerId)
      return result.changes > 0
    })
  }

  async releaseLeaderLock (lockKey: string, ownerId: string): Promise<boolean> {
    if (!this.#db) return false
    const db = this.#db
    return this.#runWrite(() => {
      const result = db
        .prepare(`DELETE FROM "${this.#locksTable}" WHERE lock_key = ? AND owner_id = ?`)
        .run(lockKey, ownerId)
      return result.changes > 0
    })
  }

  // ═══════════════════════════════════════════════════════════════════
  // CLEANUP LEADER + VACUUM
  // ═══════════════════════════════════════════════════════════════════

  #startCleanupLeaderLoop (): void {
    if (this.#cleanupIntervalMs === false) return
    if (this.#parentStorage) return // children don't run cleanup

    const tick = (): void => {
      this.#leadershipTick().catch(err => {
        this.#logger.error({ err }, 'SQLiteStorage: leadership tick failed')
      })
    }

    this.#leadershipTimer = setInterval(tick, CLEANUP_ACQUIRE_RETRY_MS)
    // Try once immediately so the first cleanup runs soon.
    setImmediate(tick)
  }

  async #leadershipTick (): Promise<void> {
    if (!this.#db) return

    if (this.#isCleanupLeader) {
      const renewed = await this.renewLeaderLock(CLEANUP_LOCK_KEY, this.#instanceId, CLEANUP_LOCK_TTL_MS)
      if (!renewed) {
        this.#isCleanupLeader = false
        this.#stopCleanupInterval()
        this.#logger.info('SQLiteStorage: lost cleanup leadership')
      }
    } else {
      const acquired = await this.acquireLeaderLock(CLEANUP_LOCK_KEY, this.#instanceId, CLEANUP_LOCK_TTL_MS)
      if (acquired) {
        this.#isCleanupLeader = true
        this.#startCleanupInterval()
        this.#logger.info('SQLiteStorage: acquired cleanup leadership')
      }
    }
  }

  #startCleanupInterval (): void {
    if (this.#cleanupInterval || this.#cleanupIntervalMs === false) return
    const intervalMs = this.#cleanupIntervalMs as number
    this.#cleanupInterval = setInterval(() => {
      this.#cleanupExpired().catch(err => {
        this.#logger.error({ err }, 'SQLiteStorage: cleanup sweep failed')
      })
    }, intervalMs)
  }

  #stopCleanupInterval (): void {
    if (this.#cleanupInterval) {
      clearInterval(this.#cleanupInterval)
      this.#cleanupInterval = null
    }
  }

  async #cleanupExpired (): Promise<void> {
    const db = this.#assertConnected()
    const now = Date.now()
    const start = now

    await this.#runWrite(() => {
      db.prepare(`DELETE FROM "${this.#resultsTable}" WHERE expires_at < ?`).run(now)
      db.prepare(`DELETE FROM "${this.#errorsTable}" WHERE expires_at < ?`).run(now)
      db.prepare(`DELETE FROM "${this.#workersTable}" WHERE expires_at < ?`).run(now)
      db.prepare(`DELETE FROM "${this.#jobsTable}" WHERE expires_at IS NOT NULL AND expires_at < ?`).run(now)
      db.prepare(`DELETE FROM "${this.#locksTable}" WHERE expires_at < ?`).run(now)
    })

    const duration = Date.now() - start
    if (duration > 1000) {
      this.#logger.warn({ durationMs: duration }, 'SQLiteStorage: cleanup sweep slow')
    }
  }

  #startVacuumLoop (): void {
    if (this.#vacuum === false || !this.#vacuum.enabled) return
    if (this.#parentStorage) return
    this.#vacuumInterval = setInterval(() => {
      this.#runWrite(() => {
        this.#db?.exec('PRAGMA optimize')
      }).catch(err => {
        this.#logger.warn({ err }, 'SQLiteStorage: pragma optimize failed')
      })
    }, this.#vacuum.intervalMs)
  }

  // ═══════════════════════════════════════════════════════════════════
  // NAMESPACE
  // ═══════════════════════════════════════════════════════════════════

  createNamespace (name: string): Storage {
    const root = this.#parentStorage ?? this
    const ns = new SQLiteStorage({
      path: root.#path,
      tablePrefix: `${this.#tablePrefix}${name}_`,
      cleanupIntervalMs: false, // children do not run cleanup
      vacuum: false,
      pragmas: this.#pragmas,
      logger: this.#logger
    })
    ns.#parentStorage = root
    return ns
  }

  /**
   * Clear all data (useful for testing).
   */
  async clear (): Promise<void> {
    if (!this.#db) return
    const db = this.#db
    await this.#runWrite(() => {
      db.exec(`
        DELETE FROM "${this.#queueTable}";
        DELETE FROM "${this.#processingTable}";
        DELETE FROM "${this.#jobsTable}";
        DELETE FROM "${this.#resultsTable}";
        DELETE FROM "${this.#errorsTable}";
        DELETE FROM "${this.#workersTable}";
        DELETE FROM "${this.#locksTable}";
      `)
    })
  }
}
