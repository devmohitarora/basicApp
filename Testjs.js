import mysql from 'mysql2';
import pLimit from 'p-limit';

// Configuration
const CONCURRENCY_LIMIT = 200; // Adjust based on your network/CPU bandwidth
const BATCH_UPDATE_SIZE = 10000;
const HTTP_TIMEOUT = 5000;    // 5 seconds timeout per URL

// Database Configuration (Aurora MySQL)
const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'Mysql@18196',
  database: 'core_shine',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

async function checkUrlStatus(url) {
  try {
    // Using AbortSignal to enforce strict timeouts
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), HTTP_TIMEOUT);

    const response = await fetch(url, {
      method: 'HEAD', // Crucial: Only fetch headers, not the whole page body
      signal: controller.signal,
      headers: { 'User-Agent': 'Mozilla/5.0 (URL-Status-Checker)' }
    });

    clearTimeout(timeoutId);
    
    // Returns true if 200-299, false if 404 or others
    return { status: response.status, isWorking: true };
  } catch (error) {
    let reason = 'FAILED';
    if (error.name === 'AbortError') reason = 'TIMEOUT';
    return { status: reason, isWorking: false };
  }
}

async function bulkUpdateDatabase(results) {
  if (results.length === 0) return;

  const connection = await pool.promise().getConnection();
  try {
    // Assuming your table has 'id', 'status_code', and 'is_active' columns
    // Using a bulk INSERT ... ON DUPLICATE KEY UPDATE or a CASE statement for speed
    // Here is an efficient transaction block for updates
    await connection.beginTransaction();
    
    const query = `
      UPDATE apx_programs SET is_invalid_url = ?  WHERE deleted_at is null and id = ?`;

    for (const res of results) {
      await connection.execute(query, [res.isWorking ? 1 : 0, res.id]);
    }

    await connection.commit();
  } catch (error) {
    await connection.rollback();
    console.error('Database batch update failed:', error);
  } finally {
    connection.release();
  }
}

async function startProcessing() {
  const limit = pLimit(CONCURRENCY_LIMIT);
  let batchBuffer = [];
  let totalProcessed = 0;

  console.log('Starting processing stream...');

  // Use a stream to prevent loading 10M rows into Node.js memory
  const stream = pool.query('SELECT id, url FROM apx_programs WHERE deleted_at IS NULL').stream();

  for await (const row of stream) {
    totalProcessed++;

    // Push the asynchronous job into the concurrency limiter
    const task = limit(async () => {
      const result = await checkUrlStatus(row.url);
      return { id: row.id, ...result };
    });

    batchBuffer.push(task);

    // Once we accumulate enough tasks to make a database write batch worthwhile
    if (batchBuffer.length >= BATCH_UPDATE_SIZE) {
      // Pause the database stream so memory doesn't overflow while we wait for network/DB
      stream.pause();

      // Resolve the current batch of network requests
      const resolvedResults = await Promise.all(batchBuffer);
      
      // Bulk update the database
      await bulkUpdateDatabase(resolvedResults);
      
      console.log(`Processed and saved ${totalProcessed} URLs...`);
      
      // Clear the buffer and resume reading from DB
      batchBuffer = [];
      stream.resume();
    }
  }

  // Handle any remaining items in the buffer at the end
  if (batchBuffer.length > 0) {
    const resolvedResults = await Promise.all(batchBuffer);
    await bulkUpdateDatabase(resolvedResults);
    console.log(`Job finished. Total URLs checked: ${totalProcessed}`);
  }

  pool.end();
}

startProcessing().catch(console.error);
