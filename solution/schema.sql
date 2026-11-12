CREATE TABLE IF NOT EXISTS leads(workflow_id TEXT NOT NULL,phone_hash TEXT NOT NULL,phone_last4 TEXT NOT NULL,batch_id TEXT NOT NULL,external_ref TEXT NOT NULL,fields_json TEXT NOT NULL,PRIMARY KEY(workflow_id,phone_hash));
CREATE TABLE IF NOT EXISTS rejections(workflow_id TEXT,batch_id TEXT,row_number INTEGER,reason TEXT,PRIMARY KEY(workflow_id,batch_id,row_number));
CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,event_type TEXT,workflow_id TEXT,call_id TEXT,phone_hash TEXT,occurred_at TEXT,payload_json TEXT,lead_linked INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS ingested_drops(drop_path TEXT PRIMARY KEY,drop_hash TEXT);
CREATE TABLE IF NOT EXISTS counters(key TEXT PRIMARY KEY,value INTEGER);