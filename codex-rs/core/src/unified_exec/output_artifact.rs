use codex_config::config_toml::RecoverableExecOutputConfig;
use codex_exec_server::create_private_local_cache_directory;
use codex_exec_server::open_local_cache_file_no_follow;
use codex_exec_server::remove_local_cache_file_no_follow;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::mpsc;
use uuid::Uuid;

const METADATA_RESERVATION: u64 = 4096;
const MAX_QUEUE_BYTES: usize = 1024 * 1024;
const MAX_CHUNK_BYTES: usize = 8192;
const MAX_SCAN_BYTES: usize = 256 * 1024;
const MAX_RETURN_BYTES: usize = 8000;
const MAX_LINE_BYTES: usize = 512;
const MAX_CURSORS: usize = 128;
const MAX_OBJECTS: usize = 4096;

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutputStream {
    Stdout,
    Stderr,
}
impl OutputStream {
    fn index(self) -> usize {
        match self {
            Self::Stdout => 0,
            Self::Stderr => 1,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    schema_version: u32,
    artifact_id: Uuid,
    thread_id: String,
    quota_session_id: String,
    environment_id: String,
    run_id: Uuid,
    call_id: String,
    created_at: u64,
    expires_at: u64,
    reserved_bytes: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub(crate) struct StreamReceipt {
    pub observed_bytes: u64,
    pub stored_bytes: u64,
    pub omitted_bytes: u64,
    pub observed_sha256: String,
    pub stored_sha256: String,
    pub hash_final: bool,
}
#[derive(Clone, Debug, Serialize, PartialEq)]
pub(crate) struct ArtifactReceipt {
    pub schema_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<Uuid>,
    pub command_status: String,
    pub artifact_status: String,
    pub complete: bool,
    pub created_at: u64,
    pub expires_at: u64,
    pub encoding: &'static str,
    #[serde(skip)]
    pub preview_max_tokens: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<StreamReceipt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<StreamReceipt>,
}
impl ArtifactReceipt {
    pub(crate) fn unavailable(reason: &str) -> Self {
        Self {
            schema_version: 1,
            artifact_id: None,
            command_status: "running".into(),
            artifact_status: "artifact_unavailable".into(),
            complete: false,
            created_at: 0,
            expires_at: 0,
            encoding: "utf-8",
            preview_max_tokens: 400,
            reason: Some(reason.into()),
            stdout: None,
            stderr: None,
        }
    }
}

#[derive(Default)]
struct StreamState {
    observed: u64,
    stored: u64,
    observed_hash: Sha256,
    stored_hash: Sha256,
    utf8_pending: Vec<u8>,
    eof: bool,
}
struct CaptureState {
    streams: [StreamState; 2],
    accepted: u64,
    reason: Option<String>,
    command_status: String,
    flushed: bool,
}
impl Default for CaptureState {
    fn default() -> Self {
        Self {
            streams: Default::default(),
            accepted: 0,
            reason: None,
            command_status: "running".into(),
            flushed: false,
        }
    }
}
struct Entry {
    manifest: Manifest,
    files: [File; 2],
    // A live writer or reader keeps the cross-process cleanup lease locked.
    _lease: File,
    state: Arc<Mutex<CaptureState>>,
    preview_max_tokens: usize,
}
impl Entry {
    fn receipt(&self) -> ArtifactReceipt {
        let state = lock(&self.state);
        let expired = now() >= self.manifest.expires_at;
        let complete = !expired
            && state.reason.is_none()
            && state.flushed
            && state.streams.iter().all(|s| s.eof)
            && state.command_status == "completed";
        let has_content = state.streams.iter().any(|s| s.stored > 0);
        let unsupported = state.reason.as_deref() == Some("unsupported_encoding");
        ArtifactReceipt {
            schema_version: 1,
            artifact_id: (!expired && !unsupported && (has_content || state.reason.is_none()))
                .then_some(self.manifest.artifact_id),
            command_status: state.command_status.clone(),
            artifact_status: if expired {
                "expired"
            } else if unsupported {
                "artifact_unavailable"
            } else if state.reason.is_some() {
                if has_content {
                    "partial"
                } else {
                    "artifact_unavailable"
                }
            } else if state.flushed {
                "available"
            } else {
                "recording"
            }
            .into(),
            complete,
            created_at: self.manifest.created_at,
            expires_at: self.manifest.expires_at,
            encoding: "utf-8",
            preview_max_tokens: self.preview_max_tokens,
            reason: state.reason.clone(),
            stdout: Some(stream_receipt(
                &state.streams[0],
                state.flushed && state.command_status == "completed",
            )),
            stderr: Some(stream_receipt(
                &state.streams[1],
                state.flushed && state.command_status == "completed",
            )),
        }
    }
}
fn stream_receipt(s: &StreamState, flushed: bool) -> StreamReceipt {
    StreamReceipt {
        observed_bytes: s.observed,
        stored_bytes: s.stored,
        omitted_bytes: s.observed.saturating_sub(s.stored),
        observed_sha256: format!("{:x}", s.observed_hash.clone().finalize()),
        stored_sha256: format!("{:x}", s.stored_hash.clone().finalize()),
        hash_final: flushed && s.eof,
    }
}

struct QueuedChunk {
    stream: OutputStream,
    bytes: Vec<u8>,
    queue_bytes: Arc<AtomicUsize>,
}
impl Drop for QueuedChunk {
    fn drop(&mut self) {
        self.queue_bytes
            .fetch_sub(self.bytes.len(), Ordering::AcqRel);
    }
}
pub(crate) struct ArtifactCapture {
    entry: Arc<Entry>,
    sender: Mutex<Option<mpsc::Sender<QueuedChunk>>>,
    queue_bytes: Arc<AtomicUsize>,
    max_bytes: u64,
}
impl ArtifactCapture {
    pub(crate) fn observe(&self, stream: OutputStream, bytes: &[u8]) {
        let mut state = lock(&self.entry.state);
        let s = &mut state.streams[stream.index()];
        s.observed = s.observed.saturating_add(bytes.len() as u64);
        s.observed_hash.update(bytes);
        if !validate_utf8(s, bytes) {
            state.reason = Some("unsupported_encoding".into());
        }
        if now() >= self.entry.manifest.expires_at {
            state.reason = Some("expired".into());
        }
        if state.reason.is_some() {
            return;
        }
        for bytes in bytes.chunks(MAX_CHUNK_BYTES) {
            let available = self.max_bytes.saturating_sub(state.accepted) as usize;
            let count = bytes.len().min(available);
            if count == 0 {
                state.reason = Some("artifact_quota".into());
                break;
            }
            if self
                .queue_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    (used + count <= MAX_QUEUE_BYTES).then_some(used + count)
                })
                .is_err()
            {
                state.reason = Some("queue_full".into());
                break;
            }
            let queued = QueuedChunk {
                stream,
                bytes: bytes[..count].to_vec(),
                queue_bytes: Arc::clone(&self.queue_bytes),
            };
            let sender = lock(&self.sender);
            let result = sender.as_ref().map(|sender| sender.try_send(queued));
            match result {
                Some(Ok(())) => {}
                Some(Err(mpsc::error::TrySendError::Full(_))) => {
                    state.reason = Some("queue_full".into());
                    break;
                }
                _ => {
                    state.reason = Some("writer_unavailable".into());
                    break;
                }
            }
            state.accepted += count as u64;
            if count < bytes.len() {
                state.reason = Some("artifact_quota".into());
                break;
            }
        }
    }
    pub(crate) fn stop(&self) {
        let mut state = lock(&self.entry.state);
        if !state.streams.iter().all(|s| s.eof) && state.reason.is_none() {
            state.reason = Some("capture_closed".into());
        }
        drop(state);
        lock(&self.sender).take();
    }
    pub(crate) fn streams_closed(&self) {
        let mut state = lock(&self.entry.state);
        for s in &mut state.streams {
            s.eof = true;
        }
        if state.streams.iter().any(|s| !s.utf8_pending.is_empty()) {
            state.reason = Some("unsupported_encoding".into());
        }
        drop(state);
        lock(&self.sender).take();
    }
    pub(crate) fn record_command_status(&self, status: &str) {
        let mut state = lock(&self.entry.state);
        // Timeout is the command's authoritative terminal outcome, even if
        // the exit watcher won the race with timeout handling.
        if state.command_status == "running" || status == "timed_out" {
            state.command_status = status.into();
        }
    }
    pub(crate) fn receipt(&self) -> ArtifactReceipt {
        self.entry.receipt()
    }
}
impl Drop for ArtifactCapture {
    fn drop(&mut self) {
        let mut state = lock(&self.entry.state);
        if !state.streams.iter().all(|s| s.eof) && state.reason.is_none() {
            state.reason = Some("capture_closed".into());
        }
    }
}
fn write_prefix(file: &mut impl Write, bytes: &[u8]) -> usize {
    let mut written = 0;
    while written < bytes.len() {
        match file.write(&bytes[written..]) {
            Ok(0) => break,
            Ok(count) => written += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    written
}

fn validate_utf8(s: &mut StreamState, bytes: &[u8]) -> bool {
    if bytes.contains(&0) {
        return false;
    }
    let mut pending = std::mem::take(&mut s.utf8_pending);
    pending.extend_from_slice(bytes);
    match std::str::from_utf8(&pending) {
        Ok(_) => true,
        Err(error) if error.error_len().is_none() => {
            s.utf8_pending
                .extend_from_slice(&pending[error.valid_up_to()..]);
            true
        }
        Err(_) => false,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryRequest {
    pub artifact_id: Uuid,
    pub stream: OutputStream,
    pub environment_id: Option<String>,
    pub cursor: Option<Uuid>,
    pub start_line: Option<u64>,
    pub line_count: Option<usize>,
    pub query: Option<String>,
    pub max_matches: Option<usize>,
    pub context_lines: Option<usize>,
}
#[derive(Clone)]
struct Cursor {
    artifact_id: Uuid,
    stream: OutputStream,
    query: Option<String>,
    offset: u64,
    line: u64,
    snapshot_len: u64,
    expires_at: u64,
    carry: String,
    context_lines: usize,
    trailing: usize,
    preceding: std::collections::VecDeque<(u64, String, bool)>,
}
#[derive(Debug, Serialize)]
pub(crate) struct OutputLine {
    pub line: u64,
    pub text: String,
    pub continued: bool,
}
#[derive(Serialize)]
pub(crate) struct QueryReceipt {
    pub artifact_id: Uuid,
    pub stream: OutputStream,
    pub recovery: ArtifactReceipt,
    pub lines: Vec<OutputLine>,
    pub snapshot_bytes: u64,
    pub scanned_bytes: usize,
    pub trailing_incomplete_utf8_bytes: usize,
    pub complete: bool,
    pub next_cursor: Option<Uuid>,
}

pub(crate) struct OutputArtifactStore {
    root: PathBuf,
    config: RecoverableExecOutputConfig,
    entries: Mutex<HashMap<Uuid, Arc<Entry>>>,
    cursors: Mutex<HashMap<Uuid, Cursor>>,
    queue_bytes: Arc<AtomicUsize>,
}
impl OutputArtifactStore {
    pub(crate) fn new(root: PathBuf, config: RecoverableExecOutputConfig) -> Self {
        Self {
            root,
            config,
            entries: Default::default(),
            cursors: Default::default(),
            queue_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub(crate) async fn begin(
        self: &Arc<Self>,
        thread_id: String,
        quota_session_id: String,
        environment_id: String,
        call_id: String,
    ) -> Result<Arc<ArtifactCapture>, &'static str> {
        if thread_id.len() > 256
            || quota_session_id.len() > 256
            || environment_id.len() > 256
            || call_id.len() > 256
        {
            return Err("identity_too_long");
        }
        lock(&self.entries).retain(|_, entry| entry.manifest.expires_at > now());
        let store = Arc::clone(self);
        let entry = tokio::task::spawn_blocking(move || {
            store.allocate(thread_id, quota_session_id, environment_id, call_id)
        })
        .await
        .map_err(|_| "storage_unavailable")?
        .map_err(|_| "storage_unavailable")?;
        let files = [entry.files[0].try_clone(), entry.files[1].try_clone()];
        let [stdout, stderr] = files;
        let files = [
            stdout.map_err(|_| "storage_unavailable")?,
            stderr.map_err(|_| "storage_unavailable")?,
        ];
        let (sender, receiver) = mpsc::channel::<QueuedChunk>(32);
        Self::spawn_writer(Arc::clone(&entry), files, receiver);
        lock(&self.entries).insert(entry.manifest.artifact_id, Arc::clone(&entry));
        Ok(Arc::new(ArtifactCapture {
            entry,
            sender: Mutex::new(Some(sender)),
            queue_bytes: Arc::clone(&self.queue_bytes),
            max_bytes: self.config.artifact_max_bytes,
        }))
    }

    fn spawn_writer(
        entry: Arc<Entry>,
        mut files: [File; 2],
        mut receiver: mpsc::Receiver<QueuedChunk>,
    ) {
        tokio::task::spawn_blocking(move || {
            let mut failed = false;
            while let Some(chunk) = receiver.blocking_recv() {
                if failed {
                    continue;
                }
                let file = &mut files[chunk.stream.index()];
                // Account partial writes exactly, then stop at the first gap.
                let written = write_prefix(file, &chunk.bytes);
                let mut state = lock(&entry.state);
                let stream = &mut state.streams[chunk.stream.index()];
                stream.stored += written as u64;
                stream.stored_hash.update(&chunk.bytes[..written]);
                if written != chunk.bytes.len() {
                    failed = true;
                    state.reason.get_or_insert_with(|| "write_failed".into());
                }
            }
            let flushed = files.iter_mut().try_for_each(Write::flush).is_ok();
            let mut state = lock(&entry.state);
            state.flushed = true;
            if !flushed {
                state.reason.get_or_insert_with(|| "write_failed".into());
            }
        });
    }

    fn allocate(
        &self,
        thread_id: String,
        quota_session_id: String,
        environment_id: String,
        call_id: String,
    ) -> io::Result<Arc<Entry>> {
        create_private_local_cache_directory(&self.root)?;
        let lock_path = self.root.join("quota.lock");
        let quota_lock = match open_local_cache_file_no_follow(&lock_path, /*create_new*/ true) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                open_local_cache_file_no_follow(&lock_path, /*create_new*/ false)?
            }
            Err(error) => return Err(error),
        };
        // Contention is an optimization failure, not a reason to stall a command.
        quota_lock.try_lock().map_err(io::Error::from)?;
        let mut global = 0_u64;
        let mut session = 0_u64;
        let mut objects = 0;
        for (index, item) in std::fs::read_dir(&self.root)?.enumerate() {
            if index >= MAX_OBJECTS * 5 + 2 {
                return Err(io::Error::other("cache entry limit"));
            }
            let item = item?;
            let Some(name) = item.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(id) = name
                .strip_suffix(".json")
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                continue;
            };
            objects += 1;
            if objects >= MAX_OBJECTS {
                return Err(io::Error::other("object limit exceeded"));
            }
            let mut manifest_file =
                open_local_cache_file_no_follow(&item.path(), /*create_new*/ false)?;
            let mut bytes = Vec::new();
            (&mut manifest_file)
                .take(METADATA_RESERVATION + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > METADATA_RESERVATION {
                return Err(io::Error::other("invalid cache metadata"));
            }
            let manifest: Manifest = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            if manifest.schema_version != 1
                || manifest.artifact_id != id
                || manifest.reserved_bytes == 0
            {
                return Err(io::Error::other("invalid cache ownership"));
            }
            let lease = open_local_cache_file_no_follow(
                &self.path(id, "lease"),
                /*create_new*/ false,
            )?;
            if manifest.expires_at <= now() && lease.try_lock().is_ok() {
                // Delete only regular files belonging to a validated manifest, under the global lock.
                for suffix in ["stdout", "stderr", "json"] {
                    remove_local_cache_file_no_follow(&self.path(id, suffix))?;
                }
                drop(lease);
                remove_local_cache_file_no_follow(&self.path(id, "lease"))?;
                continue;
            }
            global = global.saturating_add(manifest.reserved_bytes);
            if manifest.quota_session_id == quota_session_id {
                session = session.saturating_add(manifest.reserved_bytes);
            }
        }
        // Cleanup may remove later directory entries. Account the remaining
        // files after cleanup, without charging deleted objects or statting
        // entries from the stale enumeration.
        let mut physical = 0_u64;
        for (index, item) in std::fs::read_dir(&self.root)?.enumerate() {
            if index >= MAX_OBJECTS * 5 + 2 {
                return Err(io::Error::other("cache entry limit"));
            }
            physical = physical.saturating_add(item?.metadata()?.len());
            if physical > self.config.global_max_bytes {
                return Err(io::Error::other("global quota exceeded"));
            }
        }
        let reserved = self.config.artifact_max_bytes + METADATA_RESERVATION;
        if global.max(physical).saturating_add(reserved) > self.config.global_max_bytes
            || session.saturating_add(reserved) > self.config.session_max_bytes
        {
            return Err(io::Error::other("output quota exceeded"));
        }
        let id = Uuid::new_v4();
        let manifest = Manifest {
            schema_version: 1,
            artifact_id: id,
            thread_id,
            quota_session_id,
            environment_id,
            run_id: Uuid::new_v4(),
            call_id,
            created_at: now(),
            expires_at: now().saturating_add(self.config.ttl_seconds),
            reserved_bytes: reserved,
        };
        let mut created = Vec::new();
        let result = (|| {
            let mut create = |suffix| {
                let path = self.path(id, suffix);
                let file = open_local_cache_file_no_follow(&path, /*create_new*/ true)?;
                created.push(path);
                Ok::<_, io::Error>(file)
            };
            let lease = create("lease")?;
            lease.try_lock().map_err(io::Error::from)?;
            let files = [create("stdout")?, create("stderr")?];
            let mut metadata = create("json")?;
            let bytes = serde_json::to_vec(&manifest).map_err(io::Error::other)?;
            if bytes.len() as u64 > METADATA_RESERVATION {
                return Err(io::Error::other("metadata limit exceeded"));
            }
            metadata.write_all(&bytes)?;
            Ok(Arc::new(Entry {
                manifest,
                files,
                _lease: lease,
                state: Default::default(),
                preview_max_tokens: self.config.preview_max_tokens,
            }))
        })();
        if result.is_err() {
            for path in created {
                let _ = remove_local_cache_file_no_follow(&path);
            }
        }
        result
    }
    fn path(&self, id: Uuid, suffix: &str) -> PathBuf {
        self.root.join(format!("{id}.{suffix}"))
    }

    pub(crate) async fn query(
        self: &Arc<Self>,
        thread_id: String,
        environments: Vec<String>,
        request: QueryRequest,
        search: bool,
    ) -> Result<QueryReceipt, &'static str> {
        if request.start_line == Some(0)
            || request.line_count.is_some_and(|n| n == 0 || n > 100)
            || request.max_matches.is_some_and(|n| n == 0 || n > 50)
            || request.context_lines.unwrap_or(0) > 5
        {
            return Err("invalid_query_limits");
        }
        if search
            && request.query.as_ref().is_none_or(|q| {
                q.is_empty()
                    || q.len() > 512
                    || q.contains('\n')
                    || q.contains('\r')
                    || q.contains('\0')
            })
        {
            return Err("invalid_literal_query");
        }
        if !search && request.query.is_some() {
            return Err("read_does_not_accept_query");
        }
        let entry = lock(&self.entries)
            .get(&request.artifact_id)
            .cloned()
            .ok_or("unknown_artifact")?;
        if entry.manifest.thread_id != thread_id
            || !environments.contains(&entry.manifest.environment_id)
            || request
                .environment_id
                .as_ref()
                .is_some_and(|id| id != &entry.manifest.environment_id)
        {
            return Err("artifact_owner_mismatch");
        }
        if now() >= entry.manifest.expires_at {
            lock(&self.entries).remove(&request.artifact_id);
            return Err("artifact_expired");
        }
        let receipt = entry.receipt();
        if receipt.artifact_id.is_none() {
            return Err("artifact_unavailable");
        }
        let stored = lock(&entry.state).streams[request.stream.index()].stored;
        let cursor = if let Some(id) = request.cursor {
            let mut cursors = lock(&self.cursors);
            let cursor = cursors.get(&id).ok_or("invalid_cursor")?;
            if cursor.artifact_id != request.artifact_id
                || cursor.stream != request.stream
                || cursor.query != request.query
                || cursor.context_lines != request.context_lines.unwrap_or(0)
                || cursor.expires_at <= now()
            {
                return Err("invalid_cursor");
            }
            cursors.remove(&id).ok_or("invalid_cursor")?
        } else {
            Cursor {
                artifact_id: request.artifact_id,
                stream: request.stream,
                query: request.query.clone(),
                offset: 0,
                line: 1,
                snapshot_len: stored,
                expires_at: entry.manifest.expires_at,
                carry: String::new(),
                context_lines: request.context_lines.unwrap_or(0),
                trailing: 0,
                preceding: Default::default(),
            }
        };
        let maximum = if search {
            request.max_matches.unwrap_or(20)
        } else {
            request.line_count.unwrap_or(50)
        };
        let start_line = request.start_line.unwrap_or(1);
        let context_lines = request.context_lines.unwrap_or(0);
        let path = self.path(
            request.artifact_id,
            match request.stream {
                OutputStream::Stdout => "stdout",
                OutputStream::Stderr => "stderr",
            },
        );
        let (mut response, next) = tokio::task::spawn_blocking(move || {
            // Windows seek_read changes the handle's cursor. A new no-follow
            // handle keeps live reads independent of the writer and other reads.
            let file = open_local_cache_file_no_follow(&path, /*create_new*/ false)?;
            scan(&entry, &file, cursor, start_line, maximum, context_lines)
        })
        .await
        .map_err(|_| "read_unavailable")?
        .map_err(|_| "read_unavailable")?;
        if let Some(next) = next {
            let mut cursors = lock(&self.cursors);
            cursors.retain(|_, cursor| cursor.expires_at > now());
            if cursors.len() >= MAX_CURSORS {
                return Err("cursor_limit");
            }
            let id = Uuid::new_v4();
            cursors.insert(id, next);
            response.next_cursor = Some(id);
        }
        Ok(response)
    }
}

#[cfg(unix)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}
#[cfg(windows)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buffer, offset)
}
fn scan(
    entry: &Entry,
    file: &File,
    mut cursor: Cursor,
    start_line: u64,
    maximum: usize,
    context_lines: usize,
) -> io::Result<(QueryReceipt, Option<Cursor>)> {
    let available = cursor
        .snapshot_len
        .saturating_sub(cursor.offset)
        .min(MAX_SCAN_BYTES as u64) as usize;
    let mut bytes = vec![0; available];
    let count = read_at(file, &mut bytes, cursor.offset)?;
    bytes.truncate(count);
    // Never expose a half UTF-8 code point at a read boundary.
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(error) if error.error_len().is_none() => {
            std::str::from_utf8(&bytes[..error.valid_up_to()]).map_err(io::Error::other)?
        }
        Err(error) => return Err(io::Error::other(error)),
    };
    let mut lines = Vec::new();
    let mut consumed = 0;
    let mut returned = 0;
    let mut matches = 0;
    let mut trailing = cursor.trailing;
    let mut preceding = std::mem::take(&mut cursor.preceding);
    'scan: while consumed < text.len() {
        let remaining = &text[consumed..];
        let limit = remaining.len().min(MAX_LINE_BYTES);
        let limit = (0..=limit)
            .rev()
            .find(|end| remaining.is_char_boundary(*end))
            .unwrap_or(0);
        let prefix = &remaining[..limit];
        let end = prefix.find('\n').map_or(limit, |index| index + 1);
        let line = &remaining[..end];
        let end = line.len().min(MAX_LINE_BYTES);
        let end = (0..=end)
            .rev()
            .find(|end| line.is_char_boundary(*end))
            .unwrap_or(0);
        let fragment = &line[..end];
        let continued = end < line.len()
            || !fragment.ends_with('\n')
                && cursor.offset + consumed as u64 + (end as u64) < cursor.snapshot_len;
        let content = if let Some(content) = fragment.strip_suffix("\r\n") {
            content
        } else if let Some(content) = fragment.strip_suffix('\n') {
            content
        } else {
            fragment
        };
        let searchable = format!("{}{content}", cursor.carry);
        let hit = matches < maximum
            && cursor
                .query
                .as_ref()
                .is_some_and(|query| searchable.contains(query));
        let content = if hit && !cursor.carry.is_empty() {
            searchable.as_str()
        } else {
            content
        };
        let wanted = if cursor.query.is_some() {
            hit || trailing > 0
        } else {
            cursor.line >= start_line
        };
        if wanted {
            if hit {
                while let Some((line_number, text, continued)) = preceding.front().cloned() {
                    if returned
                        + serde_json::to_string(&text)
                            .map_err(io::Error::other)?
                            .len()
                        + 80
                        > MAX_RETURN_BYTES - 1536
                    {
                        break 'scan;
                    }
                    preceding.pop_front();
                    returned += serde_json::to_string(&text)
                        .map_err(io::Error::other)?
                        .len()
                        + 80;
                    lines.push(OutputLine {
                        line: line_number,
                        text,
                        continued,
                    });
                }
            }
            if returned
                + serde_json::to_string(content)
                    .map_err(io::Error::other)?
                    .len()
                + 80
                > MAX_RETURN_BYTES - 1536
            {
                break;
            }
            returned += serde_json::to_string(content)
                .map_err(io::Error::other)?
                .len()
                + 80;
            lines.push(OutputLine {
                line: cursor.line,
                text: content.into(),
                continued,
            });
            if hit {
                matches += 1;
                trailing = context_lines;
            } else {
                trailing = trailing.saturating_sub(1);
            }
        } else if cursor.query.is_some() && context_lines > 0 {
            if preceding.len() == context_lines {
                preceding.pop_front();
            }
            preceding.push_back((cursor.line, content.into(), continued));
        }
        consumed += end;
        if fragment.ends_with('\n') {
            cursor.line += 1;
            cursor.carry.clear();
        } else if let Some(query) = &cursor.query {
            let start = searchable
                .len()
                .saturating_sub(query.len().saturating_sub(1));
            let start = (start..=searchable.len())
                .find(|start| searchable.is_char_boundary(*start))
                .unwrap_or(searchable.len());
            cursor.carry = searchable[start..].into();
        }
        if cursor.query.is_some() && matches >= maximum && trailing == 0
            || cursor.query.is_none() && lines.len() >= maximum
        {
            break;
        }
        // Long lines advance in bounded fragments; no allocation follows their full length.
        if end == 0 {
            break;
        }
    }
    let trailing_incomplete_utf8_bytes =
        if consumed == text.len() && cursor.offset + count as u64 == cursor.snapshot_len {
            count - text.len()
        } else {
            0
        };
    consumed += trailing_incomplete_utf8_bytes;
    cursor.offset += consumed as u64;
    cursor.trailing = trailing;
    cursor.preceding = preceding;
    let complete = cursor.offset >= cursor.snapshot_len;
    let response = QueryReceipt {
        artifact_id: cursor.artifact_id,
        stream: cursor.stream,
        recovery: entry.receipt(),
        lines,
        snapshot_bytes: cursor.snapshot_len,
        scanned_bytes: consumed,
        trailing_incomplete_utf8_bytes,
        complete,
        next_cursor: None,
    };
    Ok((response, (!complete).then_some(cursor)))
}

#[cfg(test)]
#[path = "output_artifact_tests.rs"]
mod tests;
