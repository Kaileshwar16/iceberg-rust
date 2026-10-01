// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use iceberg::io::{CLIENT_IO_TIMEOUT_MS, FileIO, FileIOBuilder};
use opendal::raw::*;
use opendal::{Buffer, Capability, EntryMode, Metadata, OperationContext, Result};
use tokio::time::{Instant, sleep};

use super::*;

#[derive(Debug, Default)]
pub(super) struct Counts {
    pub(super) writes: AtomicUsize,
    closes: AtomicUsize,
    completed: AtomicUsize,
}

#[derive(Debug, Clone)]
struct SlowService {
    write_delay: Duration,
    close_delay: Duration,
    counts: Arc<Counts>,
}

impl Service for SlowService {
    type Reader = ();
    type Writer = SlowService;
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("diagnostic")
    }
    fn capability(&self) -> Capability {
        Capability {
            write: true,
            ..Default::default()
        }
    }
    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(self.clone())
    }
    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unsupported()
    }
    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        unsupported()
    }
    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        unsupported()
    }
    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unsupported()
    }
    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        unsupported()
    }
    fn copy(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpCopy,
        _: OpCopier,
    ) -> Result<Self::Copier> {
        unsupported()
    }
    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unsupported()
    }
    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unsupported()
    }
}

fn unsupported<T>() -> Result<T> {
    Err(opendal::Error::new(
        opendal::ErrorKind::Unsupported,
        "diagnostic only implements writing",
    ))
}

impl oio::Write for SlowService {
    async fn write(&mut self, _: Buffer) -> Result<()> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        sleep(self.write_delay).await;
        self.counts.completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn close(&mut self) -> Result<Metadata> {
        self.counts.closes.fetch_add(1, Ordering::SeqCst);
        sleep(self.close_delay).await;
        Ok(Metadata::new(EntryMode::FILE))
    }
    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

pub(super) fn slow(write_delay: Duration, close_delay: Duration) -> (Operator, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let srv = SlowService {
        write_delay,
        close_delay,
        counts: counts.clone(),
    };
    (
        Operator::from_parts(OperationContext::default(), Arc::new(srv)),
        counts,
    )
}

#[tokio::test(start_paused = true)]
async fn test_writer_io_timeout_and_retries() {
    // Build the real factory configuration, then substitute a slow backend so
    // this exercises the production layers and OutputFile without network I/O.
    for (millis, write_secs, close_secs, succeeds) in [
        (None, 11, 0, false),
        (Some(20000), 11, 11, true),
        (None, 6, 6, true),
        (None, 0, 11, false),
        (Some(20000), 0, 11, true),
        (Some(5000), 6, 0, false),
    ] {
        let config = match millis {
            Some(millis) => {
                StorageConfig::new().with_prop(CLIENT_IO_TIMEOUT_MS, millis.to_string())
            }
            None => StorageConfig::new(),
        };
        let mut storage = OpenDalStorageFactory::Memory
            .build_storage(&config)
            .unwrap();
        let (op, counts) = slow(
            Duration::from_secs(write_secs),
            Duration::from_secs(close_secs),
        );
        storage.storage = OpenDalStorage::Memory(op);
        let output = storage.new_output("memory:/slow").unwrap();
        let mut writer = output.writer().await.unwrap();
        let start = Instant::now();
        let result = async {
            writer.write(Bytes::from_static(b"payload")).await?;
            writer.close().await?;
            Ok::<_, Error>(())
        }
        .await;

        if succeeds {
            result.unwrap();
            assert_eq!(counts.writes.load(Ordering::SeqCst), 1);
            assert_eq!(counts.closes.load(Ordering::SeqCst), 1);
            assert_eq!(
                start.elapsed(),
                Duration::from_secs(write_secs + close_secs)
            );
        } else {
            let error = result.unwrap_err();
            let timeout_secs = millis.unwrap_or(10000) / 1000;
            assert!(format!("{error:?}").contains("io operation timeout reached"));
            assert!(format!("{error:?}").contains(&format!("timeout: {timeout_secs}")));
            let attempts = if write_secs > timeout_secs {
                assert_eq!(counts.completed.load(Ordering::SeqCst), 0);
                &counts.writes
            } else {
                &counts.closes
            };
            assert_eq!(attempts.load(Ordering::SeqCst), 4);
            assert_eq!(start.elapsed(), Duration::from_secs(4 * timeout_secs + 7));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn test_output_write_retains_timeout() {
    let config = StorageConfig::new().with_prop(CLIENT_IO_TIMEOUT_MS, "20000");
    let mut storage = OpenDalStorageFactory::Memory
        .build_storage(&config)
        .unwrap();
    let (op, counts) = slow(Duration::from_secs(11), Duration::ZERO);
    storage.storage = OpenDalStorage::Memory(op);
    storage
        .new_output("memory:/slow")
        .unwrap()
        .write("data".into())
        .await
        .unwrap();
    assert_eq!(counts.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn test_timeout_reaches_multipart_executor() {
    let storage = OpenDalStorage::Memory(default_memory_operator());
    for duration in [None, Some(Duration::from_millis(12345))] {
        let (op, _) = storage.create_operator(&"memory:/part", duration).unwrap();
        let (ctx, _) = op.into_parts();
        let start = Instant::now();
        ctx.executor().clone().into_inner().timeout().unwrap().await;
        assert_eq!(start.elapsed(), duration.unwrap_or(Duration::from_secs(10)));
    }
}

#[test]
fn test_factories_reject_invalid_timeout() {
    for factory in [
        Arc::new(OpenDalStorageFactory::Memory) as Arc<dyn StorageFactory>,
        Arc::new(OpenDalResolvingStorageFactory::new()),
    ] {
        for value in ["0", "-1", "invalid", "18446744073709551616"] {
            let io = FileIOBuilder::new(factory.clone())
                .with_prop(CLIENT_IO_TIMEOUT_MS, value)
                .build();
            let error = io.new_output("memory:/invalid").unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
            assert!(error.message().contains(CLIENT_IO_TIMEOUT_MS));
        }
    }
}

#[tokio::test]
async fn test_timeout_file_io_serialization() {
    for factory in [
        Arc::new(OpenDalStorageFactory::Memory) as Arc<dyn StorageFactory>,
        Arc::new(OpenDalResolvingStorageFactory::new()),
    ] {
        let io = FileIOBuilder::new(factory)
            .with_prop(CLIENT_IO_TIMEOUT_MS, "20000")
            .build();
        let io = FileIO::deserialize_all(&io.serialize_all().unwrap()).unwrap();
        assert_eq!(
            io.config().get(CLIENT_IO_TIMEOUT_MS).map(String::as_str),
            Some("20000")
        );
        let output = io.new_output("memory:/roundtrip").unwrap();
        output.write("data".into()).await.unwrap();
        let input = io.new_input("memory:/roundtrip").unwrap();
        assert_eq!(input.read().await.unwrap(), Bytes::from_static(b"data"));
        io.delete("memory:/roundtrip").await.unwrap();
        assert!(!io.exists("memory:/roundtrip").await.unwrap());
    }
}
