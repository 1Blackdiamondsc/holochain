/// The ChainSequence database serves several purposes:
/// - enables fast forward iteration over the entire source chain
/// - knows what the chain head is, by accessing the last item's header address
/// - stores information about which headers were committed in the same transactional bundle
/// - stores info about whether each entry has undergone DHT op generation and publishing
///
/// When committing the ChainSequence db, a special step is taken to ensure source chain consistency.
/// If the chain head has moved since the db was created, committing the transaction fails with a special error type.
use sx_state::{
    buffer::{KvIntBuffer, StoreBuffer},
    db::{DbManager, DbName, CHAIN_SEQUENCE},
    error::{WorkspaceError, WorkspaceResult},
    Readable, Reader, RkvEnv, Writer,
};
use sx_types::prelude::Address;

/// A Value in the ChainSequence database.
#[derive(Clone, Serialize, Deserialize)]
pub struct ChainSequenceItem {
    header_address: Address,
    index: u32, // TODO: this is the key, so once iterators can return keys, we can remove this
    tx_seq: u32,
    dht_transforms_complete: bool,
}

type Store<'e, R> = KvIntBuffer<'e, u32, ChainSequenceItem, R>;

pub struct ChainSequenceBuffer<'e, R: Readable> {
    db: Store<'e, R>,
    next_index: u32,
    tx_seq: u32,
    current_head: Option<Address>,
    persisted_head: Option<Address>,
}

impl<'e, R: Readable> ChainSequenceBuffer<'e, R> {
    pub fn new(reader: &'e R, dbm: &'e DbManager<'e>) -> WorkspaceResult<Self> {
        let db: Store<'e, R> = KvIntBuffer::new(reader, dbm.get(&*CHAIN_SEQUENCE)?.clone())?;
        Self::from_db(db)
    }

    pub fn with_reader<RR: Readable>(
        &self,
        reader: &'e RR,
    ) -> WorkspaceResult<ChainSequenceBuffer<'e, RR>> {
        Self::from_db(self.db.with_reader(reader))
    }

    fn from_db<RR: Readable>(db: Store<'e, RR>) -> WorkspaceResult<ChainSequenceBuffer<'e, RR>> {
        let latest = db.iter_raw_reverse()?.next();
        let (next_index, tx_seq, current_head) = latest
            .map(|(_, item)| (item.index + 1, item.tx_seq + 1, Some(item.header_address)))
            .unwrap_or((0, 0, None));
        let persisted_head = current_head.clone();

        Ok(ChainSequenceBuffer {
            db,
            next_index,
            tx_seq,
            current_head,
            persisted_head,
        })
    }

    pub fn chain_head(&self) -> Option<&Address> {
        self.current_head.as_ref()
    }

    pub fn add_header(&mut self, header_address: Address) {
        self.db.put(
            self.next_index,
            ChainSequenceItem {
                header_address: header_address.clone(),
                index: self.next_index,
                tx_seq: self.tx_seq,
                dht_transforms_complete: false,
            },
        );
        self.next_index += 1;
        self.current_head = Some(header_address);
    }
}

impl<'env, R: Readable> StoreBuffer<'env> for ChainSequenceBuffer<'env, R> {
    fn finalize(self, writer: &'env mut Writer) -> WorkspaceResult<()> {
        let fresh = self.with_reader(writer)?;
        if fresh.persisted_head != self.persisted_head {
            Err(WorkspaceError::SourceChainHeadMoved)
        } else {
            self.db.finalize(writer)
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::{ChainSequenceBuffer, StoreBuffer};
    use std::sync::Arc;
    use sx_state::{
        db::{DbManager, ReadManager, WriteManager},
        env::create_lmdb_env,
        error::{WorkspaceError, WorkspaceResult},
        test_utils::test_env,
    };
    use sx_types::prelude::Address;
    use tempdir::TempDir;

    #[test]
    fn chain_sequence_scratch_awareness() -> WorkspaceResult<()> {
        let arc = test_env();
        let env = arc.read().unwrap();
        let dbm = DbManager::new(&env)?;
        let rm = ReadManager::new(&env);
        let wm = WriteManager::new(&env);
        rm.with_reader(|reader| {
            let mut buf = ChainSequenceBuffer::new(&reader, &dbm)?;
            assert_eq!(buf.chain_head(), None);
            buf.add_header(Address::from("0"));
            assert_eq!(buf.chain_head(), Some(&Address::from("0")));
            buf.add_header(Address::from("1"));
            assert_eq!(buf.chain_head(), Some(&Address::from("1")));
            buf.add_header(Address::from("2"));
            assert_eq!(buf.chain_head(), Some(&Address::from("2")));
            Ok(())
        })?;

        Ok(())
    }

    #[test]
    fn chain_sequence_functionality() -> WorkspaceResult<()> {
        let arc = test_env();
        let env = arc.read().unwrap();
        let dbm = DbManager::new(&env)?;
        let rm = ReadManager::new(&env);
        let wm = WriteManager::new(&env);
        rm.with_reader(|reader| {
            let mut buf = ChainSequenceBuffer::new(&reader, &dbm)?;
            buf.add_header(Address::from("0"));
            buf.add_header(Address::from("1"));
            assert_eq!(buf.chain_head(), Some(&Address::from("1")));
            buf.add_header(Address::from("2"));
            wm.with_writer(|mut writer| buf.finalize(&mut writer))?;
            Ok(())
        })?;

        rm.with_reader(|reader| {
            let buf = ChainSequenceBuffer::new(&reader, &dbm)?;
            assert_eq!(buf.chain_head(), Some(&Address::from("2")));
            let items: Vec<u32> = buf.db.iter_raw()?.map(|(_, i)| i.index).collect();
            assert_eq!(items, vec![0, 1, 2]);
            Ok(())
        })?;

        rm.with_reader(|reader| {
            let mut buf = ChainSequenceBuffer::new(&reader, &dbm)?;
            buf.add_header(Address::from("3"));
            buf.add_header(Address::from("4"));
            buf.add_header(Address::from("5"));
            wm.with_writer(|mut writer| buf.finalize(&mut writer))?;
            Ok(())
        })?;

        rm.with_reader(|reader| {
            let buf = ChainSequenceBuffer::new(&reader, &dbm)?;
            assert_eq!(buf.chain_head(), Some(&Address::from("5")));
            let items: Vec<u32> = buf.db.iter_raw()?.map(|(_, i)| i.tx_seq).collect();
            assert_eq!(items, vec![0, 0, 0, 1, 1, 1]);
            Ok(())
        })?;

        Ok(())
    }

    #[tokio::test]
    async fn chain_sequence_head_moved() -> anyhow::Result<()> {
        let arc = test_env();
        let arc1 = arc.clone();
        let arc2 = arc.clone();
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();

        let local = tokio::task::LocalSet::new();

        // run in same thread, because these futures are not Send...or are they?
        let (result1, result2) = local.run_until(async move {

            let task1 = tokio::task::spawn_local(async move {
                let env = arc1.read().unwrap();
                let dbm = DbManager::new(&env)?;
                let rm = ReadManager::new(&env);
                let reader = rm.reader()?;
                let mut buf = ChainSequenceBuffer::new(&reader, &dbm)?;
                buf.add_header(Address::from("0"));
                buf.add_header(Address::from("1"));
                buf.add_header(Address::from("2"));

                // let the other task run and make a commit to the chain head,
                // which will cause this one to error out when it re-enters and tries to commit
                tx1.send(()).unwrap();
                rx2.await.unwrap();

                let env = arc1.read().unwrap();
                let wm = WriteManager::new(&env);
                wm.with_writer(|mut writer| buf.finalize(&mut writer))
            });

            let task2 = tokio::task::spawn_local(async move {
                rx1.await.unwrap();
                let env = arc2.read().unwrap();
                let dbm = DbManager::new(&env)?;
                let rm = ReadManager::new(&env);
                let wm = WriteManager::new(&env);

                let reader = rm.reader()?;
                let mut buf = ChainSequenceBuffer::new(&reader, &dbm)?;
                buf.add_header(Address::from("3"));
                buf.add_header(Address::from("4"));
                buf.add_header(Address::from("5"));

                wm.with_writer(|mut writer| buf.finalize(&mut writer))?;
                tx2.send(()).unwrap();
                Result::<_, WorkspaceError>::Ok(())
            });

            tokio::join!(task1, task2)
        }).await;

        assert_eq!(result1.unwrap(), Err(WorkspaceError::SourceChainHeadMoved));
        assert!(result2.unwrap().is_ok());

        Ok(())
    }
}