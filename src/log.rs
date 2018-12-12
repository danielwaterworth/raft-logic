use std::collections::{VecDeque};
use std::cmp::Ordering;

pub type Term = u64;
pub type LogIndex = u64;
pub type LogVersion = Option<(LogIndex, Term)>;

pub fn compare_log_versions(a: LogVersion, b: LogVersion) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Less,
        (_, None) => Ordering::Greater,
        (Some((a_index, a_term)), Some((b_index, b_term))) => {
            match a_index.cmp(&b_index) {
                Ordering::Less => {
                    Ordering::Less
                },
                Ordering::Greater => {
                    Ordering::Greater
                },
                Ordering::Equal => {
                    a_term.cmp(&b_term)
                },
            }
        }
    }
}

pub enum InsertResult {
    Success {
        index: LogIndex,
    },
}

pub enum GetResult<Snapshot, Entry> {
    Entry {
        term: Term,
        entry: Entry,
    },
    Snapshot {
        snapshot: Snapshot,
        version: LogVersion,
    },
    Fail,
}

pub trait Log {
    type Entry: Clone;
    type Snapshot;

    fn empty() -> Self;

    fn commit(&mut self, index: LogIndex);
    fn insert(&mut self, prev: LogVersion, entry: (Term, Self::Entry))
        -> InsertResult;
    fn append(&mut self, term: Term, entry: Self::Entry);

    fn get(&self, index: LogIndex) -> GetResult<Self::Snapshot, Self::Entry>;
    fn version(&self) -> LogVersion;
}

struct TestEntry {
    value: &'static str,
    term: Term,
}

pub struct TestLog {
    entries: Vec<TestEntry>,
    next_commit_index: usize,
}

impl Log for TestLog {
    type Entry = &'static str;
    type Snapshot = !;

    fn empty() -> TestLog {
        TestLog {
            entries: Vec::new(),
            next_commit_index: 0,
        }
    }

    fn commit(&mut self, index: LogIndex) {
        self.next_commit_index = (index + 1) as usize;
    }

    fn insert(&mut self, prev: LogVersion, entry: (Term, &'static str))
            -> InsertResult {
        unimplemented!()
    }

    fn append(&mut self, term: Term, value: &'static str) {
        self.entries.push(
            TestEntry {
                term,
                value,
            }
        );
    }

    fn get(&self, index: LogIndex) -> GetResult<!, &'static str> {
        unimplemented!()
    }

    fn version(&self) -> LogVersion {
        self.entries.last().map(|last| {
            ((self.entries.len() - 1) as u64, last.term)
        })
    }
}