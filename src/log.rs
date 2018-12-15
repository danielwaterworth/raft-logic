use std::cmp::{max, Ordering};

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
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => a_term.cmp(&b_term),
            }
        }
    }
}

pub enum InsertError {
    NoSuchEntry,
    WrongTerm { index: LogIndex, actual_term: Term },
}

pub type InsertResult = Result<(), InsertError>;

pub enum GetResult<Snapshot, Entry> {
    Entries(Vec<(Term, Entry)>),
    Snapshot {
        snapshot: Snapshot,
        version: LogVersion,
    },
    Fail,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LogStatus {
    Unknown,
    Bad(LogIndex),
    Good(LogIndex),
    UpToDate,
}

impl PartialOrd for LogStatus {
    fn partial_cmp(&self, other: &LogStatus) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogStatus {
    fn cmp(&self, other: &LogStatus) -> Ordering {
        match (self, other) {
            (LogStatus::Unknown, LogStatus::Unknown) => Ordering::Equal,
            (LogStatus::Unknown, _) => Ordering::Less,
            (_, LogStatus::Unknown) => Ordering::Greater,
            (LogStatus::Bad(a), LogStatus::Bad(b)) => b.cmp(a),
            (LogStatus::Bad(_), _) => Ordering::Less,
            (_, LogStatus::Bad(_)) => Ordering::Greater,
            (LogStatus::Good(a), LogStatus::Good(b)) => a.cmp(b),
            (LogStatus::Good(_), _) => Ordering::Less,
            (_, LogStatus::Good(_)) => Ordering::Greater,
            (LogStatus::UpToDate, LogStatus::UpToDate) => Ordering::Equal,
        }
    }
}

pub trait Log {
    type Entry: Clone;
    type Snapshot;

    fn empty() -> Self;

    // Used by followers
    fn commit(&mut self, index: LogIndex);
    fn insert(
        &mut self,
        prev: LogVersion,
        entries: &[(Term, Self::Entry)],
    ) -> InsertResult;

    // Used by leader
    fn append(&mut self, term: Term, entry: Self::Entry) -> LogIndex;
    fn get(&self, index: LogIndex) -> GetResult<Self::Snapshot, Self::Entry>;
    fn check(&self, index: LogIndex, term: Term) -> LogStatus;

    // Used during elections
    fn version(&self) -> LogVersion;
}

#[derive(Clone)]
struct TestEntry {
    value: usize,
    term: Term,
}

#[derive(Clone)]
pub struct TestLog {
    entries: Vec<TestEntry>,
    next_commit_index: usize,
}

impl Log for TestLog {
    type Entry = usize;
    type Snapshot = !;

    fn empty() -> TestLog {
        TestLog {
            entries: Vec::new(),
            next_commit_index: 0,
        }
    }

    fn commit(&mut self, index: LogIndex) {
        self.next_commit_index =
            max(self.next_commit_index, (index + 1) as usize);
    }

    fn insert(
        &mut self,
        prev: LogVersion,
        entries: &[(Term, usize)],
    ) -> InsertResult {
        let mut index = match prev {
            None => 0,
            Some((index, term)) => match self.entries.get(index as usize) {
                None => return Err(InsertError::NoSuchEntry),
                Some(entry) => {
                    if entry.term != term {
                        return Err(InsertError::WrongTerm {
                            index,
                            actual_term: entry.term,
                        });
                    } else {
                        index + 1
                    }
                }
            },
        };
        for entry in entries.iter() {
            if self.entries.len() > index as usize {
                if self.entries[index as usize].term != entry.0 {
                    self.entries.truncate(index as usize);
                }
            } else {
                self.entries.push(TestEntry {
                    term: entry.0,
                    value: entry.1,
                });
            }
            index += 1;
        }
        Ok(())
    }

    fn append(&mut self, term: Term, value: usize) -> LogIndex {
        let index = self.entries.len() as LogIndex;
        self.entries.push(TestEntry { term, value });
        index
    }

    fn get(&self, index: LogIndex) -> GetResult<!, usize> {
        unimplemented!()
    }

    fn version(&self) -> LogVersion {
        self.entries
            .last()
            .map(|last| ((self.entries.len() - 1) as u64, last.term))
    }

    fn check(&self, index: LogIndex, term: Term) -> LogStatus {
        match self.entries.get(index as usize) {
            None => LogStatus::Bad(index),
            Some(entry) => {
                if entry.term == term {
                    if index as usize == self.entries.len() - 1 {
                        LogStatus::UpToDate
                    } else {
                        LogStatus::Good(index)
                    }
                } else {
                    LogStatus::Bad(index)
                }
            }
        }
    }
}
