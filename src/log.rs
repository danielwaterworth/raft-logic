use std::collections::{VecDeque};

pub type Term = u64;
pub type LogIndex = u64;
pub type LogVersion = Option<(LogIndex, Term)>;

pub struct Log<Entry> {
    last_committed: LogVersion,
    log: VecDeque<(Term, Entry)>,
}

impl<Entry> Log<Entry> {
    pub fn empty() -> Log<Entry> {
        Log {
            last_committed: None,
            log: VecDeque::default(),
        }
    }

    pub fn insert(&mut self, term: Term, entry: Entry) -> LogIndex {
        unimplemented!()
    }

    pub fn next_index(&self) -> LogIndex {
        self.log.len() as u64 +
        match self.last_committed {
            None => 0,
            Some(x) => x.0
        }
    }

    pub fn last_index(&self) -> Option<LogIndex> {
        match self.next_index() {
            0 => None,
            x => Some(x - 1),
        }
    }

    pub fn last_term(&self) -> Option<Term> {
        self.log
            .back()
            .map(|(term, _)| *term)
            .or(self.last_committed.map(|(term, _)| term))
    }

    pub fn version(&self) -> LogVersion {
        self.last_index().and_then(|index|
            self.last_term().map(|term|
                (term, index)
            )
        )
    }
}

