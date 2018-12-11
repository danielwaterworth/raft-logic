use std::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::cmp::Ordering;
use std::mem;

type Term = u64;
type LogIndex = u64;
type LogVersion = Option<(LogIndex, Term)>;

struct Log<Entry> {
    last_committed: LogVersion,
    log: VecDeque<(Term, Entry)>,
}

impl<Entry> Log<Entry> {
    fn empty() -> Log<Entry> {
        Log {
            last_committed: None,
            log: VecDeque::default(),
        }
    }

    fn insert(&mut self, term: Term, entry: Entry) -> LogIndex {
        unimplemented!()
    }

    fn version(&self) -> LogVersion {
        unimplemented!()
    }
}

fn compare_log_versions(a: LogVersion, b: LogVersion) -> Ordering {
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

enum State<ServerID> {
    Follower {
        voted_for: Option<ServerID>,
    },
    Candidate {
        votes_received: HashSet<ServerID>,
    },
    Leader {
        next_index: HashMap<ServerID, LogIndex>,
        match_index: HashMap<ServerID, LogIndex>,
    },
}

pub struct Node<ServerID: Hash + Eq, Entry> {
    server_id: ServerID,
    servers: HashSet<ServerID>,

    current_term: Term,
    state: State<ServerID>,

    log: Log<Entry>,
}

pub enum Message<Entry> {
    ApplyEntriesRequest {
        log_version: LogVersion,
        entries: Vec<Entry>,
    },
    RequestVote {
        version: LogVersion,
    },
    VoteAccepted,
    VoteRejected,
    EntriesApplied {

    },
    EntriesNotApplied {

    },
}

pub enum RaftInput<ServerID, Entry> {
    ClientRequest {
        entry: Entry,
    },
    OnMessage {
        term: Term,
        server_id: ServerID,
        message: Message<Entry>,
    },
    Timeout,
}

pub enum RaftOutput<ServerID, Entry> {
    AckClientRequest,
    NackClientRequest {
        current_leader: Option<ServerID>,
    },
    SendMessage {
        term: Term,
        server_id: ServerID,
        message: Message<Entry>,
    },
    SetTimeout,
    ClearTimeout,
    Commit { entry: Entry },
}

pub enum OutputIterator<'a, ServerID, Entry> {
    Ignore,
    AcceptedVote { server_id: ServerID },
    RejectedVote { server_id: ServerID },
    AcceptedClientRequest,
    RejectedClientRequest,
    EntriesApplied,
    EntriesNotApplied,
    TransitionToFollower {
        then: Box<OutputIterator<'a, ServerID, Entry>>,
    },
    TransitionToCandidate {
        servers: &'a HashSet<ServerID>,
    },
    TransitionToLeader,
    FreeForm(VecDeque<RaftOutput<ServerID, Entry>>),
}

impl<'a, ServerID, Entry> Iterator for OutputIterator<'a, ServerID, Entry> {
    type Item = RaftOutput<ServerID, Entry>;

    fn next(&mut self) -> Option<RaftOutput<ServerID, Entry>> {
        match self {
            OutputIterator::Ignore => None,
            OutputIterator::AcceptedVote { server_id } => {
                unimplemented!()
            },
            OutputIterator::RejectedVote { server_id } => {
                unimplemented!()
            },
            OutputIterator::AcceptedClientRequest => {
                unimplemented!()
            },
            OutputIterator::RejectedClientRequest => {
                unimplemented!()
            },
            OutputIterator::EntriesApplied => {
                unimplemented!()
            },
            OutputIterator::EntriesNotApplied => {
                unimplemented!()
            },
            OutputIterator::FreeForm(ref mut items) => {
                unimplemented!()
            },
            OutputIterator::TransitionToFollower { then } => {
                let mut tmp = OutputIterator::Ignore;
                mem::swap(&mut tmp, &mut **then);
                *self = tmp;
                Some(RaftOutput::SetTimeout)
            },
            OutputIterator::TransitionToCandidate { servers } => {
                unimplemented!()
            },
            OutputIterator::TransitionToLeader => {
                unimplemented!()
            },
        }
    }
}

impl<ServerID: Hash + Eq + Clone, Entry: Clone> Node<ServerID, Entry> {
    pub fn new(server_id: ServerID, servers: HashSet<ServerID>)
            -> Node<ServerID, Entry> {
        Node {
            server_id,
            servers,
            current_term: 0,
            state: State::Follower { voted_for: None },
            log: Log::empty(),
        }
    }

    pub fn process(&mut self, input: &RaftInput<ServerID, Entry>)
            -> OutputIterator<ServerID, Entry> {
        match input {
            RaftInput::OnMessage { message, term, server_id } => {
                if self.current_term > *term {
                    return OutputIterator::Ignore;
                }
                if *term > self.current_term {
                    self.current_term = *term;
                    self.state = State::Follower { voted_for: None };
                    return OutputIterator::TransitionToFollower {
                        then: Box::new(self.process(input)),
                    }
                }
            },
            _ => {}
        }

        match &mut self.state {
            State::Follower { voted_for } => {
                match input {
                    RaftInput::ClientRequest { entry } => {
                        OutputIterator::RejectedClientRequest
                    },
                    RaftInput::OnMessage { message, term, server_id } => {
                        match message {
                            Message::RequestVote { version } => {
                                let cmp =
                                    compare_log_versions(
                                        *version,
                                        self.log.version(),
                                    ) != Ordering::Less;

                                if voted_for.is_none() && cmp {
                                    *voted_for = Some(server_id.clone());

                                    OutputIterator::AcceptedVote {
                                        server_id: server_id.clone()
                                    }
                                } else {
                                    OutputIterator::RejectedVote {
                                        server_id: server_id.clone()
                                    }
                                }
                            },
                            Message::ApplyEntriesRequest {
                                        entries, log_version
                                    } => {
                                if self.log.version() == *log_version {
                                    unimplemented!();
                                    OutputIterator::EntriesApplied
                                } else {
                                    OutputIterator::EntriesNotApplied
                                }
                            },
                            Message::VoteAccepted | Message::VoteRejected => {
                                OutputIterator::Ignore
                            },
                            Message::EntriesApplied {} |
                            Message::EntriesNotApplied {} => {
                                unreachable!("\
                                    Cannot transition from leader to follower \
                                    in the same term\
                                ")
                            },
                        }
                    },
                    RaftInput::Timeout => {
                        self.current_term += 1;

                        self.state =
                            State::Candidate {
                                votes_received: HashSet::default(),
                            };

                        OutputIterator::TransitionToCandidate {
                            servers: &self.servers
                        }
                    },
                }
            },
            State::Candidate { votes_received } => {
                match input {
                    RaftInput::ClientRequest { entry } => {
                        OutputIterator::RejectedClientRequest
                    },
                    RaftInput::OnMessage { message, term, server_id } => {
                        match message {
                            Message::RequestVote { version } => {
                                OutputIterator::RejectedVote {
                                    server_id: server_id.clone()
                                }
                            },
                            Message::ApplyEntriesRequest {
                                        entries, log_version
                                    } => {
                                self.state =
                                    State::Follower {
                                        voted_for:
                                            Some(self.server_id.clone())
                                    };

                                OutputIterator::TransitionToFollower {
                                    then: Box::new(self.process(input)),
                                }
                            },
                            Message::VoteAccepted => {
                                votes_received.insert(server_id.clone());
                                let num_servers = self.servers.len();
                                let votes = votes_received.len() + 1;
                                if votes * 2 > self.servers.len() {
                                    self.state =
                                        State::Leader {
                                            next_index: HashMap::default(),
                                            match_index: HashMap::default(),
                                        };
                                    OutputIterator::TransitionToLeader
                                } else {
                                    OutputIterator::Ignore
                                }
                            },
                            Message::VoteRejected => {
                                OutputIterator::Ignore
                            },
                            Message::EntriesApplied {} |
                            Message::EntriesNotApplied {} => {
                                unreachable!("\
                                    Cannot transition from leader to \
                                    candidate in the same term\
                                ")
                            },
                        }
                    },
                    RaftInput::Timeout => {
                        self.current_term += 1;

                        self.state =
                            State::Candidate {
                                votes_received: HashSet::default(),
                            };

                        OutputIterator::TransitionToCandidate {
                            servers: &self.servers
                        }
                    },
                }
            },
            State::Leader { next_index, match_index } => {
                match input {
                    RaftInput::ClientRequest { entry } => {
                        let log_id =
                            self.log.insert(self.current_term, entry.clone());
                        OutputIterator::AcceptedClientRequest
                    },
                    RaftInput::OnMessage { message, term, server_id } => {
                        match message {
                            Message::RequestVote { version } => {
                                OutputIterator::RejectedVote {
                                    server_id: server_id.clone()
                                }
                            },
                            Message::ApplyEntriesRequest {
                                        entries, log_version
                                    } => {
                                unreachable!(
                                    "cannot have two leaders in the same term"
                                )
                            },
                            Message::VoteAccepted | Message::VoteRejected => {
                                OutputIterator::Ignore
                            },
                            Message::EntriesApplied {} => {
                                unimplemented!()
                            },
                            Message::EntriesNotApplied {} => {
                                unimplemented!()
                            },
                        }
                    },
                    RaftInput::Timeout => {
                        unreachable!("No timer should have been set")
                    },
                }
            },
        }
    }
}
