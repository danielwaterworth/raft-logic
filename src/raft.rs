use crate::log::{
    compare_log_versions, GetResult, InsertError, Log, LogIndex, LogStatus,
    LogVersion, Term,
};
use crate::singleton::Singleton;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::mem;

#[derive(Clone)]
enum FollowerState<ServerID> {
    Oblivious,
    Voted(ServerID),
    Following(ServerID),
}

impl<ServerID> FollowerState<ServerID> {
    fn can_vote(&self) -> bool {
        match self {
            FollowerState::Oblivious => true,
            _ => false,
        }
    }
}

impl<ServerID: Clone> FollowerState<ServerID> {
    fn current_leader(&self) -> Option<ServerID> {
        match self {
            FollowerState::Following(leader) => Some(leader.clone()),
            _ => None,
        }
    }
}

#[derive(Clone)]
enum State<ServerID> {
    Follower(FollowerState<ServerID>),
    Candidate {
        votes_received: HashSet<ServerID>,
    },
    Leader {
        follower_infos: HashMap<ServerID, LogStatus>,
    },
}

#[derive(Clone)]
pub struct Node<ServerID: Hash + Eq, L: Log> {
    server_id: ServerID,
    servers: HashSet<ServerID>,

    current_term: Term,
    state: State<ServerID>,

    log: L,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Message<Entry> {
    ApplyEntriesRequest {
        commit: LogVersion,
        log_version: LogVersion,
        entries: Vec<(Term, Entry)>,
    },
    InstallSnapshot,
    RequestVote {
        version: LogVersion,
    },
    VoteAccepted,
    VoteRejected,

    // Response to ApplyEntriesRequest, says that log is at least
    // version,
    LogEntryInfo {
        version: LogVersion,
    },
}

#[derive(Debug, PartialEq)]
pub enum Input<ServerID, Entry> {
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Action<ServerID, Entry> {
    AckClientRequest,
    ClientRequestRejected {
        current_leader: Option<ServerID>,
    },
    SendMessage {
        term: Term,
        server_id: ServerID,
        message: Message<Entry>,
    },
    SetTimeout,
    ClearTimeout,
    Commit {
        entry: Entry,
    },
}

pub enum Operation<'a, ServerID, Entry> {
    DoNothing,
    OneAction(Singleton<Action<ServerID, Entry>>),
    TransitionToFollower {
        then: Box<Operation<'a, ServerID, Entry>>,
    },
    Phantom(&'a u8),
    FreeForm(VecDeque<Action<ServerID, Entry>>),
}

impl<'a, ServerID, Entry> Iterator for Operation<'a, ServerID, Entry> {
    type Item = Action<ServerID, Entry>;

    fn next(&mut self) -> Option<Action<ServerID, Entry>> {
        match self {
            Operation::DoNothing => None,
            Operation::OneAction(iter) => iter.next(),
            Operation::FreeForm(ref mut items) => items.pop_front(),
            Operation::TransitionToFollower { then } => {
                let mut tmp = do_nothing();
                mem::swap(&mut tmp, &mut **then);
                *self = tmp;
                Some(Action::SetTimeout)
            }
            Operation::Phantom(_) => unreachable!(),
        }
    }
}

fn do_nothing<'a, ServerID, Entry>() -> Operation<'a, ServerID, Entry> {
    Operation::DoNothing
}

fn accepted_vote<'a, ServerID, Entry>(
    term: Term,
    server_id: ServerID,
) -> Operation<'a, ServerID, Entry> {
    Operation::OneAction(Singleton::new(Action::SendMessage {
        term,
        server_id,
        message: Message::VoteAccepted,
    }))
}

fn rejected_vote<'a, ServerID, Entry>(
    term: Term,
    server_id: ServerID,
) -> Operation<'a, ServerID, Entry> {
    Operation::OneAction(Singleton::new(Action::SendMessage {
        term,
        server_id,
        message: Message::VoteRejected,
    }))
}

fn accepted_client_request<'a, ServerID, Entry>(
    server_ids: Vec<ServerID>,
    current_term: Term,
    entry: Entry,
) -> Operation<'a, ServerID, Entry>
where
    ServerID: Eq + Hash + Clone,
    Entry: Clone,
{
    let mut actions: VecDeque<Action<ServerID, Entry>> = VecDeque::new();
    for server_id in server_ids.into_iter() {
        actions.push_back(Action::SendMessage {
            term: current_term,
            server_id: server_id.clone(),
            message: Message::ApplyEntriesRequest {
                commit: None,
                log_version: None,
                entries: vec![(current_term, entry.clone())],
            },
        });
    }

    Operation::FreeForm(actions)
}

fn rejected_client_request<'a, ServerID, Entry>(
    current_leader: Option<ServerID>,
) -> Operation<'a, ServerID, Entry> {
    Operation::OneAction(Singleton::new(Action::ClientRequestRejected {
        current_leader,
    }))
}

fn transition_to_follower<ServerID, Entry>(
    then: Operation<ServerID, Entry>,
) -> Operation<ServerID, Entry> {
    Operation::TransitionToFollower {
        then: Box::new(then),
    }
}

fn transition_to_candidate<'a, ServerID, Entry>(
    term: Term,
    servers: &HashSet<ServerID>,
    except: &ServerID,
    version: LogVersion,
) -> Operation<'a, ServerID, Entry>
where
    ServerID: Hash + Eq + Clone,
{
    let mut actions: VecDeque<Action<ServerID, Entry>> = servers
        .iter()
        .filter(|server_id| **server_id != *except)
        .map(|server_id| Action::SendMessage {
            server_id: server_id.clone(),
            term,
            message: Message::RequestVote { version },
        })
        .collect();
    actions.push_back(Action::SetTimeout);

    Operation::FreeForm(actions)
}

fn transition_to_leader<'a, ServerID, Entry>(
    term: Term,
    servers: &HashSet<ServerID>,
    except: &ServerID,
    version: LogVersion,
) -> Operation<'a, ServerID, Entry>
where
    ServerID: Hash + Eq + Clone,
{
    let mut actions: VecDeque<Action<ServerID, Entry>> = servers
        .iter()
        .filter(|server_id| *server_id != except)
        .map(|server_id| Action::SendMessage {
            server_id: server_id.clone(),
            term,
            message: Message::ApplyEntriesRequest {
                commit: None,
                log_version: version,
                entries: Vec::new(),
            },
        })
        .collect();
    actions.push_back(Action::ClearTimeout);

    Operation::FreeForm(actions)
}

fn entries_applied<'a, ServerID, Entry>(
    current_term: Term,
    leader: ServerID,
    version: LogVersion,
) -> Operation<'a, ServerID, Entry> {
    let mut actions: VecDeque<Action<ServerID, Entry>> = VecDeque::new();
    actions.push_back(Action::SetTimeout);

    actions.push_back(Action::SendMessage {
        term: current_term,
        server_id: leader,
        message: Message::LogEntryInfo { version },
    });

    Operation::FreeForm(actions)
}

fn send_entries<'a, ServerID, Entry>(
    term: Term,
    server_id: ServerID,
    log_version: LogVersion,
    entries: Vec<(Term, Entry)>,
) -> Operation<'a, ServerID, Entry>
where
    ServerID: Hash + Eq + Clone,
{
    Operation::OneAction(Singleton::new(Action::SendMessage {
        term,
        server_id,
        message: Message::ApplyEntriesRequest {
            commit: None, // FIXME
            log_version,
            entries,
        },
    }))
}

impl<ServerID: Hash + Eq + Clone, L: Log> Node<ServerID, L> {
    pub fn new(
        server_id: ServerID,
        servers: HashSet<ServerID>,
    ) -> Node<ServerID, L> {
        Node {
            server_id,
            servers,
            current_term: 0,
            state: State::Follower(FollowerState::Oblivious),
            log: L::empty(),
        }
    }

    fn transition_to_candidate(&mut self) -> Operation<ServerID, L::Entry> {
        self.current_term += 1;

        self.state = State::Candidate {
            votes_received: HashSet::default(),
        };

        transition_to_candidate(
            self.current_term,
            &self.servers,
            &self.server_id,
            self.log.version(),
        )
    }

    fn handle_client_request(
        &mut self,
        entry: L::Entry,
    ) -> Operation<ServerID, L::Entry> {
        match &mut self.state {
            State::Follower(follower_state) => {
                rejected_client_request(follower_state.current_leader())
            }
            State::Candidate { .. } => rejected_client_request(None),
            State::Leader { follower_infos } => {
                let index = self.log.append(self.current_term, entry.clone());

                let mut server_ids = Vec::new();
                for (server_id, follower_info) in follower_infos.iter_mut() {
                    if let LogStatus::UpToDate = follower_info {
                        *follower_info = LogStatus::Good { next: index };
                        server_ids.push(server_id.clone());
                    }
                }

                accepted_client_request(
                    server_ids,
                    self.current_term,
                    entry.clone(),
                )
            }
        }
    }

    fn handle_request_vote(
        &mut self,
        term: Term,
        server_id: ServerID,
        version: LogVersion,
    ) -> Operation<ServerID, L::Entry> {
        match &mut self.state {
            State::Follower(follower_state) => {
                let cmp = compare_log_versions(version, self.log.version())
                    != Ordering::Less;

                if follower_state.can_vote() && cmp {
                    *follower_state = FollowerState::Voted(server_id.clone());
                    accepted_vote(self.current_term, server_id)
                } else {
                    rejected_vote(self.current_term, server_id)
                }
            }
            State::Candidate { votes_received } => {
                rejected_vote(self.current_term, server_id)
            }
            State::Leader { follower_infos } => {
                rejected_vote(self.current_term, server_id)
            }
        }
    }

    fn handle_install_snapshot(
        &mut self,
        term: Term,
        server_id: ServerID,
    ) -> Operation<ServerID, L::Entry> {
        unimplemented!()
    }

    fn handle_apply_entries(
        &mut self,
        term: Term,
        server_id: ServerID,
        commit: LogVersion,
        entries: Vec<(Term, L::Entry)>,
        log_version: LogVersion,
    ) -> Operation<ServerID, L::Entry> {
        match &mut self.state {
            State::Follower(follower_state) => {
                *follower_state = FollowerState::Following(server_id.clone());

                let result = self.log.insert(log_version, &entries);
                match result {
                    Ok(()) | Err(InsertError::NoSuchEntry) => entries_applied(
                        term,
                        server_id.clone(),
                        self.log.version(),
                    ),
                    Err(InsertError::WrongTerm { index, actual_term }) => {
                        entries_applied(
                            term,
                            server_id.clone(),
                            Some((index, actual_term)),
                        )
                    }
                }
            }
            State::Candidate { votes_received } => {
                self.state = State::Follower(FollowerState::Following(
                    self.server_id.clone(),
                ));

                transition_to_follower(self.process(&Input::OnMessage {
                    term,
                    server_id,
                    message: Message::ApplyEntriesRequest {
                        entries,
                        commit,
                        log_version,
                    },
                }))
            }
            State::Leader { follower_infos } => {
                unreachable!("cannot have two leaders in the same term")
            }
        }
    }

    fn handle_vote_accepted(
        &mut self,
        term: Term,
        server_id: ServerID,
    ) -> Operation<ServerID, L::Entry> {
        match &mut self.state {
            State::Follower(follower_state) => do_nothing(),
            State::Candidate { votes_received } => {
                votes_received.insert(server_id.clone());
                let num_servers = self.servers.len();
                let votes = votes_received.len() + 1;
                if votes * 2 > num_servers {
                    let should_sync = self.log.version().is_some();
                    let value = if should_sync {
                        LogStatus::Unknown
                    } else {
                        LogStatus::UpToDate
                    };

                    let follower_infos = self
                        .servers
                        .iter()
                        .filter(|server_id| **server_id != self.server_id)
                        .map(|server_id| (server_id.clone(), value.clone()))
                        .collect();

                    self.state = State::Leader { follower_infos };

                    transition_to_leader(
                        self.current_term,
                        &self.servers,
                        &self.server_id,
                        self.log.version(),
                    )
                } else {
                    do_nothing()
                }
            }
            State::Leader { follower_infos } => do_nothing(),
        }
    }

    fn send_from_zero(
        &mut self,
        server_id: ServerID,
    ) -> Operation<ServerID, L::Entry> {
        match self.log.get(0) {
            GetResult::Entries(entries) => send_entries(
                self.current_term,
                server_id.clone(),
                None,
                entries,
            ),
            GetResult::Snapshot { .. } => unimplemented!(),
            GetResult::Fail => unreachable!(),
        }
    }

    fn handle_log_entry_info(
        &mut self,
        term: Term,
        server_id: ServerID,
        version: LogVersion,
    ) -> Operation<ServerID, L::Entry> {
        match &mut self.state {
            State::Follower(follower_state) => unreachable!(
                "Cannot transition from leader to follower in the same term"
            ),
            State::Candidate { votes_received } => unreachable!(
                "Cannot transition from leader to candidate in the same term"
            ),
            State::Leader { follower_infos } => {
                let follower_info = follower_infos
                    .entry(server_id.clone())
                    .or_insert(LogStatus::Unknown);

                match version {
                    None => {
                        let x = LogStatus::Good { next: 0 };
                        if x > *follower_info {
                            *follower_info = x;

                            self.send_from_zero(server_id.clone())
                        } else {
                            do_nothing()
                        }
                    }
                    Some((entry_index, entry_term)) => {
                        let x = self.log.check(entry_index, entry_term);
                        if x > *follower_info {
                            *follower_info = x;
                            match follower_info {
                                LogStatus::Unknown => unreachable!(),
                                LogStatus::Bad(index) => {
                                    assert_eq!(*index, entry_index);

                                    if *index == 0 {
                                        *follower_info =
                                            LogStatus::Good { next: 0 };
                                        self.send_from_zero(server_id.clone())
                                    } else {
                                        let index = *index - 1;
                                        send_entries(
                                            self.current_term,
                                            server_id.clone(),
                                            Some((index, 0)),
                                            vec![],
                                        )
                                    }
                                }
                                LogStatus::Good { next } => {
                                    assert_eq!(*next, entry_index + 1);

                                    match self.log.get(*next) {
                                        GetResult::Entries(entries) => {
                                            send_entries(
                                                self.current_term,
                                                server_id.clone(),
                                                Some((
                                                    entry_index,
                                                    entry_term,
                                                )),
                                                entries,
                                            )
                                        }
                                        GetResult::Snapshot { .. } => {
                                            unimplemented!()
                                        }
                                        GetResult::Fail => unreachable!(),
                                    }
                                }
                                LogStatus::UpToDate => do_nothing(),
                            }
                        } else if let LogStatus::Unknown = x {
                            unimplemented!("send snapshot")
                        } else {
                            // We received a duplicate response
                            do_nothing()
                        }
                    }
                }
            }
        }
    }

    fn handle_timeout(&mut self) -> Operation<ServerID, L::Entry> {
        match &mut self.state {
            State::Follower(follower_state) => self.transition_to_candidate(),
            State::Candidate { votes_received } => {
                self.transition_to_candidate()
            }
            State::Leader { follower_infos } => do_nothing(),
        }
    }

    pub fn process(
        &mut self,
        input: &Input<ServerID, L::Entry>,
    ) -> Operation<ServerID, L::Entry> {
        return match input {
            Input::ClientRequest { entry } => {
                self.handle_client_request(entry.clone())
            }
            Input::OnMessage {
                message,
                term,
                server_id,
            } => {
                if self.current_term > *term {
                    return do_nothing();
                }
                if *term > self.current_term {
                    self.current_term = *term;
                    self.state = State::Follower(FollowerState::Oblivious);
                    return transition_to_follower(self.process(input));
                }

                match message {
                    Message::RequestVote { version } => self
                        .handle_request_vote(
                            *term,
                            server_id.clone(),
                            *version,
                        ),
                    Message::InstallSnapshot => {
                        self.handle_install_snapshot(*term, server_id.clone())
                    }
                    Message::ApplyEntriesRequest {
                        commit,
                        entries,
                        log_version,
                    } => self.handle_apply_entries(
                        *term,
                        server_id.clone(),
                        *commit,
                        entries.clone(),
                        *log_version,
                    ),
                    Message::VoteAccepted => {
                        self.handle_vote_accepted(*term, server_id.clone())
                    }
                    Message::VoteRejected => do_nothing(),
                    Message::LogEntryInfo { version } => self
                        .handle_log_entry_info(
                            *term,
                            server_id.clone(),
                            *version,
                        ),
                }
            }
            Input::Timeout => self.handle_timeout(),
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::log::TestLog;
    use std::fmt::Debug;

    use super::*;

    type TestNode = Node<&'static str, TestLog>;

    fn expect_actions<ServerID, L: Log>(
        node: &mut Node<ServerID, L>,
        input: &Input<ServerID, L::Entry>,
        expected_actions: Vec<Action<ServerID, L::Entry>>,
    ) where
        ServerID: Hash + Eq + Debug + Clone,
        L::Entry: Hash + Eq + Debug,
    {
        let actions: HashSet<Action<ServerID, L::Entry>> =
            node.process(input).collect();

        let expected_actions: HashSet<Action<ServerID, L::Entry>> =
            expected_actions.into_iter().collect();

        assert_eq!(actions, expected_actions);
    }

    #[test]
    fn follower_apply_entries_1() {
        let mut server_ids = HashSet::new();
        server_ids.insert("a");
        server_ids.insert("b");
        server_ids.insert("c");

        let mut a: TestNode = Node::new("a", server_ids.clone());

        // From an empty log, a leader should be able to add an entry
        expect_actions(
            &mut a,
            &Input::OnMessage {
                term: 1,
                server_id: "b",
                message: Message::ApplyEntriesRequest {
                    commit: None,
                    log_version: None,
                    entries: vec![(1, 1)],
                },
            },
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message: Message::LogEntryInfo {
                        version: Some((0, 1)),
                    },
                },
                Action::SetTimeout,
            ],
        );

        // A new leader should be able to overwrite the value previously
        // written
        expect_actions(
            &mut a,
            &Input::OnMessage {
                term: 2,
                server_id: "c",
                message: Message::ApplyEntriesRequest {
                    commit: None,
                    log_version: None,
                    entries: vec![(2, 2)],
                },
            },
            vec![
                Action::SendMessage {
                    term: 2,
                    server_id: "c",
                    message: Message::LogEntryInfo {
                        version: Some((0, 2)),
                    },
                },
                Action::SetTimeout,
            ],
        );
    }

    #[test]
    fn follower_apply_entries_2() {
        let mut server_ids = HashSet::new();
        server_ids.insert("a");
        server_ids.insert("b");
        server_ids.insert("c");

        let mut a: TestNode = Node::new("a", server_ids.clone());

        expect_actions(
            &mut a,
            &Input::OnMessage {
                term: 2,
                server_id: "b",
                message: Message::ApplyEntriesRequest {
                    commit: None,
                    log_version: Some((1, 1)),
                    entries: vec![(2, 1)],
                },
            },
            vec![
                Action::SendMessage {
                    term: 2,
                    server_id: "b",
                    message: Message::LogEntryInfo { version: None },
                },
                Action::SetTimeout,
            ],
        );
    }

    #[test]
    fn candidate_step_down_same_term() {
        let mut server_ids = HashSet::new();
        server_ids.insert("a");
        server_ids.insert("b");
        server_ids.insert("c");

        let mut a: TestNode = Node::new("a", server_ids.clone());

        // Start an election
        expect_actions(
            &mut a,
            &Input::Timeout,
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message: Message::RequestVote { version: None },
                },
                Action::SendMessage {
                    term: 1,
                    server_id: "c",
                    message: Message::RequestVote { version: None },
                },
                Action::SetTimeout,
            ],
        );

        expect_actions(
            &mut a,
            &Input::OnMessage {
                term: 1,
                server_id: "b",
                message: Message::ApplyEntriesRequest {
                    commit: None,
                    log_version: None,
                    entries: Vec::new(),
                },
            },
            vec![
                Action::SetTimeout,
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message: Message::LogEntryInfo { version: None },
                },
            ],
        );
    }

    #[test]
    fn failed_election() {
        let mut server_ids = HashSet::new();
        server_ids.insert("a");
        server_ids.insert("b");
        server_ids.insert("c");

        let mut a: TestNode = Node::new("a", server_ids.clone());

        // Start an election
        expect_actions(
            &mut a,
            &Input::Timeout,
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message: Message::RequestVote { version: None },
                },
                Action::SendMessage {
                    term: 1,
                    server_id: "c",
                    message: Message::RequestVote { version: None },
                },
                Action::SetTimeout,
            ],
        );

        // Another candidate asks for a vote
        expect_actions(
            &mut a,
            &Input::OnMessage {
                message: Message::RequestVote { version: None },
                server_id: "b",
                term: 1,
            },
            vec![Action::SendMessage {
                server_id: "b",
                term: 1,
                message: Message::VoteRejected,
            }],
        );

        // Our vote has been rejected
        expect_actions(
            &mut a,
            &Input::OnMessage {
                message: Message::VoteRejected,
                server_id: "b",
                term: 1,
            },
            vec![],
        );

        // Trigger another election
        expect_actions(
            &mut a,
            &Input::Timeout,
            vec![
                Action::SetTimeout,
                Action::SendMessage {
                    term: 2,
                    server_id: "b",
                    message: Message::RequestVote { version: None },
                },
                Action::SendMessage {
                    term: 2,
                    server_id: "c",
                    message: Message::RequestVote { version: None },
                },
            ],
        );
    }

    #[test]
    fn happy_path() {
        let mut server_ids = HashSet::new();
        server_ids.insert("a");
        server_ids.insert("b");
        server_ids.insert("c");

        let mut a: TestNode = Node::new("a", server_ids.clone());
        let mut b: TestNode = Node::new("b", server_ids.clone());

        // We expect client requests to fail at this point
        expect_actions(
            &mut a,
            &Input::ClientRequest { entry: 1 },
            vec![Action::ClientRequestRejected {
                current_leader: None,
            }],
        );

        // A timeout here should trigger an election
        expect_actions(
            &mut a,
            &Input::Timeout,
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message: Message::RequestVote { version: None },
                },
                Action::SendMessage {
                    term: 1,
                    server_id: "c",
                    message: Message::RequestVote { version: None },
                },
                Action::SetTimeout,
            ],
        );

        // Let's pass on the vote request to b
        expect_actions(
            &mut b,
            &Input::OnMessage {
                message: Message::RequestVote { version: None },
                server_id: "a",
                term: 1,
            },
            vec![
                Action::SetTimeout,
                Action::SendMessage {
                    server_id: "a",
                    term: 1,
                    message: Message::VoteAccepted,
                },
            ],
        );

        // Let's pass the response back to a
        expect_actions(
            &mut a,
            &Input::OnMessage {
                message: Message::VoteAccepted,
                server_id: "b",
                term: 1,
            },
            vec![
                Action::ClearTimeout,
                Action::SendMessage {
                    server_id: "b",
                    term: 1,
                    message: Message::ApplyEntriesRequest {
                        commit: None,
                        log_version: None,
                        entries: vec![],
                    },
                },
                Action::SendMessage {
                    server_id: "c",
                    term: 1,
                    message: Message::ApplyEntriesRequest {
                        commit: None,
                        log_version: None,
                        entries: vec![],
                    },
                },
            ],
        );

        // Let's pass on the heartbeat message to b
        expect_actions(
            &mut b,
            &Input::OnMessage {
                message: Message::ApplyEntriesRequest {
                    commit: None,
                    log_version: None,
                    entries: Vec::new(),
                },
                server_id: "a",
                term: 1,
            },
            vec![
                Action::SetTimeout,
                Action::SendMessage {
                    server_id: "a",
                    term: 1,
                    message: Message::LogEntryInfo { version: None },
                },
            ],
        );

        // b should now consider a the leader, let's check with a client
        // request
        expect_actions(
            &mut b,
            &Input::ClientRequest { entry: 2 },
            vec![Action::ClientRequestRejected {
                current_leader: Some("a"),
            }],
        );

        // Having established leadership, let's publish a message
        expect_actions(
            &mut a,
            &Input::ClientRequest { entry: 3 },
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "c",
                    message: Message::ApplyEntriesRequest {
                        commit: None,
                        log_version: None,
                        entries: vec![(1, 3)],
                    },
                },
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message: Message::ApplyEntriesRequest {
                        commit: None,
                        log_version: None,
                        entries: vec![(1, 3)],
                    },
                },
            ],
        );

        // Let's pass on the message to b
        expect_actions(
            &mut b,
            &Input::OnMessage {
                term: 1,
                server_id: "a",
                message: Message::ApplyEntriesRequest {
                    commit: None,
                    log_version: None,
                    entries: vec![(1, 3)],
                },
            },
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "a",
                    message: Message::LogEntryInfo {
                        version: Some((0, 1)),
                    },
                },
                Action::SetTimeout,
            ],
        );

        // This is should cause the entry to be committed
        expect_actions(
            &mut a,
            &Input::OnMessage {
                term: 1,
                server_id: "b",
                message: Message::LogEntryInfo {
                    version: Some((0, 1)),
                },
            },
            vec![],
        );
    }
}
