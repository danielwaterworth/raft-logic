mod singleton;
mod log;

use crate::singleton::Singleton;
use crate::log::{Log, Term, LogIndex, LogVersion};

use std::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::cmp::Ordering;
use std::mem;
use std::fmt::Debug;

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

enum State<ServerID> {
    Follower(FollowerState<ServerID>),
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Message<Entry> {
    ApplyEntriesRequest {
        log_version: LogVersion,
        entries: Vec<(Term, Entry)>,
    },
    InstallSnapshot,
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
    Commit { entry: Entry },
}

pub enum Operation<'a, ServerID, Entry> {
    DoNothing,
    OneAction(Singleton<Action<ServerID, Entry>>),
    AcceptedClientRequest,
    EntriesApplied,
    EntriesNotApplied,
    TransitionToFollower {
        then: Box<Operation<'a, ServerID, Entry>>,
    },
    TransitionToLeader,
    Phantom(&'a u8),
    FreeForm(VecDeque<Action<ServerID, Entry>>),
}

impl<'a, ServerID, Entry> Iterator for Operation<'a, ServerID, Entry> {
    type Item = Action<ServerID, Entry>;

    fn next(&mut self) -> Option<Action<ServerID, Entry>> {
        match self {
            Operation::DoNothing => None,
            Operation::OneAction(iter) => {
                iter.next()
            },
            Operation::AcceptedClientRequest => {
                unimplemented!()
            },
            Operation::EntriesApplied => {
                unimplemented!()
            },
            Operation::EntriesNotApplied => {
                unimplemented!()
            },
            Operation::FreeForm(ref mut items) => {
                items.pop_front()
            },
            Operation::TransitionToFollower { then } => {
                let mut tmp = do_nothing();
                mem::swap(&mut tmp, &mut **then);
                *self = tmp;
                Some(Action::SetTimeout)
            },
            Operation::TransitionToLeader => {
                unimplemented!()
            },
            Operation::Phantom(_) => unreachable!(),
        }
    }
}

fn do_nothing<'a, ServerID, Entry>() -> Operation<'a, ServerID, Entry> {
    Operation::DoNothing
}

fn accepted_vote<'a, ServerID, Entry>(term: Term, server_id: ServerID)
        -> Operation<'a, ServerID, Entry> {
    Operation::OneAction(
        Singleton::new(
            Action::SendMessage {
                term,
                server_id,
                message: Message::VoteAccepted,
            }
        )
    )
}

fn rejected_vote<'a, ServerID, Entry>(term: Term, server_id: ServerID)
        -> Operation<'a, ServerID, Entry> {
    Operation::OneAction(
        Singleton::new(
            Action::SendMessage {
                term,
                server_id,
                message: Message::VoteRejected,
            }
        )
    )
}

fn accepted_client_request<'a, ServerID, Entry>()
        -> Operation<'a, ServerID, Entry> {
    unimplemented!()
}

fn rejected_client_request<'a, ServerID, Entry>(
            current_leader: Option<ServerID>,
        ) -> Operation<'a, ServerID, Entry> {
    Operation::OneAction(
        Singleton::new(
            Action::ClientRequestRejected {
                current_leader
            }
        )
    )
}

fn transition_to_follower<'a, ServerID, Entry>(
            then: Operation<'a, ServerID, Entry>
        ) -> Operation<'a, ServerID, Entry> {
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
                ServerID: Hash + Eq + Clone {
    let mut actions: VecDeque<Action<ServerID, Entry>> =
        servers
            .iter()
            .filter(|server_id| **server_id != *except)
            .map(|server_id|
                Action::SendMessage {
                    server_id: server_id.clone(),
                    term,
                    message:
                        Message::RequestVote {
                            version
                        }
                }
            )
            .collect();
    actions.push_back(
        Action::SetTimeout,
    );

    Operation::FreeForm(actions)
}

fn transition_to_leader<'a, ServerID, Entry>(
            term: Term,
            servers: &HashSet<ServerID>,
            except: &ServerID,
            version: LogVersion,
        ) -> Operation<'a, ServerID, Entry>
            where
                ServerID: Hash + Eq + Clone {
    let mut actions: VecDeque<Action<ServerID, Entry>> =
        servers
            .iter()
            .filter(|server_id| *server_id != except)
            .map(|server_id|
                Action::SendMessage {
                    server_id: server_id.clone(),
                    term,
                    message:
                        Message::ApplyEntriesRequest {
                            log_version: version,
                            entries: Vec::new(),
                        }
                }
            )
            .collect();
    actions.push_back(
        Action::ClearTimeout,
    );

    Operation::FreeForm(actions)
}

fn entries_applied<'a, ServerID, Entry>(
        term: Term,
        leader: ServerID,
    ) -> Operation<'a, ServerID, Entry> {
    let mut actions: VecDeque<Action<ServerID, Entry>> = VecDeque::new();
    actions.push_back(
        Action::SetTimeout,
    );
    actions.push_back(
        Action::SendMessage {
            term,
            server_id: leader,
            message:
                Message::EntriesApplied {

                },
        },
    );

    Operation::FreeForm(actions)
}

fn entries_not_applied<'a, ServerID, Entry>(
        term: Term,
        leader: ServerID,
    ) -> Operation<'a, ServerID, Entry> {
    // ResetTimeout
    // Reply to leader
    unimplemented!()
}

impl<ServerID: Hash + Eq + Clone, Entry: Clone> Node<ServerID, Entry> {
    pub fn new(server_id: ServerID, servers: HashSet<ServerID>)
            -> Node<ServerID, Entry> {
        Node {
            server_id,
            servers,
            current_term: 0,
            state: State::Follower(FollowerState::Oblivious),
            log: Log::empty(),
        }
    }

    fn transition_to_candidate(&mut self) -> Operation<ServerID, Entry> {
        self.current_term += 1;

        self.state =
            State::Candidate {
                votes_received: HashSet::default(),
            };

        transition_to_candidate(
            self.current_term,
            &self.servers,
            &self.server_id,
            self.log.version(),
        )
    }

    pub fn process(&mut self, input: &Input<ServerID, Entry>)
            -> Operation<ServerID, Entry> {
        match input {
            Input::OnMessage { message, term, server_id } => {
                if self.current_term > *term {
                    return do_nothing();
                }
                if *term > self.current_term {
                    self.current_term = *term;
                    self.state =
                        State::Follower(
                            FollowerState::Oblivious
                        );
                    return transition_to_follower(self.process(input));
                }
            },
            _ => {}
        }

        match &mut self.state {
            State::Follower(follower_state) => {
                match input {
                    Input::ClientRequest { entry } => {
                        rejected_client_request(
                            follower_state.current_leader(),
                        )
                    },
                    Input::OnMessage { message, term, server_id } => {
                        match message {
                            Message::RequestVote { version } => {
                                let cmp =
                                    compare_log_versions(
                                        *version,
                                        self.log.version(),
                                    ) != Ordering::Less;

                                if follower_state.can_vote() && cmp {
                                    *follower_state =
                                        FollowerState::Voted(
                                            server_id.clone(),
                                        );
                                    accepted_vote(
                                        self.current_term,
                                        server_id.clone(),
                                    )
                                } else {
                                    rejected_vote(
                                        self.current_term,
                                        server_id.clone(),
                                    )
                                }
                            },
                            Message::InstallSnapshot => {
                                unimplemented!()
                            },
                            Message::ApplyEntriesRequest {
                                        entries, log_version
                                    } => {
                                *follower_state =
                                    FollowerState::Following(
                                        server_id.clone(),
                                    );
                                if self.log.version() == *log_version {
                                    for entry in entries {
                                        self.log.insert(
                                            entry.0,
                                            entry.1.clone(),
                                        );
                                    }
                                    entries_applied(
                                        *term,
                                        server_id.clone(),
                                    )
                                } else {
                                    entries_not_applied(
                                        *term,
                                        server_id.clone(),
                                    )
                                }
                            },
                            Message::VoteAccepted | Message::VoteRejected => {
                                do_nothing()
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
                    Input::Timeout => {
                        self.transition_to_candidate()
                    },
                }
            },
            State::Candidate { votes_received } => {
                match input {
                    Input::ClientRequest { entry } => {
                        rejected_client_request(None)
                    },
                    Input::OnMessage { message, term, server_id } => {
                        match message {
                            Message::RequestVote { version } => {
                                rejected_vote(
                                    *term,
                                    server_id.clone(),
                                )
                            },
                            Message::ApplyEntriesRequest { .. }
                            | Message::InstallSnapshot => {
                                self.state =
                                    State::Follower(
                                        FollowerState::Following(
                                            self.server_id.clone()
                                        )
                                    );

                                transition_to_follower(self.process(input))
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

                                    transition_to_leader(
                                        self.current_term,
                                        &self.servers,
                                        &self.server_id,
                                        self.log.version(),
                                    )
                                } else {
                                    do_nothing()
                                }
                            },
                            Message::VoteRejected => {
                                do_nothing()
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
                    Input::Timeout => {
                        self.transition_to_candidate()
                    },
                }
            },
            State::Leader { next_index, match_index } => {
                match input {
                    Input::ClientRequest { entry } => {
                        let log_id =
                            self.log.insert(self.current_term, entry.clone());
                        accepted_client_request()
                    },
                    Input::OnMessage { message, term, server_id } => {
                        match message {
                            Message::RequestVote { version } => {
                                rejected_vote(
                                    *term,
                                    server_id.clone(),
                                )
                            },
                            Message::ApplyEntriesRequest { .. }
                            | Message::InstallSnapshot => {
                                unreachable!(
                                    "cannot have two leaders in the same term"
                                )
                            },
                            Message::VoteAccepted | Message::VoteRejected => {
                                do_nothing()
                            },
                            Message::EntriesApplied {} => {
                                // FIXME: update internal state
                                do_nothing()
                            },
                            Message::EntriesNotApplied {} => {
                                // FIXME: update internal state
                                do_nothing()
                            },
                        }
                    },
                    Input::Timeout => {
                        unreachable!("No timer should have been set")
                    },
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_actions<ServerID, Entry>(
                node: &mut Node<ServerID, Entry>,
                input: &Input<ServerID, Entry>,
                expected_actions: Vec<Action<ServerID, Entry>>,
            ) where
                ServerID: Hash + Eq + Debug + Clone,
                Entry: Hash + Eq + Debug + Clone, {
        let actions: HashSet<Action<ServerID, Entry>> =
            node.process(input).collect();

        let expected_actions: HashSet<Action<ServerID, Entry>> =
            expected_actions.into_iter().collect();

        assert_eq!(actions, expected_actions);
    }

    #[test]
    fn candidate_step_down_same_term() {
        let mut server_ids = HashSet::new();
        server_ids.insert("a");
        server_ids.insert("b");
        server_ids.insert("c");

        let mut a: Node<&str, &str> = Node::new("a", server_ids.clone());

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
                message:
                    Message::ApplyEntriesRequest {
                        log_version: None,
                        entries: Vec::new(),
                    },
            },
            vec![
                Action::SetTimeout,
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message:
                        Message::EntriesApplied {
                        },
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

        let mut a: Node<&str, &str> = Node::new("a", server_ids.clone());

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
            vec![
                Action::SendMessage {
                    server_id: "b",
                    term: 1,
                    message: Message::VoteRejected,
                },
            ],
        );

        // Our vote has been rejected
        expect_actions(
            &mut a,
            &Input::OnMessage {
                message: Message::VoteRejected,
                server_id: "b",
                term: 1,
            },
            vec![
            ],
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

        let mut a: Node<&str, &str> = Node::new("a", server_ids.clone());
        let mut b: Node<&str, &str> = Node::new("b", server_ids.clone());

        // We expect client requests to fail at this point
        expect_actions(
            &mut a,
            &Input::ClientRequest {
                entry: "1.1",
            },
            vec![
                Action::ClientRequestRejected {
                    current_leader: None,
                },
            ],
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
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message:
                        Message::ApplyEntriesRequest {
                            log_version: None,
                            entries: Vec::new(),
                        },
                },
                Action::SendMessage {
                    term: 1,
                    server_id: "c",
                    message:
                        Message::ApplyEntriesRequest {
                            log_version: None,
                            entries: Vec::new(),
                        },
                },
                Action::ClearTimeout,
            ],
        );

        // Let's pass on the heartbeat message to b
        expect_actions(
            &mut b,
            &Input::OnMessage {
                message:
                    Message::ApplyEntriesRequest {
                        log_version: None,
                        entries: Vec::new(),
                    },
                server_id: "a",
                term: 1,
            },
            vec![
                Action::SetTimeout,
                Action::SendMessage {
                    term: 1,
                    server_id: "a",
                    message:
                        Message::EntriesApplied {
                        },
                },
            ],
        );

        // b should now consider a the leader, let's check with a client
        // request
        expect_actions(
            &mut b,
            &Input::ClientRequest {
                entry: "1.2",
            },
            vec![
                Action::ClientRequestRejected {
                    current_leader: Some("a"),
                },
            ],
        );

        // Let's pass the entries-applied message back to a
        expect_actions(
            &mut a,
            &Input::OnMessage {
                server_id: "b",
                term: 1,
                message:
                    Message::EntriesApplied {
                    },
            },
            vec![
            ],
        );

        // Working up to here
        return;

        // Having established leadership, let's publish a message
        expect_actions(
            &mut a,
            &Input::ClientRequest {
                entry: "1.3",
            },
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "b",
                    message:
                        Message::ApplyEntriesRequest {
                            log_version: None,
                            entries: vec![(1, "1.3")],
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
                message:
                    Message::ApplyEntriesRequest {
                        log_version: None,
                        entries: vec![(1, "1.3")],
                    },
            },
            vec![
                Action::SendMessage {
                    term: 1,
                    server_id: "a",
                    message:
                        Message::EntriesApplied {
                        },
                },
                Action::SetTimeout,
            ],
        );
    }
}
