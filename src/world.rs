use crate::log::*;
use crate::raft::*;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct InFlightMessage {
    to: u8,
    from: u8,
    term: Term,
    message: Message<usize>,
}

#[derive(Clone)]
pub struct World {
    nodes: [Node<u8, TestLog>; 3],
    timers: [bool; 3],
    messages: Vec<InFlightMessage>,
    message_counter: usize,
}

#[derive(Clone, Debug)]
pub enum WorldUpdate {
    Timeout(u8),
    Deliver(usize, InFlightMessage),
    ClientRequest(u8),
}

impl World {
    pub fn initial() -> Self {
        let mut names = HashSet::new();
        names.insert(0);
        names.insert(1);
        names.insert(2);

        let nodes = [
            Node::new(0, names.clone()),
            Node::new(1, names.clone()),
            Node::new(2, names.clone()),
        ];

        let timers = [true; 3];
        let messages = Vec::new();

        World {
            nodes,
            timers,
            messages,
            message_counter: 0,
        }
    }

    fn new_message(&mut self) -> usize {
        self.message_counter += 1;
        self.message_counter
    }

    fn process(&mut self, i: usize, input: &Input<u8, usize>) {
        for action in self.nodes[i].process(input) {
            match action {
                Action::ClearTimeout => {
                    self.timers[i] = false;
                }
                Action::SetTimeout => {
                    self.timers[i] = true;
                }
                Action::SendMessage {
                    message,
                    server_id,
                    term,
                } => {
                    self.messages.push(InFlightMessage {
                        to: server_id,
                        from: i as u8,
                        message,
                        term,
                    });
                }
                Action::AckClientRequest { .. } => {}
                Action::ClientRequestRejected { .. } => {}
                Action::Commit { .. } => {}
            }
        }
    }

    pub fn apply(&mut self, update: WorldUpdate) {
        match update {
            WorldUpdate::Timeout(i) => {
                self.process(i as usize, &Input::Timeout);
            }
            WorldUpdate::Deliver(i, _) => {
                let msg = self.messages.remove(i);
                self.process(
                    msg.to as usize,
                    &Input::OnMessage {
                        server_id: msg.from,
                        message: msg.message.clone(),
                        term: msg.term,
                    },
                );
            }
            WorldUpdate::ClientRequest(i) => {
                let entry = self.new_message();
                self.process(i as usize, &Input::ClientRequest { entry });
            }
        }
    }

    pub fn options(&self) -> Vec<WorldUpdate> {
        let mut output = Vec::new();

        for i in 0..3 {
            if self.timers[i] {
                output.push(WorldUpdate::Timeout(i as u8));
            }
        }

        for i in 0..self.messages.len() {
            let msg = self.messages.get(i).unwrap().clone();
            output.push(WorldUpdate::Deliver(i, msg));
        }

        for i in 0..3 {
            output.push(WorldUpdate::ClientRequest(i as u8));
        }

        output
    }
}
