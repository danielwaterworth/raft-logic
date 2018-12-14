use crate::log::*;
use crate::raft::*;
use std::collections::HashSet;

#[derive(Clone)]
struct InFlightMessage {
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

    pub fn options(&self) -> Vec<Self> {
        let mut output = Vec::new();

        for i in 0..3 {
            if self.timers[i] {
                let mut option = self.clone();
                option.timers[i] = false;
                option.process(i, &Input::Timeout);
                output.push(option);
            }
        }

        for i in 0..self.messages.len() {
            let mut option = self.clone();
            let msg = option.messages.remove(i);
            option.process(
                msg.to as usize,
                &Input::OnMessage {
                    server_id: msg.from,
                    message: msg.message,
                    term: msg.term,
                },
            );
            output.push(option);
        }

        for i in 0..3 {
            let mut option = self.clone();
            let entry = option.new_message();
            option.process(i, &Input::ClientRequest { entry });
            output.push(option);
        }

        output
    }
}
