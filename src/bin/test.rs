use raft::world::{World, WorldUpdate};
use std::panic;

fn simulate(
    trace: &mut Vec<WorldUpdate>,
    world: World,
    depth: usize,
) -> Result<(), Vec<WorldUpdate>> {
    if depth == 0 {
        return Ok(());
    }

    for option in world.options() {
        trace.push(option.clone());
        let mut new_world = world.clone();
        let result = panic::catch_unwind(move || {
            new_world.apply(option);
            new_world
        });
        match result {
            Err(_) => {
                return Err(trace.clone());
            }
            Ok(new_world) => {
                simulate(trace, new_world, depth - 1)?;
                trace.pop();
            }
        }
    }

    Ok(())
}

fn main() {
    // Interesting scenarios tend to start by electing a leader and publishing
    // a client request, so lets do that before the search
    let mut trace =
        vec![
            WorldUpdate::Timeout(0),
            WorldUpdate::Deliver(0, None), // RequestVote
            WorldUpdate::Deliver(1, None), // AcceptVote
            WorldUpdate::ClientRequest(0),
        ];

    let mut world = World::initial();

    for update in trace.iter() {
        world.apply(update.clone());
    }

    let output = simulate(&mut trace, world, 7);

    println!("{:#?}", output);
}
