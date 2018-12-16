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
    // Its easier to find bugs with some setup
    let mut trace = vec![
        // Make 0 leader in term 1
        WorldUpdate::Timeout(0),
        WorldUpdate::Deliver(0, None), // RequestVote
        WorldUpdate::Deliver(1, None), // AcceptVote
        WorldUpdate::ClientRequest(0),
    ];

    let mut world = World::initial();

    for update in trace.iter_mut() {
        world.apply_hints(update);
        world.apply(update.clone());
    }

    println!("{:#?}", trace);

    let output = simulate(&mut trace, world, 9);

    println!("{:#?}", output);
}
