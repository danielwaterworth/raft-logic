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
    let mut trace = Vec::new();
    let output = simulate(&mut trace, World::initial(), 6);
    println!("{:#?}", output);
}
