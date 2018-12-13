use raft::world::World;

fn simulate(world: World, depth: usize) {
    if depth == 0 {
        return;
    }

    for option in world.options() {
        simulate(option, depth - 1);
    }
}

fn main() {
    simulate(World::initial(), 4);
}
