use std::mem;

pub struct Singleton<X> {
    value: Option<X>,
}

impl<X> Iterator for Singleton<X> {
    type Item = X;

    fn next(&mut self) -> Option<X> {
        let mut output = None;
        mem::swap(&mut self.value, &mut output);
        output
    }
}

impl<X> Singleton<X> {
    pub fn new(x: X) -> Self {
        Singleton {
            value: Some(x),
        }
    }
}
