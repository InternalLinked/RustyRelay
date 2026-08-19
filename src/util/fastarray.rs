pub enum FastArrayError {
    NoneExistingId,
    EntrySoltsFull,
    OutOfBounds,
}

pub struct FastArray<T> {
    offset: usize,
    used: usize,
    capacity: usize,
    last_removed: Vec<usize>,
    entries: Vec<Option<T>>,
}

#[allow(dead_code)]
impl<T> FastArray<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            offset: 0,
            used: 0,
            capacity,
            last_removed: Vec::with_capacity(capacity),
            entries: (0..capacity).map(|_| None).collect(),
        }
    }

    pub fn is_full(&self) -> bool {
        self.used >= self.capacity
    }

    pub fn add_entry(&mut self, entry: T) -> Result<usize, FastArrayError> {
        self.used += 1;

        if let Some(value) = self.last_removed.pop() {
            self.entries[value] = Some(entry);

            return Ok(value);
        }

        self.entries[self.offset] = Some(entry);
        let offset: usize = self.offset;
        self.offset += 1;
        return Ok(offset);
    }

    pub fn remove_entry(&mut self, id: usize) -> Result<(), FastArrayError> {
        if id >= self.capacity {
            return Err(FastArrayError::OutOfBounds);
        }

        if let Some(_) = &self.entries[id] {
            self.used -= 1;
            self.last_removed.push(id);
            self.entries[id] = None;
            return Ok(());
        }

        Err(FastArrayError::NoneExistingId)
    }

    pub fn get_entry(&mut self, id: usize) -> Option<&mut T> {
        self.entries.get_mut(id).and_then(Option::as_mut)
    }
}
