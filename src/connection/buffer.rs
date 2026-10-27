pub(super) struct Buffer {
    pub(super) data: Vec<u8>,
    pub(super) read_pos: usize,
    pub(super) write_pos: usize,
}

impl Buffer {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            data: vec![0; buffer_size],
            read_pos: 0,
            write_pos: 0,
        }
    }

    pub fn compact(&mut self) {
        if self.read_pos == 0 {
            return;
        }

        self.data.copy_within(self.read_pos..self.write_pos, 0);

        self.write_pos -= self.read_pos;
        self.read_pos = 0;
    }

    pub fn is_full(&self) -> bool {
        self.write_pos - self.read_pos == self.buffer_size()
    }

    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    pub fn buffer_size(&self) -> usize {
        self.data.len()
    }
}
