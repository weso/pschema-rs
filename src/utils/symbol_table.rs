use bimap::BiMap;

pub struct SymbolTable<'a> {
    symbol_table: BiMap<&'a str, u8>,
    last: u8,
}

impl<'a> SymbolTable<'a> {
    pub fn new() -> Self {
        SymbolTable {
            symbol_table: BiMap::<&'a str, u8>::new(),
            last: Default::default(),
        }
    }

    pub fn insert(mut self, label: &'a str) -> u8 {
        self.symbol_table.insert(label, self.last);
        self.last += 1;
        self.last - 1
    }

    pub fn get(self, value: u8) -> &'a str {
        match self.symbol_table.get_by_right(&value) {
            Some(label) => label,
            None => "",
        }
    }
}

impl<'a> Default for SymbolTable<'a> {
    fn default() -> Self {
        Self::new()
    }
}
