use rattler_conda_types::MatchSpec;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::RwLock;
use typed_arena::Arena;

pub struct Cache<'a, 'b, T> {
    arena: Arena<T>,
    lookup: RwLock<HashMap<&'a str, &'b T>>,
}

unsafe impl<'a, 'b, T> Sync for Cache<'a, 'b, T> {}

impl<'a, 'b, T: FromStr> Cache<'a, 'b, T>
where
    T: FromStr,
    T::Err: Debug,
{
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Cache {
            arena: Arena::with_capacity(capacity),
            lookup: RwLock::new(HashMap::with_capacity(capacity)),
        }
    }

    pub fn get_or_insert(&'b self, key: &'a str) -> &'b T {
        {
            // Read Path
            let reader = self.lookup.read().unwrap();
            if let Some(val) = reader.get(key) {
                return val;
            }
        }
        {
            // Read-and-Probably-Write Path
            self.lookup
                .write()
                .unwrap()
                .entry(key)
                .or_insert_with(|| self.arena.alloc(T::from_str(key).expect("Malformed input")))
        }
    }
}

pub type MatchspecCache<'a, 'b> = Cache<'a, 'b, MatchSpec>;

#[cfg(test)]
mod tests {
    use crate::matchspeccache::MatchspecCache;

    #[test]
    fn matchspec_cache() {
        let cache = MatchspecCache::with_capacity(8);
        let spec1 = cache.get_or_insert("python 3.6");
        let spec2 = cache.get_or_insert("python 3.6");
        let spec3 = cache.get_or_insert("python 3.7");
        assert_eq!(spec1, spec2);
        assert_ne!(spec2, spec3);
    }
}
