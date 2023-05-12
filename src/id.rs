use wikidata::{Fid, Lid, Pid, Qid, Sid};

pub enum Id {
    Fid(Fid),
    Lid(Lid),
    Pid(Pid),
    Qid(Qid),
    Sid(Sid),
}

impl<'a> From<&'a str> for Id {
    fn from(value: &'a str) -> Self {
        match value.get(0..1) {
            Some("L") => Self::Lid(Lid(value[1..].parse::<u64>().unwrap())),
            Some("P") => Self::Pid(Pid(value[1..].parse::<u64>().unwrap())),
            Some("Q") => Self::Qid(Qid(value[1..].parse::<u64>().unwrap())),
            Some("F") => {
                let mut parts = value[1..].split('-');
                Self::Fid(Fid(
                    Lid(parts.next().unwrap().parse::<u64>().unwrap()),
                    parts.next().unwrap()[1..].parse::<u16>().unwrap(),
                ))
            }
            Some("S") => {
                let mut parts = value[1..].split('-');
                Self::Sid(Sid(
                    Lid(parts.next().unwrap().parse::<u64>().unwrap()),
                    parts.next().unwrap()[1..].parse::<u16>().unwrap(),
                ))
            }
            _ => panic!("Invalid ID: {}", value),
        }
    }
}

impl From<Id> for u64 {
    fn from(id: Id) -> Self {
        match id {
            Id::Fid(fid) => u64::from(Id::Lid(fid.0)) + (fid.1 as u64 * 100_000_000_000),
            Id::Lid(lid) => lid.0 + 2_000_000_000,
            Id::Pid(pid) => pid.0 + 1_000_000_000,
            Id::Qid(qid) => qid.0,
            Id::Sid(sid) => {
                u64::from(Id::Lid(sid.0)) + (sid.1 as u64 * 100_000_000_000) + 10_000_000_000
            }
        }
    }
}
