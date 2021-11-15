use std::time::{SystemTime, UNIX_EPOCH};

pub fn timestamp() -> u64 {
	let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
	since_the_epoch.as_secs() * 1000 +
		since_the_epoch.subsec_nanos() as u64 / 1_000_000
}

pub fn msgid_to_str(raw: &Vec<u8>) -> String {
    let mut dst = [0 as u8; 8];
    dst.clone_from_slice(&raw.as_slice()[0..8]);
    format!("{}", u64::from_be_bytes(dst))
}

pub fn msgid_to_raw(sid: &String) -> Vec<u8> {
    let n = sid.parse::<u64>().unwrap();
    n.to_be_bytes().to_vec()
}