use std::cmp::Ordering;

pub(crate) fn compare_text_case_insensitive(left: &str, right: &str) -> Ordering {
    if left.is_ascii() && right.is_ascii() {
        return compare_ascii_case_insensitive(left.as_bytes(), right.as_bytes());
    }

    compare_unicode_case_insensitive(left, right)
}

pub(crate) fn normalize_text_case_insensitive(value: &str) -> String {
    if value.is_ascii() {
        return value.to_ascii_lowercase();
    }

    value.chars().flat_map(|ch| ch.to_lowercase()).collect()
}

fn compare_ascii_case_insensitive(left: &[u8], right: &[u8]) -> Ordering {
    let shared = left.len().min(right.len());

    for idx in 0..shared {
        let l = left[idx].to_ascii_lowercase();
        let r = right[idx].to_ascii_lowercase();

        if l < r {
            return Ordering::Less;
        }

        if l > r {
            return Ordering::Greater;
        }
    }

    left.len().cmp(&right.len())
}

fn compare_unicode_case_insensitive(left: &str, right: &str) -> Ordering {
    let mut left_iter = left.chars().flat_map(|ch| ch.to_lowercase());
    let mut right_iter = right.chars().flat_map(|ch| ch.to_lowercase());

    loop {
        match (left_iter.next(), right_iter.next()) {
            (Some(l), Some(r)) if l < r => return Ordering::Less,
            (Some(l), Some(r)) if l > r => return Ordering::Greater,
            (Some(_), Some(_)) => continue,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }
}