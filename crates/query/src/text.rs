use std::cmp::Ordering;

use crate::StringComparisonMode;

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

pub(crate) fn like_pattern_matches(
    value: &str,
    pattern: &str,
    string_comparison_mode: StringComparisonMode,
) -> bool {
    let (value_chars, pattern_chars) = match string_comparison_mode {
        StringComparisonMode::CaseInsensitive => (
            normalize_text_case_insensitive(value).chars().collect::<Vec<_>>(),
            normalize_text_case_insensitive(pattern)
                .chars()
                .collect::<Vec<_>>(),
        ),
        StringComparisonMode::CaseSensitive => (
            value.chars().collect::<Vec<_>>(),
            pattern.chars().collect::<Vec<_>>(),
        ),
    };

    like_pattern_matches_chars(&value_chars, &pattern_chars)
}

fn like_pattern_matches_chars(value: &[char], pattern: &[char]) -> bool {
    let mut value_idx = 0usize;
    let mut pattern_idx = 0usize;
    let mut last_percent_idx = None;
    let mut backtrack_value_idx = 0usize;

    while value_idx < value.len() {
        if pattern_idx < pattern.len()
            && (pattern[pattern_idx] == '_' || pattern[pattern_idx] == value[value_idx])
        {
            value_idx += 1;
            pattern_idx += 1;
            continue;
        }

        if pattern_idx < pattern.len() && pattern[pattern_idx] == '%' {
            last_percent_idx = Some(pattern_idx);
            pattern_idx += 1;
            backtrack_value_idx = value_idx;
            continue;
        }

        if let Some(percent_idx) = last_percent_idx {
            pattern_idx = percent_idx + 1;
            backtrack_value_idx += 1;
            value_idx = backtrack_value_idx;
            continue;
        }

        return false;
    }

    while pattern_idx < pattern.len() && pattern[pattern_idx] == '%' {
        pattern_idx += 1;
    }

    pattern_idx == pattern.len()
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
